-- 历史 GC 是独立维护域：准备写不可变计划，执行只回收快照之前的连续批次。
-- 不清理运行身份、快照、永久 guard、pin、receipt、event 或 lease。
CREATE TABLE pulse_durable_distributed.history_gc_plans(
 namespace text NOT NULL, run_id text NOT NULL, plan_id text NOT NULL,
 actor name NOT NULL, request_digest text NOT NULL CHECK(request_digest ~ '^[0-9a-f]{64}$'),
 plan_digest text NOT NULL CHECK(plan_digest ~ '^[0-9a-f]{64}$'), plan jsonb NOT NULL,
 prepared_at timestamptz NOT NULL,
 executed boolean NOT NULL DEFAULT false,
 PRIMARY KEY(namespace,run_id,plan_id),
 FOREIGN KEY(namespace,run_id) REFERENCES pulse_durable_distributed.runs(namespace,run_id)
);
CREATE TABLE pulse_durable_distributed.history_gc_receipts(
 namespace text NOT NULL, run_id text NOT NULL, plan_id text NOT NULL,
 receipt_digest text NOT NULL CHECK(receipt_digest ~ '^[0-9a-f]{64}$'),
 result jsonb NOT NULL, occurred_at timestamptz NOT NULL,
 PRIMARY KEY(namespace,run_id,plan_id),
 FOREIGN KEY(namespace,run_id,plan_id) REFERENCES pulse_durable_distributed.history_gc_plans(namespace,run_id,plan_id)
);
-- apply 时间是 ReplayHistoryTTL 的权威依据；finish 等同修订事件不得替代。
CREATE INDEX history_apply_times ON pulse_durable_distributed.mutation_guards(namespace,run_id,revision) WHERE operation='apply';
REVOKE ALL ON pulse_durable_distributed.history_gc_plans,pulse_durable_distributed.history_gc_receipts FROM PUBLIC;

-- 固定顺序的字符串数组与 Go encoding/json 完全同义，复用已有字符串转义。
CREATE FUNCTION pulse_durable_distributed.history_gc_digest(VARIADIC fields text[]) RETURNS text LANGUAGE sql IMMUTABLE SET search_path=pg_catalog,pulse_durable_distributed,pg_temp AS $$
 SELECT encode(sha256(convert_to('['||coalesce(string_agg(pin_quote(v),',' ORDER BY n),'')||']','UTF8')),'hex') FROM unnest(fields) WITH ORDINALITY f(v,n)
$$;
CREATE FUNCTION pulse_durable_distributed.history_gc_uint(a jsonb,k text,maximum numeric) RETURNS numeric LANGUAGE plpgsql IMMUTABLE SET search_path=pg_catalog,pulse_durable_distributed,pg_temp AS $$
DECLARE v text:=a->>k; n numeric;
BEGIN
 IF jsonb_typeof(a->k) IS DISTINCT FROM 'string' OR v !~ '^[1-9][0-9]{0,19}$' THEN RAISE EXCEPTION USING ERRCODE='HG001',MESSAGE='invalid gc integer'; END IF;
 n:=v::numeric;
 IF n>maximum THEN RAISE EXCEPTION USING ERRCODE='HG001',MESSAGE='gc integer exceeds bound'; END IF;
 RETURN n;
END $$;
CREATE FUNCTION pulse_durable_distributed.history_gc_auth(a jsonb) RETURNS text LANGUAGE plpgsql SET search_path=pg_catalog,pulse_durable_distributed,pg_temp AS $$
DECLARE ns text:=retention_policy_auth(a); k text;
BEGIN
 FOREACH k IN ARRAY ARRAY['namespace','run_id','plan_id'] LOOP
  IF jsonb_typeof(a->k) IS DISTINCT FROM 'string' OR coalesce(octet_length(a->>k),0) NOT BETWEEN 1 AND 256 THEN RAISE EXCEPTION USING ERRCODE='HG001',MESSAGE='invalid gc identity'; END IF;
 END LOOP;
 IF a ?| ARRAY['epoch','worker_id','boot_id','lease_id','actor','holder_digest'] THEN RAISE EXCEPTION USING ERRCODE='HG001',MESSAGE='gc identity context forbidden'; END IF;
 RETURN ns;
END $$;
-- 调用者先锁 namespace policy；本 helper 固定 plan advisory → identity advisory → run。
-- 不接收 worker token，不反向获取 policy/plan 锁，单事务只涉及一个 Run。
CREATE FUNCTION pulse_durable_distributed.history_gc_lock(a jsonb) RETURNS pulse_durable_distributed.runs LANGUAGE plpgsql SET search_path=pg_catalog,pulse_durable_distributed,pg_temp AS $$
DECLARE r pulse_durable_distributed.runs;
BEGIN
 PERFORM pg_advisory_xact_lock(hashtextextended(jsonb_build_array(a->>'namespace',a->>'run_id',a->>'plan_id')::text,5788618723189269580));
 PERFORM pg_advisory_xact_lock(hashtextextended(jsonb_build_array(a->>'namespace',a->>'run_id')::text,5788618723189269578));
 SELECT * INTO r FROM pulse_durable_distributed.runs WHERE namespace=a->>'namespace' AND run_id=a->>'run_id' FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='HG006',MESSAGE='gc run not found'; END IF;
 RETURN r;
END $$;
CREATE FUNCTION pulse_durable_distributed.history_gc_request_digest(q jsonb) RETURNS text LANGUAGE sql IMMUTABLE SET search_path=pg_catalog,pulse_durable_distributed,pg_temp AS $$
 SELECT history_gc_digest('pulse-durable/history-gc/request/v1',q->'identity'->>'namespace',q->'identity'->>'run_id',q->>'plan_id',q->>'policy_version',q->>'policy_digest',q->>'max_rows',q->>'max_bytes')
$$;
CREATE FUNCTION pulse_durable_distributed.history_gc_plan_digest(p jsonb) RETURNS text LANGUAGE sql IMMUTABLE SET search_path=pg_catalog,pulse_durable_distributed,pg_temp AS $$
 SELECT history_gc_digest('pulse-durable/history-gc/plan/v1',p->>'request_digest',p->>'head_revision',p->>'head_chain',p->>'snapshot_generation',p->>'snapshot_revision',p->>'snapshot_chain',p->>'snapshot_digest',p->>'before_floor',p->>'after_floor',p->>'first_revision',p->>'through_revision',p->>'prefix_digest',p->>'rows',p->>'bytes',(extract(epoch FROM (p->>'prepared_at')::timestamptz)*1000000)::numeric(30,0)::text)
$$;
CREATE FUNCTION pulse_durable_distributed.history_gc_receipt_digest(p jsonb) RETURNS text LANGUAGE sql IMMUTABLE SET search_path=pg_catalog,pulse_durable_distributed,pg_temp AS $$
 SELECT history_gc_digest('pulse-durable/history-gc/receipt/v1',p->'identity'->>'namespace',p->'identity'->>'run_id',p->>'plan_id',p->>'plan_digest',p->>'rows',p->>'bytes',p->>'history_floor',(extract(epoch FROM (p->>'occurred_at')::timestamptz)*1000000)::numeric(30,0)::text)
$$;
-- 保护整个 Run，避免首片引入多快照 pin 恢复路径选择。永久 guards 不阻止无关批次回收。
CREATE FUNCTION pulse_durable_distributed.history_gc_protect(r pulse_durable_distributed.runs,t timestamptz) RETURNS void LANGUAGE plpgsql SET search_path=pg_catalog,pulse_durable_distributed,pg_temp AS $$
BEGIN
 IF EXISTS(SELECT 1 FROM pulse_durable_distributed.history_pins WHERE namespace=r.namespace AND run_id=r.run_id AND NOT released AND (expires_at IS NULL OR expires_at>t)) OR EXISTS(SELECT 1 FROM pulse_durable_distributed.build_pins WHERE namespace=r.namespace AND run_id=r.run_id AND NOT closed AND expires_at>t) THEN RAISE EXCEPTION USING ERRCODE='HG003',MESSAGE='gc run protected'; END IF;
END $$;
CREATE FUNCTION pulse_durable_distributed.history_gc_check(r pulse_durable_distributed.runs,p pulse_durable_distributed.retention_policies,t timestamptz) RETURNS pulse_durable_distributed.storage_snapshots LANGUAGE plpgsql SET search_path=pg_catalog,pulse_durable_distributed,pg_temp AS $$
DECLARE s pulse_durable_distributed.storage_snapshots; cutoff timestamptz;
BEGIN
 IF r.status NOT IN ('succeeded','failed','canceled') OR r.revision<1 OR r.active_snapshot IS NULL OR r.finished_at IS NULL OR r.finished_at>t OR r.created_at>t OR r.receipt_coverage_floor>t THEN RAISE EXCEPTION USING ERRCODE='HG002',MESSAGE='gc terminal snapshot required'; END IF;
 IF NOT p.enabled OR p.replay_history_ttl IS NULL OR p.replay_history_ttl<3600000000000 OR NOT retention_policy_validate(p.policy) OR p.policy->>'Enabled'<>'true' OR (p.policy->>'ReplayHistoryTTL')::bigint IS DISTINCT FROM p.replay_history_ttl OR p.policy_digest IS DISTINCT FROM encode(sha256(convert_to(replace(replace(p.policy::text,': ',':'),', ',','),'UTF8')),'hex') THEN RAISE EXCEPTION USING ERRCODE='HG002',MESSAGE='gc enabled history policy required'; END IF;
 SELECT * INTO s FROM pulse_durable_distributed.storage_snapshots WHERE namespace=r.namespace AND run_id=r.run_id AND generation=r.active_snapshot;
 IF NOT FOUND OR s.generation<>r.representation_generation OR s.revision<r.history_floor OR s.revision>r.revision OR encode(sha256(s.record),'hex') IS DISTINCT FROM s.digest THEN RAISE EXCEPTION USING ERRCODE='HG004',MESSAGE='gc published snapshot invalid'; END IF;
 -- history_floor 将提升至这个快照，因此整个失效前缀都必须满足 TTL，不能
 -- 因写预算只删一部分，就提前使仍应保留的晚近修订失效。
 cutoff:=t-(p.replay_history_ttl/1000000000)*interval '1 second';
 IF EXISTS(SELECT 1 FROM pulse_durable_distributed.batches b LEFT JOIN LATERAL (SELECT count(*) AS n,max(occurred_at) AS at FROM pulse_durable_distributed.mutation_guards g WHERE g.namespace=b.namespace AND g.run_id=b.run_id AND g.revision=b.revision AND g.operation='apply') g ON true WHERE b.namespace=r.namespace AND b.run_id=r.run_id AND b.revision>r.history_gc_through AND b.revision<s.revision AND (g.n<>1 OR g.at IS NULL OR g.at>cutoff)) THEN RAISE EXCEPTION USING ERRCODE='HG004',MESSAGE='gc prefix time evidence missing or retained'; END IF;
 PERFORM history_gc_protect(r,t);
 PERFORM history_path(r,r.revision);
 RETURN s;
END $$;
CREATE FUNCTION pulse_durable_distributed.history_gc_verify_plan(g pulse_durable_distributed.history_gc_plans) RETURNS void LANGUAGE plpgsql SET search_path=pg_catalog,pulse_durable_distributed,pg_temp AS $$
BEGIN
 IF g.plan->'request'->'identity'->>'namespace' IS DISTINCT FROM g.namespace OR g.plan->'request'->'identity'->>'run_id' IS DISTINCT FROM g.run_id OR g.plan->'request'->>'plan_id' IS DISTINCT FROM g.plan_id OR g.plan->>'request_digest' IS DISTINCT FROM g.request_digest OR history_gc_request_digest(g.plan->'request') IS DISTINCT FROM g.request_digest OR g.plan->>'plan_digest' IS DISTINCT FROM g.plan_digest OR history_gc_plan_digest(g.plan) IS DISTINCT FROM g.plan_digest OR (g.plan->>'prepared_at')::timestamptz IS DISTINCT FROM g.prepared_at THEN RAISE EXCEPTION USING ERRCODE='HG005',MESSAGE='gc immutable plan corrupt'; END IF;
END $$;
CREATE FUNCTION pulse_durable_distributed.history_gc_prepare(a jsonb) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pulse_durable_distributed,pg_temp AS $$
DECLARE ns text:=history_gc_auth(a); r pulse_durable_distributed.runs; p pulse_durable_distributed.retention_policies; s pulse_durable_distributed.storage_snapshots; g pulse_durable_distributed.history_gc_plans; q jsonb; answer jsonb; req text; pd text; t timestamptz; first_sample timestamptz; ver numeric; maxrows numeric; maxbytes numeric; expected numeric; firstrev numeric; lastrev numeric; n bigint:=0; size bigint:=0; entry record; parts text[]:=ARRAY['pulse-durable/history-gc/prefix/v1'];
BEGIN
 ver:=history_gc_uint(a,'policy_version',18446744073709551615); maxrows:=history_gc_uint(a,'max_rows',1024); maxbytes:=history_gc_uint(a,'max_bytes',16777216);
 IF jsonb_typeof(a->'policy_digest') IS DISTINCT FROM 'string' OR coalesce(a->>'policy_digest','') !~ '^[0-9a-f]{64}$' OR coalesce(a->>'request_digest','') !~ '^[0-9a-f]{64}$' THEN RAISE EXCEPTION USING ERRCODE='HG001',MESSAGE='invalid gc digest'; END IF;
 q:=jsonb_build_object('identity',jsonb_build_object('namespace',ns,'run_id',a->>'run_id'),'plan_id',a->>'plan_id','policy_version',ver::text,'policy_digest',a->>'policy_digest','max_rows',maxrows::text,'max_bytes',maxbytes::text);
 req:=history_gc_request_digest(q);
 IF req IS DISTINCT FROM a->>'request_digest' THEN RAISE EXCEPTION USING ERRCODE='HG001',MESSAGE='gc request digest mismatch'; END IF;
 SELECT * INTO p FROM pulse_durable_distributed.retention_policies WHERE namespace=ns FOR SHARE;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='HG002',MESSAGE='gc policy missing'; END IF;
 r:=history_gc_lock(a); t:=storage_clock(r,NULL); first_sample:=t;
 SELECT * INTO g FROM pulse_durable_distributed.history_gc_plans WHERE namespace=ns AND run_id=r.run_id AND plan_id=a->>'plan_id';
 IF FOUND THEN
  IF g.actor<>session_user THEN RAISE EXCEPTION USING ERRCODE='42501',MESSAGE='gc actor denied'; END IF;
  PERFORM history_gc_verify_plan(g);
  IF g.request_digest<>req THEN RAISE EXCEPTION USING ERRCODE='HG002',MESSAGE='gc request identity conflict'; END IF;
  RETURN g.plan;
 END IF;
 IF p.policy_version<>ver OR p.policy_digest<>a->>'policy_digest' THEN RAISE EXCEPTION USING ERRCODE='HG002',MESSAGE='gc policy changed'; END IF;
 s:=history_gc_check(r,p,t); expected:=r.history_gc_through+1; firstrev:=expected;
 FOR entry IN SELECT * FROM pulse_durable_distributed.batches WHERE namespace=ns AND run_id=r.run_id AND revision>r.history_gc_through AND revision<s.revision ORDER BY revision LIMIT maxrows::integer LOOP
  IF entry.revision<>expected OR encode(sha256(entry.wire),'hex') IS DISTINCT FROM entry.digest THEN RAISE EXCEPTION USING ERRCODE='HG004',MESSAGE='gc deletion prefix incomplete'; END IF;
  IF size+octet_length(entry.wire)>maxbytes THEN EXIT; END IF;
  parts:=array_append(parts,history_gc_digest(entry.revision::text,entry.digest,entry.parent_chain,entry.next_chain,octet_length(entry.wire)::text));
  n:=n+1; size:=size+octet_length(entry.wire); lastrev:=entry.revision; expected:=expected+1;
 END LOOP;
 IF n=0 THEN RAISE EXCEPTION USING ERRCODE='HG006',MESSAGE='no eligible gc prefix within budget'; END IF;
 t:=storage_clock(r,first_sample);
 answer:=jsonb_build_object('request',q,'request_digest',req,'head_revision',r.revision::text,'head_chain',r.head_chain,'snapshot_generation',s.generation::text,'snapshot_revision',s.revision::text,'snapshot_chain',s.chain,'snapshot_digest',s.digest,'before_floor',r.history_floor::text,'after_floor',s.revision::text,'first_revision',firstrev::text,'through_revision',lastrev::text,'prefix_digest',history_gc_digest(VARIADIC parts),'rows',n::text,'bytes',size::text,'prepared_at',t);
 pd:=history_gc_plan_digest(answer); answer:=answer||jsonb_build_object('plan_digest',pd);
 INSERT INTO pulse_durable_distributed.history_gc_plans(namespace,run_id,plan_id,actor,request_digest,plan_digest,plan,prepared_at) VALUES(ns,r.run_id,a->>'plan_id',session_user,req,pd,answer,t);
 UPDATE pulse_durable_distributed.runs SET maintenance_watermark=t WHERE namespace=ns AND run_id=r.run_id;
 RETURN answer;
END $$;
CREATE FUNCTION pulse_durable_distributed.history_gc_execute(a jsonb) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pulse_durable_distributed,pg_temp AS $$
DECLARE ns text:=history_gc_auth(a); r pulse_durable_distributed.runs; p pulse_durable_distributed.retention_policies; s pulse_durable_distributed.storage_snapshots; g pulse_durable_distributed.history_gc_plans; receipt pulse_durable_distributed.history_gc_receipts; plan jsonb; answer jsonb; t timestamptz; first_sample timestamptz; expected numeric; lastrev numeric; n bigint:=0; size bigint:=0; dn bigint; db bigint; entry record; parts text[]:=ARRAY['pulse-durable/history-gc/prefix/v1']; rd text;
BEGIN
 IF jsonb_typeof(a->'plan_digest') IS DISTINCT FROM 'string' OR coalesce(a->>'plan_digest','') !~ '^[0-9a-f]{64}$' THEN RAISE EXCEPTION USING ERRCODE='HG001',MESSAGE='invalid gc plan digest'; END IF;
 SELECT * INTO p FROM pulse_durable_distributed.retention_policies WHERE namespace=ns FOR SHARE;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='HG002',MESSAGE='gc policy missing'; END IF;
 r:=history_gc_lock(a); t:=storage_clock(r,NULL); first_sample:=t;
 SELECT * INTO g FROM pulse_durable_distributed.history_gc_plans WHERE namespace=ns AND run_id=r.run_id AND plan_id=a->>'plan_id';
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='HG006',MESSAGE='gc plan missing'; END IF;
 IF g.actor<>session_user THEN RAISE EXCEPTION USING ERRCODE='42501',MESSAGE='gc actor denied'; END IF;
 PERFORM history_gc_verify_plan(g); plan:=g.plan;
 IF g.plan_digest<>a->>'plan_digest' THEN RAISE EXCEPTION USING ERRCODE='HG002',MESSAGE='gc plan identity conflict'; END IF;
 -- 精确已提交结果先于当前策略/代际校验；策略变化不能使已确认回执不可核对。
 SELECT * INTO receipt FROM pulse_durable_distributed.history_gc_receipts WHERE namespace=ns AND run_id=r.run_id AND plan_id=g.plan_id;
 IF FOUND THEN
  IF NOT g.executed OR receipt.result->>'plan_digest' IS DISTINCT FROM g.plan_digest OR receipt.receipt_digest IS DISTINCT FROM history_gc_receipt_digest(receipt.result) OR receipt.result->>'receipt_digest' IS DISTINCT FROM receipt.receipt_digest OR receipt.occurred_at>t THEN RAISE EXCEPTION USING ERRCODE='HG005',MESSAGE='gc receipt corrupt'; END IF;
  RETURN jsonb_build_object('state','executed','plan',plan,'receipt',receipt.result);
 END IF;
 IF g.executed THEN RAISE EXCEPTION USING ERRCODE='HG005',MESSAGE='gc executed receipt missing'; END IF;
 IF p.policy_version::text IS DISTINCT FROM plan->'request'->>'policy_version' OR p.policy_digest IS DISTINCT FROM plan->'request'->>'policy_digest' OR r.revision::text IS DISTINCT FROM plan->>'head_revision' OR r.head_chain IS DISTINCT FROM plan->>'head_chain' OR r.history_floor::text IS DISTINCT FROM plan->>'before_floor' OR (r.history_gc_through+1)::text IS DISTINCT FROM plan->>'first_revision' THEN RAISE EXCEPTION USING ERRCODE='HG002',MESSAGE='gc authority changed'; END IF;
 s:=history_gc_check(r,p,t);
 IF s.generation::text IS DISTINCT FROM plan->>'snapshot_generation' OR s.revision::text IS DISTINCT FROM plan->>'snapshot_revision' OR s.chain IS DISTINCT FROM plan->>'snapshot_chain' OR s.digest IS DISTINCT FROM plan->>'snapshot_digest' THEN RAISE EXCEPTION USING ERRCODE='HG002',MESSAGE='gc snapshot changed'; END IF;
 expected:=(plan->>'first_revision')::numeric; lastrev:=(plan->>'through_revision')::numeric;
 IF lastrev>=s.revision OR lastrev<expected OR lastrev-expected+1>1024 THEN RAISE EXCEPTION USING ERRCODE='HG005',MESSAGE='gc plan range invalid'; END IF;
 FOR entry IN SELECT * FROM pulse_durable_distributed.batches WHERE namespace=ns AND run_id=r.run_id AND revision>=expected AND revision<=lastrev ORDER BY revision LOOP
  IF entry.revision<>expected OR encode(sha256(entry.wire),'hex') IS DISTINCT FROM entry.digest THEN RAISE EXCEPTION USING ERRCODE='HG004',MESSAGE='gc deletion prefix corrupt'; END IF;
  parts:=array_append(parts,history_gc_digest(entry.revision::text,entry.digest,entry.parent_chain,entry.next_chain,octet_length(entry.wire)::text));
  n:=n+1; size:=size+octet_length(entry.wire); expected:=expected+1;
 END LOOP;
 IF expected<>lastrev+1 OR n::text IS DISTINCT FROM plan->>'rows' OR size::text IS DISTINCT FROM plan->>'bytes' OR n>(plan->'request'->>'max_rows')::numeric OR size>(plan->'request'->>'max_bytes')::numeric OR history_gc_digest(VARIADIC parts) IS DISTINCT FROM plan->>'prefix_digest' THEN RAISE EXCEPTION USING ERRCODE='HG002',MESSAGE='gc prefix changed'; END IF;
 t:=storage_clock(r,first_sample); PERFORM history_gc_protect(r,t);
 IF t<g.prepared_at THEN RAISE EXCEPTION USING ERRCODE='PD007',MESSAGE='gc clock regressed'; END IF;
 WITH deleted AS (DELETE FROM pulse_durable_distributed.batches WHERE namespace=ns AND run_id=r.run_id AND revision>r.history_gc_through AND revision<=lastrev RETURNING octet_length(wire)::bigint AS wire_bytes) SELECT count(*),coalesce(sum(deleted.wire_bytes),0) INTO dn,db FROM deleted;
 IF dn<>n OR db<>size THEN RAISE EXCEPTION USING ERRCODE='HG005',MESSAGE='gc deleted accounting mismatch'; END IF;
 -- 删除之后仍在同一事务中再次采样；回拨导致整个删除回滚。
 t:=storage_clock(r,t);
 UPDATE pulse_durable_distributed.runs SET history_floor=s.revision,history_gc_through=lastrev,maintenance_watermark=t WHERE namespace=ns AND run_id=r.run_id;
 answer:=jsonb_build_object('identity',jsonb_build_object('namespace',ns,'run_id',r.run_id),'plan_id',g.plan_id,'plan_digest',g.plan_digest,'rows',n::text,'bytes',size::text,'history_floor',s.revision::text,'occurred_at',t);
 rd:=history_gc_receipt_digest(answer); answer:=answer||jsonb_build_object('receipt_digest',rd);
 INSERT INTO pulse_durable_distributed.history_gc_receipts VALUES(ns,r.run_id,g.plan_id,rd,answer,t);
 UPDATE pulse_durable_distributed.history_gc_plans SET executed=true WHERE namespace=ns AND run_id=r.run_id AND plan_id=g.plan_id;
 RETURN jsonb_build_object('state','executed','plan',plan,'receipt',answer);
END $$;
CREATE FUNCTION pulse_durable_distributed.history_gc_find(a jsonb) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pulse_durable_distributed,pg_temp AS $$
DECLARE ns text:=history_gc_auth(a); r pulse_durable_distributed.runs; p pulse_durable_distributed.retention_policies; g pulse_durable_distributed.history_gc_plans; receipt pulse_durable_distributed.history_gc_receipts; t timestamptz;
BEGIN
 IF jsonb_typeof(a->'request_digest') IS DISTINCT FROM 'string' OR coalesce(a->>'request_digest','') !~ '^[0-9a-f]{64}$' THEN RAISE EXCEPTION USING ERRCODE='HG001',MESSAGE='invalid gc query digest'; END IF;
 SELECT * INTO p FROM pulse_durable_distributed.retention_policies WHERE namespace=ns FOR SHARE;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='HG002',MESSAGE='gc policy missing'; END IF;
 r:=history_gc_lock(a); t:=storage_clock(r,NULL);
 SELECT * INTO g FROM pulse_durable_distributed.history_gc_plans WHERE namespace=ns AND run_id=r.run_id AND plan_id=a->>'plan_id';
 IF NOT FOUND THEN RETURN jsonb_build_object('state','absent','plan',NULL,'receipt',NULL); END IF;
 IF g.actor<>session_user THEN RAISE EXCEPTION USING ERRCODE='42501',MESSAGE='gc actor denied'; END IF;
 PERFORM history_gc_verify_plan(g);
 IF g.request_digest<>a->>'request_digest' THEN RAISE EXCEPTION USING ERRCODE='HG002',MESSAGE='gc query identity conflict'; END IF;
 SELECT * INTO receipt FROM pulse_durable_distributed.history_gc_receipts WHERE namespace=ns AND run_id=r.run_id AND plan_id=g.plan_id;
 IF NOT FOUND THEN
  IF g.executed THEN RAISE EXCEPTION USING ERRCODE='HG005',MESSAGE='gc executed receipt missing'; END IF;
  RETURN jsonb_build_object('state','prepared','plan',g.plan,'receipt',NULL);
 END IF;
 IF NOT g.executed OR receipt.result->>'plan_digest' IS DISTINCT FROM g.plan_digest OR receipt.receipt_digest IS DISTINCT FROM history_gc_receipt_digest(receipt.result) OR receipt.result->>'receipt_digest' IS DISTINCT FROM receipt.receipt_digest OR receipt.occurred_at>t THEN RAISE EXCEPTION USING ERRCODE='HG005',MESSAGE='gc receipt corrupt'; END IF;
 RETURN jsonb_build_object('state','executed','plan',g.plan,'receipt',receipt.result);
END $$;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA pulse_durable_distributed FROM PUBLIC;
