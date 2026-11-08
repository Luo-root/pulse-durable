-- 每次调用一个事务；只锁身份和 run，不锁 lease，也不删除历史数据。
CREATE FUNCTION pulse_durable_distributed.storage_state(r pulse_durable_distributed.runs) RETURNS jsonb LANGUAGE sql VOLATILE SET search_path=pg_catalog,pulse_durable_distributed,pg_temp AS $$
 SELECT jsonb_build_object('generation',r.representation_generation::text,'base_revision',coalesce(s.revision,1)::text,'base_chain',coalesce(s.chain,encode(sha256(convert_to('pulse-durable/chain/seed/v1','UTF8')||decode('00','hex')||decode(r.snapshot_digest,'hex')||int8send(1::bigint)),'hex')),'head_revision',r.revision::text,'head_chain',r.head_chain)
 FROM (SELECT 1) x LEFT JOIN pulse_durable_distributed.storage_snapshots s ON s.namespace=r.namespace AND s.run_id=r.run_id AND s.generation=r.active_snapshot
$$;
-- 为历史引用和维护请求验证完整恢复路径，而不把单条链摘要存在当作可恢复。
-- history_floor 只限制新请求；精确旧回执在调用此函数前裁决，不从这里获得新权限。
CREATE FUNCTION pulse_durable_distributed.history_path(r pulse_durable_distributed.runs, target numeric) RETURNS text LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pulse_durable_distributed,pg_temp AS $$
DECLARE s pulse_durable_distributed.storage_snapshots; base numeric:=1; anchor text; expected numeric; entry record; rec bytea:=r.record; dg text:=r.snapshot_digest; computed text;
BEGIN
 IF target IS NULL OR trunc(target)<>target OR target<r.history_floor OR target>r.revision OR r.revision=0 THEN RAISE EXCEPTION USING ERRCODE='HG004',MESSAGE='history outside retained range'; END IF;
 SELECT * INTO s FROM pulse_durable_distributed.storage_snapshots WHERE namespace=r.namespace AND run_id=r.run_id AND revision>=r.history_floor AND revision<=target ORDER BY revision DESC,generation DESC LIMIT 1;
 IF FOUND THEN base:=s.revision; anchor:=s.chain; rec:=s.record; dg:=s.digest;
 ELSE
  IF r.history_floor>1 THEN RAISE EXCEPTION USING ERRCODE='HG004',MESSAGE='retained history base missing'; END IF;
  anchor:=encode(sha256(convert_to('pulse-durable/chain/seed/v1','UTF8')||decode('00','hex')||decode(r.snapshot_digest,'hex')||int8send(1::bigint)),'hex');
 END IF;
 IF rec IS NULL OR dg IS NULL OR encode(sha256(rec),'hex') IS DISTINCT FROM dg OR coalesce(anchor,'') !~ '^[0-9a-f]{64}$' THEN RAISE EXCEPTION USING ERRCODE='HG004',MESSAGE='retained history base corrupt'; END IF;
 expected:=base+1;
 FOR entry IN SELECT * FROM pulse_durable_distributed.batches WHERE namespace=r.namespace AND run_id=r.run_id AND revision>base AND revision<=target ORDER BY revision LOOP
  IF entry.revision<>expected OR entry.parent_chain IS DISTINCT FROM anchor OR encode(sha256(entry.wire),'hex') IS DISTINCT FROM entry.digest THEN RAISE EXCEPTION USING ERRCODE='HG004',MESSAGE='retained history gap or corrupt batch'; END IF;
  computed:=encode(sha256(convert_to('pulse-durable/chain/batch/v1','UTF8')||decode('00','hex')||decode(anchor,'hex')||decode(entry.digest,'hex')||decode(lpad(to_hex(trunc(expected/4294967296)::bigint),8,'0')||lpad(to_hex(mod(expected,4294967296)::bigint),8,'0'),'hex')),'hex');
  IF entry.next_chain IS DISTINCT FROM computed THEN RAISE EXCEPTION USING ERRCODE='HG004',MESSAGE='retained history chain corrupt'; END IF;
  anchor:=computed; expected:=expected+1;
 END LOOP;
 IF expected<>target+1 OR (target=r.revision AND anchor IS DISTINCT FROM r.head_chain) THEN RAISE EXCEPTION USING ERRCODE='HG004',MESSAGE='retained history target missing'; END IF;
 RETURN anchor;
END $$;
CREATE FUNCTION pulse_durable_distributed.storage_image(r pulse_durable_distributed.runs, through_rev numeric, active boolean) RETURNS jsonb LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pulse_durable_distributed,pg_temp AS $$
DECLARE s pulse_durable_distributed.storage_snapshots; base_rev numeric:=1; base_chain text; bytes bytea:=r.record; dg text:=r.snapshot_digest; anchor text;
BEGIN
 IF active AND r.active_snapshot IS NOT NULL THEN
  SELECT * INTO STRICT s FROM pulse_durable_distributed.storage_snapshots WHERE namespace=r.namespace AND run_id=r.run_id AND generation=r.active_snapshot;
  base_rev:=s.revision; base_chain:=s.chain; bytes:=s.record; dg:=s.digest;
 ELSE base_chain:=encode(sha256(convert_to('pulse-durable/chain/seed/v1','UTF8')||decode('00','hex')||decode(r.snapshot_digest,'hex')||int8send(1::bigint)),'hex'); END IF;
 IF through_rev=base_rev THEN anchor:=base_chain;
 ELSE SELECT next_chain INTO anchor FROM pulse_durable_distributed.batches WHERE namespace=r.namespace AND run_id=r.run_id AND revision=through_rev; END IF;
 IF through_rev=r.revision THEN anchor:=r.head_chain; END IF;
 IF through_rev<r.history_floor THEN RAISE EXCEPTION USING ERRCODE='HG004',MESSAGE='requested history expired'; END IF;
 IF anchor IS NULL OR through_rev<base_rev OR through_rev>r.revision THEN RAISE EXCEPTION USING ERRCODE='PD005',MESSAGE='invalid path'; END IF;
 RETURN snapshot(r)||jsonb_build_object('revision',through_rev::text,'record',encode(bytes,'base64'),'snapshot_digest',dg,'base_revision',base_rev::text,'base_chain',base_chain,'head_chain',anchor,'batches',coalesce((SELECT jsonb_agg(jsonb_build_object('revision',revision::text,'wire',encode(wire,'base64'),'digest',digest,'parent_chain',parent_chain,'next_chain',next_chain) ORDER BY revision) FROM pulse_durable_distributed.batches WHERE namespace=r.namespace AND run_id=r.run_id AND revision>base_rev AND revision<=through_rev),'[]'::jsonb));
END $$;
-- 内部 helper 仅鉴权并按身份 advisory → run FOR UPDATE 顺序加锁，不分派阶段。
CREATE FUNCTION pulse_durable_distributed.storage_lock(a jsonb) RETURNS pulse_durable_distributed.runs LANGUAGE plpgsql SET search_path=pg_catalog,pulse_durable_distributed,pg_temp AS $$
DECLARE ns text:=a->>'namespace'; rid text:=a->>'run_id'; r pulse_durable_distributed.runs;
BEGIN
 IF jsonb_typeof(a->'snapshot_budget_version') IS DISTINCT FROM 'number' OR a->>'snapshot_budget_version' IS DISTINCT FROM '2' THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='unsupported snapshot budget client'; END IF;
 IF NOT check_schema() THEN RAISE EXCEPTION USING ERRCODE='42501',MESSAGE='unsafe schema or credentials'; END IF;
 IF coalesce(octet_length(ns),0) NOT BETWEEN 1 AND 256 OR coalesce(octet_length(rid),0) NOT BETWEEN 1 AND 256 OR NOT EXISTS(SELECT 1 FROM pulse_durable_distributed.access x WHERE x.namespace=ns AND x.permission='maintain' AND pg_has_role(session_user,x.role_name,'MEMBER')) THEN RAISE EXCEPTION USING ERRCODE='42501',MESSAGE='maintenance access denied'; END IF;
 IF a ?| ARRAY['epoch','worker_id','boot_id','lease_id'] THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='worker token forbidden'; END IF;
 PERFORM pg_advisory_xact_lock(hashtextextended(jsonb_build_array(ns,rid)::text,5788618723189269578));
 SELECT * INTO r FROM pulse_durable_distributed.runs WHERE namespace=ns AND run_id=rid FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='PD003',MESSAGE='not found'; END IF;
 RETURN r;
END $$;
CREATE FUNCTION pulse_durable_distributed.storage_clock(r pulse_durable_distributed.runs, first_sample timestamptz) RETURNS timestamptz LANGUAGE plpgsql VOLATILE SET search_path=pg_catalog,pulse_durable_distributed,pg_temp AS $$
DECLARE t timestamptz:=clock_timestamp();
BEGIN
 IF t<first_sample OR t<r.updated_at OR t<r.maintenance_watermark OR t<r.receipt_observed_through THEN RAISE EXCEPTION USING ERRCODE='PD007',MESSAGE='clock regression'; END IF;
 RETURN t;
END $$;
CREATE FUNCTION pulse_durable_distributed.storage_inspect(a jsonb) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pulse_durable_distributed,pg_temp AS $$
DECLARE r pulse_durable_distributed.runs;
BEGIN
 r:=storage_lock(a);
 PERFORM storage_clock(r,NULL);
 IF r.revision=0 THEN RAISE EXCEPTION USING ERRCODE='PD003',MESSAGE='no checkpoint'; END IF;
 RETURN storage_state(r);
END $$;
CREATE FUNCTION pulse_durable_distributed.storage_compact_begin(a jsonb) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pulse_durable_distributed,pg_temp AS $$
DECLARE ns text:=a->>'namespace'; rid text:=a->>'run_id'; mid text:=a->>'mutation_id'; md text:=a->>'mutation_digest'; r pulse_durable_distributed.runs; g pulse_durable_distributed.maintenance_guards; t timestamptz; rev numeric; gen bigint; anchor text; answer jsonb;
BEGIN
 r:=storage_lock(a); t:=storage_clock(r,NULL);
 IF coalesce(octet_length(mid),0) NOT BETWEEN 1 AND 256 OR coalesce(md,'') !~ '^[0-9a-f]{64}$' THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='invalid maintenance mutation'; END IF;
 SELECT * INTO g FROM pulse_durable_distributed.maintenance_guards WHERE namespace=ns AND run_id=rid AND mutation_id=mid;
 IF FOUND AND g.digest IS DISTINCT FROM md THEN RAISE EXCEPTION USING ERRCODE='PD005',MESSAGE='maintenance mutation conflict'; END IF;
 SELECT result INTO answer FROM pulse_durable_distributed.maintenance_receipts WHERE namespace=ns AND run_id=rid AND mutation_id=mid;
 IF FOUND THEN RETURN jsonb_build_object('status','published','result',answer); END IF;
 -- 永久 guard 不因取消或过期而释放。
 IF g.mutation_id IS NOT NULL THEN RAISE EXCEPTION USING ERRCODE='PD005',MESSAGE='maintenance mutation already reserved'; END IF;
 rev:=(a->>'through_revision')::numeric; gen:=(a->>'generation')::bigint;
 IF md IS DISTINCT FROM encode(sha256(convert_to('pulse-durable/maintenance/compact/v1','UTF8')||decode('00','hex')||convert_to(octet_length(ns)::text||':'||ns||octet_length(rid)::text||':'||rid||':'||gen::text||':'||rev::text,'UTF8')),'hex') THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='maintenance plan digest'; END IF;
 IF rev IS NULL OR trunc(rev)<>rev OR rev<1 OR rev>r.revision OR gen IS DISTINCT FROM r.representation_generation OR gen=9223372036854775807 OR rev<(storage_state(r)->>'base_revision')::numeric THEN RAISE EXCEPTION USING ERRCODE='PD005',MESSAGE='representation conflict'; END IF;
 IF r.history_floor>1 THEN
  anchor:=history_path(r,rev);
 ELSE
  IF rev=1 THEN anchor:=encode(sha256(convert_to('pulse-durable/chain/seed/v1','UTF8')||decode('00','hex')||decode(r.snapshot_digest,'hex')||int8send(1::bigint)),'hex'); ELSE SELECT next_chain INTO anchor FROM pulse_durable_distributed.batches WHERE namespace=ns AND run_id=rid AND revision=rev; END IF;
 END IF;
 IF anchor IS NULL THEN RAISE EXCEPTION USING ERRCODE='PD005',MESSAGE='source missing'; END IF;
 INSERT INTO pulse_durable_distributed.maintenance_guards VALUES(ns,rid,mid,md,'compact_storage');
 INSERT INTO pulse_durable_distributed.build_pins VALUES(ns,rid,mid,session_user,gen,r.revision,r.head_chain,rev,anchor,t+interval '10 minutes',false);
 UPDATE pulse_durable_distributed.runs SET maintenance_watermark=t WHERE namespace=ns AND run_id=rid;
 RETURN jsonb_build_object('status','building','image',storage_image(r,rev,true),'state',storage_state(r));
END $$;
CREATE FUNCTION pulse_durable_distributed.storage_compact_find(a jsonb) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pulse_durable_distributed,pg_temp AS $$
DECLARE ns text:=a->>'namespace'; rid text:=a->>'run_id'; mid text:=a->>'mutation_id'; md text:=a->>'mutation_digest'; r pulse_durable_distributed.runs; p pulse_durable_distributed.build_pins; g pulse_durable_distributed.maintenance_guards; t timestamptz; answer jsonb;
BEGIN
 r:=storage_lock(a); t:=storage_clock(r,NULL);
 IF coalesce(octet_length(mid),0) NOT BETWEEN 1 AND 256 OR coalesce(md,'') !~ '^[0-9a-f]{64}$' THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='invalid maintenance mutation'; END IF;
 SELECT * INTO g FROM pulse_durable_distributed.maintenance_guards WHERE namespace=ns AND run_id=rid AND mutation_id=mid;
 IF FOUND AND g.digest IS DISTINCT FROM md THEN RAISE EXCEPTION USING ERRCODE='PD005',MESSAGE='maintenance mutation conflict'; END IF;
 SELECT result INTO answer FROM pulse_durable_distributed.maintenance_receipts WHERE namespace=ns AND run_id=rid AND mutation_id=mid;
 IF FOUND THEN RETURN jsonb_build_object('status','published','result',answer); END IF;
 SELECT * INTO p FROM pulse_durable_distributed.build_pins WHERE namespace=ns AND run_id=rid AND mutation_id=mid;
 RETURN jsonb_build_object('status',CASE WHEN g.mutation_id IS NULL THEN 'absent' WHEN p.closed THEN 'closed' WHEN p.expires_at<=t THEN 'expired' ELSE 'building' END,'result',NULL);
END $$;
CREATE FUNCTION pulse_durable_distributed.storage_compact_abort(a jsonb) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pulse_durable_distributed,pg_temp AS $$
DECLARE ns text:=a->>'namespace'; rid text:=a->>'run_id'; mid text:=a->>'mutation_id'; md text:=a->>'mutation_digest'; r pulse_durable_distributed.runs; p pulse_durable_distributed.build_pins; g pulse_durable_distributed.maintenance_guards; t timestamptz; answer jsonb;
BEGIN
 r:=storage_lock(a); t:=storage_clock(r,NULL);
 IF coalesce(octet_length(mid),0) NOT BETWEEN 1 AND 256 OR coalesce(md,'') !~ '^[0-9a-f]{64}$' THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='invalid maintenance mutation'; END IF;
 SELECT * INTO g FROM pulse_durable_distributed.maintenance_guards WHERE namespace=ns AND run_id=rid AND mutation_id=mid;
 IF FOUND AND g.digest IS DISTINCT FROM md THEN RAISE EXCEPTION USING ERRCODE='PD005',MESSAGE='maintenance mutation conflict'; END IF;
 SELECT result INTO answer FROM pulse_durable_distributed.maintenance_receipts WHERE namespace=ns AND run_id=rid AND mutation_id=mid;
 IF FOUND THEN RETURN jsonb_build_object('status','published','result',answer); END IF;
 SELECT * INTO p FROM pulse_durable_distributed.build_pins WHERE namespace=ns AND run_id=rid AND mutation_id=mid;
 IF p.mutation_id IS NULL OR p.holder<>session_user OR p.closed OR p.expires_at<=t THEN RAISE EXCEPTION USING ERRCODE='PD005',MESSAGE='build pin unavailable'; END IF;
 UPDATE pulse_durable_distributed.build_pins SET closed=true WHERE namespace=ns AND run_id=rid AND mutation_id=mid;
 UPDATE pulse_durable_distributed.runs SET maintenance_watermark=t WHERE namespace=ns AND run_id=rid;
 RETURN jsonb_build_object('status','closed','result',NULL);
END $$;
CREATE FUNCTION pulse_durable_distributed.storage_compact_publish(a jsonb) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pulse_durable_distributed,pg_temp AS $$
DECLARE ns text:=a->>'namespace'; rid text:=a->>'run_id'; mid text:=a->>'mutation_id'; md text:=a->>'mutation_digest'; r pulse_durable_distributed.runs; p pulse_durable_distributed.build_pins; g pulse_durable_distributed.maintenance_guards; t timestamptz; first_sample timestamptz; b bytea; j jsonb; answer jsonb; suffix_bytes bigint; suffix_count bigint;
BEGIN
 r:=storage_lock(a); t:=storage_clock(r,NULL); first_sample:=t;
 IF coalesce(octet_length(mid),0) NOT BETWEEN 1 AND 256 OR coalesce(md,'') !~ '^[0-9a-f]{64}$' THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='invalid maintenance mutation'; END IF;
 SELECT * INTO g FROM pulse_durable_distributed.maintenance_guards WHERE namespace=ns AND run_id=rid AND mutation_id=mid;
 IF FOUND AND g.digest IS DISTINCT FROM md THEN RAISE EXCEPTION USING ERRCODE='PD005',MESSAGE='maintenance mutation conflict'; END IF;
 SELECT result INTO answer FROM pulse_durable_distributed.maintenance_receipts WHERE namespace=ns AND run_id=rid AND mutation_id=mid;
 IF FOUND THEN RETURN jsonb_build_object('status','published','result',answer); END IF;
 SELECT * INTO p FROM pulse_durable_distributed.build_pins WHERE namespace=ns AND run_id=rid AND mutation_id=mid;
 IF p.mutation_id IS NULL OR p.holder<>session_user OR p.closed OR p.expires_at<=t THEN RAISE EXCEPTION USING ERRCODE='PD005',MESSAGE='build pin unavailable'; END IF;
 IF r.representation_generation<>p.generation OR r.revision<p.source_revision OR (r.revision=p.source_revision AND r.head_chain IS DISTINCT FROM p.source_chain) OR (r.revision>p.source_revision AND ((p.source_revision=1 AND p.source_chain IS DISTINCT FROM encode(sha256(convert_to('pulse-durable/chain/seed/v1','UTF8')||decode('00','hex')||decode(r.snapshot_digest,'hex')||int8send(1::bigint)),'hex')) OR (p.source_revision>1 AND NOT EXISTS(SELECT 1 FROM pulse_durable_distributed.batches WHERE namespace=ns AND run_id=rid AND revision=p.source_revision AND next_chain=p.source_chain)))) THEN RAISE EXCEPTION USING ERRCODE='PD005',MESSAGE='source head conflict'; END IF;
 -- source_revision=1 无 batch 行；有并发 suffix 时也验证 seed。
 b:=decode(a->>'record','base64');
 IF b IS NULL OR octet_length(b) NOT BETWEEN 1 AND 16777216 OR encode(sha256(b),'hex') IS DISTINCT FROM a->>'snapshot_digest' OR a->>'chain' IS DISTINCT FROM p.through_chain THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='invalid synthesized snapshot'; END IF;
 IF NOT (convert_from(b,'UTF8') IS JSON OBJECT WITH UNIQUE KEYS) THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='invalid snapshot JSON'; END IF;
 j:=convert_from(b,'UTF8')::jsonb;
 -- 当前后端只发布 snapshot2 的完整基础事实；未知证据组件在此边界
 -- 明确拒绝，不由维护入口忽略后再重写。schema 与精确大小共用预算规则。
 IF snapshot_value_bytes(j,'snapshot',false) IS DISTINCT FROM octet_length(b) OR j->>'version' IS DISTINCT FROM '2' THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='unsupported synthesized snapshot'; END IF;
 IF jsonb_typeof(j->'record'->'run_id') IS DISTINCT FROM 'string' OR jsonb_typeof(j->'record'->'revision') IS DISTINCT FROM 'number' OR jsonb_typeof(j->'record'->'version') IS DISTINCT FROM 'string' OR jsonb_typeof(j->'record'->'definition') IS DISTINCT FROM 'string' OR jsonb_typeof(j->'record'->'inputs') IS DISTINCT FROM 'string' OR jsonb_typeof(j->'record'->'root_scope') IS DISTINCT FROM 'string' OR jsonb_typeof(j->'record'->'scopes') IS DISTINCT FROM 'object' OR jsonb_typeof(j->'record'->'attempts') IS DISTINCT FROM 'object' OR j->'record'->>'run_id' IS DISTINCT FROM rid OR j->'record'->>'revision' IS DISTINCT FROM p.through_revision::text OR j->'record'->>'version' IS DISTINCT FROM r.route->>'business_version' OR j->'record'->>'definition' IS DISTINCT FROM r.route->>'definition' OR j->'record'->>'inputs' IS DISTINCT FROM (convert_from(r.task,'UTF8')::jsonb)->>'inputs' OR j->'record'->>'root_scope' IS DISTINCT FROM r.root_scope THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='snapshot identity'; END IF;
 INSERT INTO pulse_durable_distributed.storage_snapshots VALUES(ns,rid,p.generation+1,p.through_revision,p.through_chain,a->>'snapshot_digest',b);
 -- run 锁同时隔离 Apply；发布时重算实际 suffix 和预算，不改变业务 head/lease/events。
 SELECT coalesce(sum(octet_length(wire)),0),count(*) INTO suffix_bytes,suffix_count FROM pulse_durable_distributed.batches WHERE namespace=ns AND run_id=rid AND revision>p.through_revision AND revision<=r.revision;
 t:=storage_clock(r,first_sample);
 IF p.expires_at<=t THEN RAISE EXCEPTION USING ERRCODE='PD005',MESSAGE='build pin expired'; END IF;
 UPDATE pulse_durable_distributed.runs SET active_snapshot=p.generation+1,representation_generation=p.generation+1,active_log_bytes=suffix_bytes,active_log_count=suffix_count,maintenance_watermark=t WHERE namespace=ns AND run_id=rid RETURNING * INTO r;
 answer:=jsonb_build_object('mutation_id',mid,'mutation_digest',md,'operation','compact_storage','state',storage_state(r),'occurred_at',t);
 INSERT INTO pulse_durable_distributed.maintenance_receipts VALUES(ns,rid,mid,answer);
 UPDATE pulse_durable_distributed.build_pins SET closed=true WHERE namespace=ns AND run_id=rid AND mutation_id=mid;
 RETURN jsonb_build_object('status','published','result',answer);
END $$;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA pulse_durable_distributed FROM PUBLIC;
