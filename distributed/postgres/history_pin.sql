-- 历史引用、永久幂等保护与不可变回执独立于物理构建固定点。
CREATE TABLE pulse_durable_distributed.history_pins(
 namespace text NOT NULL, run_id text NOT NULL, pin_id text NOT NULL,
 revision numeric(20,0) NOT NULL CHECK(revision BETWEEN 1 AND 18446744073709551615), chain text NOT NULL CHECK(chain ~ '^[0-9a-f]{64}$'),
 version numeric(20,0) NOT NULL CHECK(version BETWEEN 1 AND 18446744073709551615), holder name NOT NULL, holder_digest text NOT NULL CHECK(holder_digest ~ '^[0-9a-f]{64}$'),
 mutation_id text NOT NULL, mutation_digest text NOT NULL, ttl_seconds bigint CHECK(ttl_seconds BETWEEN 1 AND 9223372036), expires_at timestamptz, released boolean NOT NULL DEFAULT false,
 CHECK((ttl_seconds IS NULL)=(expires_at IS NULL)), CHECK(expires_at IS NULL OR isfinite(expires_at)),
 PRIMARY KEY(namespace,run_id,pin_id), FOREIGN KEY(namespace,run_id) REFERENCES pulse_durable_distributed.runs(namespace,run_id));
CREATE TABLE pulse_durable_distributed.pin_guards(
 namespace text NOT NULL, run_id text NOT NULL, mutation_id text NOT NULL, pin_id text NOT NULL, operation text NOT NULL CHECK(operation IN ('create','renew','release')), digest text NOT NULL CHECK(digest ~ '^[0-9a-f]{64}$'), actor name NOT NULL, requires_maintain boolean NOT NULL,
 PRIMARY KEY(namespace,run_id,mutation_id), FOREIGN KEY(namespace,run_id,pin_id) REFERENCES pulse_durable_distributed.history_pins(namespace,run_id,pin_id));
CREATE TABLE pulse_durable_distributed.pin_receipts(
 namespace text NOT NULL, run_id text NOT NULL, mutation_id text NOT NULL, result jsonb NOT NULL,
 PRIMARY KEY(namespace,run_id,mutation_id), FOREIGN KEY(namespace,run_id,mutation_id) REFERENCES pulse_durable_distributed.pin_guards(namespace,run_id,mutation_id));
-- Go encoding/json 的字符串转义（含 HTML 与 U+2028/U+2029），不依赖 jsonb 键排序。
CREATE FUNCTION pulse_durable_distributed.pin_quote(v text) RETURNS text LANGUAGE sql IMMUTABLE SET search_path=pg_catalog,pulse_durable_distributed,pg_temp AS $$
 SELECT replace(replace(replace(replace(replace(to_json(v)::text,'<','\u003c'),'>','\u003e'),'&','\u0026'),chr(8232),'\u2028'),chr(8233),'\u2029')
$$;
CREATE FUNCTION pulse_durable_distributed.pin_permission(ns text, perm text) RETURNS boolean LANGUAGE sql STABLE SET search_path=pg_catalog,pulse_durable_distributed,pg_temp AS $$
 SELECT EXISTS(SELECT 1 FROM pulse_durable_distributed.access x WHERE x.namespace=ns AND x.permission=perm AND pg_has_role(session_user,x.role_name,'MEMBER'))
$$;
-- 只校验形状并加身份/run 锁；权限由各公开入口独立声明。
CREATE FUNCTION pulse_durable_distributed.pin_lock(a jsonb) RETURNS pulse_durable_distributed.runs LANGUAGE plpgsql SET search_path=pg_catalog,pulse_durable_distributed,pg_temp AS $$
DECLARE ns text:=a->>'namespace'; rid text:=a->>'run_id'; r pulse_durable_distributed.runs;
BEGIN
 IF NOT check_schema() THEN RAISE EXCEPTION USING ERRCODE='42501',MESSAGE='unsafe schema or credentials'; END IF;
 IF jsonb_typeof(a->'namespace') IS DISTINCT FROM 'string' OR jsonb_typeof(a->'run_id') IS DISTINCT FROM 'string' OR jsonb_typeof(a->'pin_id') IS DISTINCT FROM 'string' OR coalesce(octet_length(ns),0) NOT BETWEEN 1 AND 256 OR coalesce(octet_length(rid),0) NOT BETWEEN 1 AND 256 OR coalesce(octet_length(a->>'pin_id'),0) NOT BETWEEN 1 AND 256 THEN RAISE EXCEPTION USING ERRCODE='PH001',MESSAGE='invalid pin identity'; END IF;
 PERFORM pg_advisory_xact_lock(hashtextextended(jsonb_build_array(ns,rid)::text,5788618723189269578));
 SELECT * INTO r FROM pulse_durable_distributed.runs WHERE namespace=ns AND run_id=rid FOR UPDATE;
 RETURN r;
END $$;
-- 重建完整规范请求摘要，所有整数先验证规范十进制，不经过浮点数。
CREATE FUNCTION pulse_durable_distributed.pin_validate(a jsonb, operation text) RETURNS void LANGUAGE plpgsql SET search_path=pg_catalog,pulse_durable_distributed,pg_temp AS $$
DECLARE k text; v text; wire text; rev text:=a->>'revision'; ver text:=a->>'expected_version'; ttl text:=a->>'ttl_ns';
BEGIN
 FOREACH k IN ARRAY ARRAY['mutation_id','holder_digest','mutation_digest','revision','chain','expected_version'] LOOP
  IF jsonb_typeof(a->k) IS DISTINCT FROM 'string' THEN RAISE EXCEPTION USING ERRCODE='PH001',MESSAGE='invalid pin field'; END IF;
 END LOOP;
 IF coalesce(octet_length(a->>'mutation_id'),0) NOT BETWEEN 1 AND 256 OR a->>'holder_digest' !~ '^[0-9a-f]{64}$' OR a->>'mutation_digest' !~ '^[0-9a-f]{64}$' OR rev !~ '^(0|[1-9][0-9]{0,19})$' OR ver !~ '^(0|[1-9][0-9]{0,19})$' OR rev::numeric>18446744073709551615 OR ver::numeric>18446744073709551615 THEN RAISE EXCEPTION USING ERRCODE='PH001',MESSAGE='invalid pin binding'; END IF;
 IF NOT (a ? 'ttl_ns') OR (ttl IS NOT NULL AND (jsonb_typeof(a->'ttl_ns') IS DISTINCT FROM 'string' OR ttl !~ '^[1-9][0-9]{0,18}$')) THEN RAISE EXCEPTION USING ERRCODE='PH001',MESSAGE='invalid pin ttl'; END IF;
 IF ttl IS NOT NULL AND (ttl::numeric NOT BETWEEN 1000000000 AND 9223372036000000000 OR mod(ttl::numeric,1000000000)<>0) THEN RAISE EXCEPTION USING ERRCODE='PH001',MESSAGE='invalid pin ttl'; END IF;
 IF (operation='create' AND (rev='0' OR ver<>'0' OR a->>'chain' !~ '^[0-9a-f]{64}$')) OR (operation IN ('renew','release') AND (rev<>'0' OR ver='0' OR a->>'chain'<>'')) OR (operation='release' AND ttl IS NOT NULL) THEN RAISE EXCEPTION USING ERRCODE='PH001',MESSAGE='invalid pin operation'; END IF;
 wire:='{"Format":"history-pin-v1","Operation":'||pin_quote(operation)||',"Identity":{"namespace":'||pin_quote(a->>'namespace')||',"run_id":'||pin_quote(a->>'run_id')||'},"PinID":'||pin_quote(a->>'pin_id')||',"MutationID":'||pin_quote(a->>'mutation_id')||',"Revision":'||rev||',"Chain":'||pin_quote(a->>'chain')||',"HolderDigest":'||pin_quote(a->>'holder_digest')||',"ExpectedVersion":'||ver||',"TTL":'||coalesce(ttl,'null')||'}';
 IF encode(sha256(convert_to(wire,'UTF8')),'hex') IS DISTINCT FROM a->>'mutation_digest' THEN RAISE EXCEPTION USING ERRCODE='PH001',MESSAGE='invalid pin digest'; END IF;
END $$;
CREATE FUNCTION pulse_durable_distributed.pin_state(p pulse_durable_distributed.history_pins, t timestamptz) RETURNS jsonb LANGUAGE sql STABLE SET search_path=pg_catalog,pulse_durable_distributed,pg_temp AS $$
 SELECT jsonb_build_object('status',CASE WHEN p.released THEN 'released' WHEN p.expires_at<=t THEN 'expired' ELSE 'active' END,'state',jsonb_build_object('pin_id',p.pin_id,'identity',jsonb_build_object('namespace',p.namespace,'run_id',p.run_id),'revision',p.revision::text,'chain',p.chain,'version',p.version::text,'long_lived',p.ttl_seconds IS NULL,'expires_at',p.expires_at,'holder_digest',p.holder_digest,'mutation',jsonb_build_object('id',p.mutation_id,'digest',p.mutation_digest)))
$$;
-- 精确回执先于 CAS；已知 guard 缺回执绝不能降级为 absent。
CREATE FUNCTION pulse_durable_distributed.pin_receipt(a jsonb, operation text, actor_check boolean) RETURNS jsonb LANGUAGE plpgsql SET search_path=pg_catalog,pulse_durable_distributed,pg_temp AS $$
DECLARE g pulse_durable_distributed.pin_guards; answer jsonb;
BEGIN
 SELECT * INTO g FROM pulse_durable_distributed.pin_guards WHERE namespace=a->>'namespace' AND run_id=a->>'run_id' AND mutation_id=a->>'mutation_id';
 IF NOT FOUND THEN RETURN NULL; END IF;
 IF g.pin_id IS DISTINCT FROM a->>'pin_id' OR g.operation IS DISTINCT FROM operation OR g.digest IS DISTINCT FROM a->>'mutation_digest' THEN RAISE EXCEPTION USING ERRCODE='PH002',MESSAGE='pin mutation conflict'; END IF;
 IF actor_check AND g.requires_maintain AND NOT pin_permission(g.namespace,'maintain') THEN RAISE EXCEPTION USING ERRCODE='42501',MESSAGE='pin receipt maintain denied'; END IF;
 IF actor_check AND g.actor<>session_user THEN RAISE EXCEPTION USING ERRCODE='42501',MESSAGE='pin actor denied'; END IF;
 SELECT result INTO answer FROM pulse_durable_distributed.pin_receipts WHERE namespace=g.namespace AND run_id=g.run_id AND mutation_id=g.mutation_id;
 IF answer IS NULL OR answer->>'request_digest' IS DISTINCT FROM g.digest OR answer->>'operation' IS DISTINCT FROM g.operation OR answer->>'pin_id' IS DISTINCT FROM g.pin_id THEN RAISE EXCEPTION USING ERRCODE='PH005',MESSAGE='pin receipt missing or corrupt'; END IF;
 RETURN answer;
END $$;
CREATE FUNCTION pulse_durable_distributed.pin_publish(p pulse_durable_distributed.history_pins, a jsonb, operation text, t timestamptz, requires_maintain boolean) RETURNS jsonb LANGUAGE plpgsql SET search_path=pg_catalog,pulse_durable_distributed,pg_temp AS $$
DECLARE answer jsonb; r pulse_durable_distributed.runs;
BEGIN
 SELECT * INTO STRICT r FROM pulse_durable_distributed.runs WHERE namespace=p.namespace AND run_id=p.run_id;
 t:=storage_clock(r,t);
 IF NOT p.released AND p.expires_at<=t THEN RAISE EXCEPTION USING ERRCODE='PH004',MESSAGE='pin expired before publish'; END IF;
 answer:=jsonb_build_object('identity',jsonb_build_object('namespace',p.namespace,'run_id',p.run_id),'pin_id',p.pin_id,'mutation',jsonb_build_object('id',a->>'mutation_id','digest',a->>'mutation_digest'),'operation',operation,'version',p.version::text,'occurred_at',t,'request_digest',a->>'mutation_digest','result',pin_state(p,t));
 INSERT INTO pulse_durable_distributed.pin_guards VALUES(p.namespace,p.run_id,a->>'mutation_id',p.pin_id,operation,a->>'mutation_digest',session_user,requires_maintain);
 INSERT INTO pulse_durable_distributed.pin_receipts VALUES(p.namespace,p.run_id,a->>'mutation_id',answer);
 UPDATE pulse_durable_distributed.runs SET maintenance_watermark=t WHERE namespace=p.namespace AND run_id=p.run_id;
 RETURN answer->'result';
END $$;
CREATE FUNCTION pulse_durable_distributed.pin_deadline(t timestamptz, seconds bigint) RETURNS timestamptz LANGUAGE plpgsql SET search_path=pg_catalog,pulse_durable_distributed,pg_temp AS $$
DECLARE expiry timestamptz;
BEGIN
 IF seconds IS NULL THEN RETURN NULL; END IF;
 BEGIN
  expiry:=t+seconds*interval '1 second';
 EXCEPTION WHEN datetime_field_overflow OR interval_field_overflow OR numeric_value_out_of_range THEN
  RAISE EXCEPTION USING ERRCODE='PH001',MESSAGE='pin deadline overflow';
 END;
 IF NOT isfinite(expiry) OR expiry<=t THEN RAISE EXCEPTION USING ERRCODE='PH001',MESSAGE='invalid pin deadline'; END IF;
 RETURN expiry;
END $$;
CREATE FUNCTION pulse_durable_distributed.pin_create(a jsonb) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pulse_durable_distributed,pg_temp AS $$
DECLARE r pulse_durable_distributed.runs; p pulse_durable_distributed.history_pins; t timestamptz; answer jsonb; anchor text; seconds bigint;
BEGIN
 r:=pin_lock(a);
 IF NOT pin_permission(a->>'namespace',CASE WHEN a->>'ttl_ns' IS NULL THEN 'maintain' ELSE 'replay' END) THEN RAISE EXCEPTION USING ERRCODE='42501',MESSAGE='pin create denied'; END IF;
 PERFORM pin_validate(a,'create');
 IF r.run_id IS NULL THEN RAISE EXCEPTION USING ERRCODE='PH003',MESSAGE='pin target missing'; END IF;
 t:=storage_clock(r,NULL);
 answer:=pin_receipt(a,'create',true); IF answer IS NOT NULL THEN RETURN answer->'result'; END IF;
 IF EXISTS(SELECT 1 FROM pulse_durable_distributed.history_pins WHERE namespace=r.namespace AND run_id=r.run_id AND pin_id=a->>'pin_id') THEN RAISE EXCEPTION USING ERRCODE='PH002',MESSAGE='pin id reused'; END IF;
 IF r.revision=0 OR (a->>'revision')::numeric>r.revision THEN RAISE EXCEPTION USING ERRCODE='PH003',MESSAGE='pin revision missing'; END IF;
 IF a->>'revision'='1' THEN anchor:=encode(sha256(convert_to('pulse-durable/chain/seed/v1','UTF8')||decode('00','hex')||decode(r.snapshot_digest,'hex')||int8send(1::bigint)),'hex');
 ELSE SELECT next_chain INTO anchor FROM pulse_durable_distributed.batches WHERE namespace=r.namespace AND run_id=r.run_id AND revision=(a->>'revision')::numeric; END IF;
 IF anchor IS NULL THEN RAISE EXCEPTION USING ERRCODE='PH003',MESSAGE='pin revision missing'; END IF;
 IF anchor IS DISTINCT FROM a->>'chain' THEN RAISE EXCEPTION USING ERRCODE='PH002',MESSAGE='pin chain conflict'; END IF;
 seconds:=((a->>'ttl_ns')::numeric/1000000000)::bigint;
 INSERT INTO pulse_durable_distributed.history_pins VALUES(r.namespace,r.run_id,a->>'pin_id',(a->>'revision')::numeric,anchor,1,session_user,a->>'holder_digest',a->>'mutation_id',a->>'mutation_digest',seconds,pin_deadline(t,seconds),false) RETURNING * INTO p;
 RETURN pin_publish(p,a,'create',t,seconds IS NULL);
END $$;
CREATE FUNCTION pulse_durable_distributed.pin_renew(a jsonb) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pulse_durable_distributed,pg_temp AS $$
DECLARE r pulse_durable_distributed.runs; p pulse_durable_distributed.history_pins; t timestamptz; answer jsonb; seconds bigint; expiry timestamptz; maint boolean; was_long boolean;
BEGIN
 r:=pin_lock(a); maint:=pin_permission(a->>'namespace','maintain');
 IF NOT (maint OR pin_permission(a->>'namespace','replay')) THEN RAISE EXCEPTION USING ERRCODE='42501',MESSAGE='pin renew denied'; END IF;
 PERFORM pin_validate(a,'renew');
 SELECT * INTO p FROM pulse_durable_distributed.history_pins WHERE namespace=a->>'namespace' AND run_id=a->>'run_id' AND pin_id=a->>'pin_id';
 IF p.pin_id IS NOT NULL AND (p.holder<>session_user OR p.holder_digest IS DISTINCT FROM a->>'holder_digest' OR (p.ttl_seconds IS NULL AND NOT maint)) THEN RAISE EXCEPTION USING ERRCODE='42501',MESSAGE='pin holder denied'; END IF;
 IF r.run_id IS NOT NULL THEN t:=storage_clock(r,NULL); END IF;
 answer:=pin_receipt(a,'renew',true); IF answer IS NOT NULL THEN RETURN answer->'result'; END IF;
 IF p.pin_id IS NULL THEN RAISE EXCEPTION USING ERRCODE='PH003',MESSAGE='pin missing'; END IF;
 IF p.released THEN RAISE EXCEPTION USING ERRCODE='PH002',MESSAGE='pin released'; END IF;
 IF p.expires_at<=t THEN RAISE EXCEPTION USING ERRCODE='PH004',MESSAGE='pin expired'; END IF;
 IF p.version<>(a->>'expected_version')::numeric OR p.version=18446744073709551615 THEN RAISE EXCEPTION USING ERRCODE='PH002',MESSAGE='pin version conflict'; END IF;
 was_long:=p.ttl_seconds IS NULL;
 seconds:=coalesce(((a->>'ttl_ns')::numeric/1000000000)::bigint,p.ttl_seconds);
 expiry:=pin_deadline(t,seconds);
 UPDATE pulse_durable_distributed.history_pins SET version=version+1,ttl_seconds=seconds,expires_at=CASE WHEN seconds IS NULL THEN NULL ELSE greatest(expires_at,expiry) END WHERE namespace=p.namespace AND run_id=p.run_id AND pin_id=p.pin_id RETURNING * INTO p;
 RETURN pin_publish(p,a,'renew',t,was_long);
END $$;
CREATE FUNCTION pulse_durable_distributed.pin_release(a jsonb) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pulse_durable_distributed,pg_temp AS $$
DECLARE r pulse_durable_distributed.runs; p pulse_durable_distributed.history_pins; t timestamptz; answer jsonb; maint boolean;
BEGIN
 r:=pin_lock(a); maint:=pin_permission(a->>'namespace','maintain');
 IF NOT (maint OR pin_permission(a->>'namespace','replay')) THEN RAISE EXCEPTION USING ERRCODE='42501',MESSAGE='pin release denied'; END IF;
 PERFORM pin_validate(a,'release');
 SELECT * INTO p FROM pulse_durable_distributed.history_pins WHERE namespace=a->>'namespace' AND run_id=a->>'run_id' AND pin_id=a->>'pin_id';
 IF p.pin_id IS NOT NULL AND NOT maint AND (p.holder<>session_user OR p.holder_digest IS DISTINCT FROM a->>'holder_digest' OR p.ttl_seconds IS NULL) THEN RAISE EXCEPTION USING ERRCODE='42501',MESSAGE='pin holder denied'; END IF;
 IF r.run_id IS NOT NULL THEN t:=storage_clock(r,NULL); END IF;
 answer:=pin_receipt(a,'release',true); IF answer IS NOT NULL THEN RETURN answer->'result'; END IF;
 IF p.pin_id IS NULL THEN RAISE EXCEPTION USING ERRCODE='PH003',MESSAGE='pin missing'; END IF;
 IF p.released THEN RAISE EXCEPTION USING ERRCODE='PH002',MESSAGE='pin released'; END IF;
 IF p.expires_at<=t THEN RAISE EXCEPTION USING ERRCODE='PH004',MESSAGE='pin expired'; END IF;
 IF p.version<>(a->>'expected_version')::numeric OR p.version=18446744073709551615 THEN RAISE EXCEPTION USING ERRCODE='PH002',MESSAGE='pin version conflict'; END IF;
 UPDATE pulse_durable_distributed.history_pins SET version=version+1,released=true WHERE namespace=p.namespace AND run_id=p.run_id AND pin_id=p.pin_id RETURNING * INTO p;
 RETURN pin_publish(p,a,'release',t,p.ttl_seconds IS NULL OR p.holder<>session_user OR p.holder_digest IS DISTINCT FROM a->>'holder_digest');
END $$;
CREATE FUNCTION pulse_durable_distributed.pin_find(a jsonb) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pulse_durable_distributed,pg_temp AS $$
DECLARE r pulse_durable_distributed.runs; p pulse_durable_distributed.history_pins; t timestamptz;
BEGIN
 r:=pin_lock(a);
 IF NOT pin_permission(a->>'namespace','query') THEN RAISE EXCEPTION USING ERRCODE='42501',MESSAGE='pin query denied'; END IF;
 SELECT * INTO p FROM pulse_durable_distributed.history_pins WHERE namespace=a->>'namespace' AND run_id=a->>'run_id' AND pin_id=a->>'pin_id';
 IF r.run_id IS NULL THEN RETURN jsonb_build_object('status','absent','state',NULL); END IF;
 t:=storage_clock(r,NULL);
 UPDATE pulse_durable_distributed.runs SET maintenance_watermark=t WHERE namespace=r.namespace AND run_id=r.run_id;
 IF p.pin_id IS NULL THEN RETURN jsonb_build_object('status','absent','state',NULL); END IF;
 RETURN pin_state(p,t);
END $$;
CREATE FUNCTION pulse_durable_distributed.pin_find_mutation(a jsonb) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pulse_durable_distributed,pg_temp AS $$
DECLARE r pulse_durable_distributed.runs; answer jsonb; t timestamptz;
BEGIN
 r:=pin_lock(a);
 IF NOT pin_permission(a->>'namespace','query') THEN RAISE EXCEPTION USING ERRCODE='42501',MESSAGE='pin query denied'; END IF;
 IF jsonb_typeof(a->'mutation_id') IS DISTINCT FROM 'string' OR coalesce(octet_length(a->>'mutation_id'),0) NOT BETWEEN 1 AND 256 OR coalesce(a->>'mutation_digest','') !~ '^[0-9a-f]{64}$' OR coalesce(a->>'operation','') NOT IN ('create','renew','release') THEN RAISE EXCEPTION USING ERRCODE='PH001',MESSAGE='invalid pin receipt query'; END IF;
 IF r.run_id IS NOT NULL THEN
  t:=storage_clock(r,NULL);
  UPDATE pulse_durable_distributed.runs SET maintenance_watermark=t WHERE namespace=r.namespace AND run_id=r.run_id;
 END IF;
 answer:=pin_receipt(a,a->>'operation',false);
 RETURN jsonb_build_object('status',CASE WHEN answer IS NULL THEN 'absent' ELSE 'found' END,'receipt',answer);
END $$;
REVOKE ALL ON ALL TABLES IN SCHEMA pulse_durable_distributed FROM PUBLIC;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA pulse_durable_distributed FROM PUBLIC;
