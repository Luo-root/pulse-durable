CREATE TABLE pulse_durable_distributed.retention_policies(
 namespace text PRIMARY KEY,
 policy_version numeric(20,0) NOT NULL CHECK(policy_version>=1 AND policy_version<=18446744073709551615),
 policy jsonb NOT NULL,
 policy_digest text NOT NULL CHECK(policy_digest ~ '^[0-9a-f]{64}$'),
 enabled boolean NOT NULL,
 terminal_payload_ttl bigint,
 replay_history_ttl bigint,
 event_ttl bigint,
 receipt_ttl bigint,
 updated_at timestamptz NOT NULL
);
CREATE TABLE pulse_durable_distributed.retention_previews(
 dry_run_id text PRIMARY KEY,
 namespace text NOT NULL,
 policy_version numeric(20,0) NOT NULL,
 policy jsonb NOT NULL,
 policy_digest text NOT NULL CHECK(policy_digest ~ '^[0-9a-f]{64}$'),
 observed_from timestamptz NOT NULL,
 observed_through timestamptz NOT NULL,
 cursor jsonb NOT NULL,
 "limit" integer NOT NULL CHECK("limit" BETWEEN 1 AND 1000000),
 receipt_digest text NOT NULL CHECK(receipt_digest ~ '^[0-9a-f]{64}$'),
 created_at timestamptz NOT NULL,
 FOREIGN KEY(namespace) REFERENCES pulse_durable_distributed.retention_policies(namespace)
);
REVOKE ALL ON ALL TABLES IN SCHEMA pulse_durable_distributed FROM PUBLIC;
CREATE FUNCTION pulse_durable_distributed.retention_policy_validate(p jsonb) RETURNS boolean LANGUAGE plpgsql IMMUTABLE SET search_path=pg_catalog,pulse_durable_distributed,pg_temp AS $$
DECLARE k text; v jsonb; n numeric;
BEGIN
 IF jsonb_typeof(p) IS DISTINCT FROM 'object' OR (SELECT count(*) FROM jsonb_object_keys(p) AS keys(key))<>5 THEN RETURN false; END IF;
 IF EXISTS(SELECT 1 FROM jsonb_object_keys(p) AS keys(key) WHERE key NOT IN ('Enabled','TerminalPayloadTTL','ReplayHistoryTTL','EventTTL','ReceiptTTL')) THEN RETURN false; END IF;
 IF jsonb_typeof(p->'Enabled') IS DISTINCT FROM 'boolean' THEN RETURN false; END IF;
 FOREACH k IN ARRAY ARRAY['TerminalPayloadTTL','ReplayHistoryTTL','EventTTL','ReceiptTTL'] LOOP
  v:=p->k; IF jsonb_typeof(v)='null' THEN CONTINUE; END IF;
  IF jsonb_typeof(v) IS DISTINCT FROM 'number' OR v::text !~ '^-?[0-9]+$' THEN RETURN false; END IF;
  BEGIN n:=(v::text)::numeric; EXCEPTION WHEN others THEN RETURN false; END;
  IF n<3600000000000 OR n>9223372036854775807 THEN RETURN false; END IF;
  IF mod(n,1000000000)<>0 THEN RETURN false; END IF;
 END LOOP;
 RETURN true;
END $$;
CREATE FUNCTION pulse_durable_distributed.retention_policy_auth(a jsonb) RETURNS text LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pulse_durable_distributed,pg_temp AS $$
DECLARE ns text:=a->>'namespace';
BEGIN
 IF NOT check_schema() OR coalesce(octet_length(ns),0) NOT BETWEEN 1 AND 256 OR NOT EXISTS(SELECT 1 FROM access WHERE namespace=ns AND permission='retain' AND pg_has_role(session_user,role_name,'MEMBER')) THEN RAISE EXCEPTION USING ERRCODE='42501',MESSAGE='retention access denied'; END IF;
 RETURN ns;
END $$;
CREATE FUNCTION pulse_durable_distributed.retention_policy_get(a jsonb) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pulse_durable_distributed,pg_temp AS $$
DECLARE ns text:=retention_policy_auth(a); p retention_policies; t timestamptz:=clock_timestamp();
BEGIN
 INSERT INTO retention_policies(namespace,policy_version,policy,policy_digest,enabled,updated_at) VALUES(ns,1,jsonb_build_object('Enabled',false,'TerminalPayloadTTL',NULL,'ReplayHistoryTTL',NULL,'EventTTL',NULL,'ReceiptTTL',NULL),encode(sha256(convert_to('{"Enabled":false,"EventTTL":null,"ReceiptTTL":null,"ReplayHistoryTTL":null,"TerminalPayloadTTL":null}','UTF8')),'hex'),false,t) ON CONFLICT DO NOTHING;
 SELECT * INTO STRICT p FROM retention_policies WHERE namespace=ns;
 RETURN jsonb_build_object('namespace',p.namespace,'policy_version',p.policy_version::text,'policy',p.policy,'policy_digest',p.policy_digest,'updated_at',p.updated_at);
END $$;
CREATE FUNCTION pulse_durable_distributed.retention_policy_preview(a jsonb) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pulse_durable_distributed,pg_temp AS $$
DECLARE ns text:=retention_policy_auth(a); p retention_policies; q retention_previews; id text:=a->>'dry_run_id'; proposed jsonb:=a->'policy'; dg text:=a->>'policy_digest'; vf timestamptz:=(a->>'observed_from')::timestamptz; vt timestamptz:=(a->>'observed_through')::timestamptz; cur jsonb:=a->'cursor'; lim integer:=(a->>'limit')::integer; rd text; t timestamptz:=clock_timestamp();
BEGIN
 IF coalesce(octet_length(id),0) NOT BETWEEN 1 AND 256 OR NOT retention_policy_validate(proposed) OR coalesce(dg,'') !~ '^[0-9a-f]{64}$' OR vf IS NULL OR vt IS NULL OR vt<vf OR jsonb_typeof(cur) IS DISTINCT FROM 'object' OR (SELECT count(*) FROM jsonb_object_keys(cur) AS keys(key))<>2 OR jsonb_typeof(cur->'set') IS DISTINCT FROM 'boolean' OR jsonb_typeof(cur->'value') IS DISTINCT FROM 'string' OR ((cur->>'set')::boolean=false AND cur->>'value'<>'') OR ((cur->>'set')::boolean=true AND coalesce(octet_length(cur->>'value'),0) NOT BETWEEN 1 AND 256) OR lim IS NULL OR lim NOT BETWEEN 1 AND 1000000 THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='invalid retention preview'; END IF;
 INSERT INTO retention_policies(namespace,policy_version,policy,policy_digest,enabled,updated_at) VALUES(ns,1,jsonb_build_object('Enabled',false,'TerminalPayloadTTL',NULL,'ReplayHistoryTTL',NULL,'EventTTL',NULL,'ReceiptTTL',NULL),encode(sha256(convert_to('{"Enabled":false,"EventTTL":null,"ReceiptTTL":null,"ReplayHistoryTTL":null,"TerminalPayloadTTL":null}','UTF8')),'hex'),false,t) ON CONFLICT DO NOTHING;
 SELECT * INTO STRICT p FROM retention_policies WHERE namespace=ns;
 IF dg IS DISTINCT FROM encode(sha256(convert_to(replace(replace(proposed::text, ': ', ':'), ', ', ','),'UTF8')),'hex') THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='retention policy digest'; END IF;
 IF (a->>'policy_version')::numeric IS DISTINCT FROM p.policy_version THEN RAISE EXCEPTION USING ERRCODE='PD005',MESSAGE='retention policy version'; END IF;
 rd:=encode(sha256(convert_to(id||':'||ns||':'||p.policy_version::text||':'||dg||':'||vf::text||':'||vt::text||':'||cur::text||':'||lim::text,'UTF8')),'hex');
 INSERT INTO retention_previews(dry_run_id,namespace,policy_version,policy,policy_digest,observed_from,observed_through,cursor,"limit",receipt_digest,created_at) VALUES(id,ns,p.policy_version,proposed,dg,vf,vt,cur,lim,rd,t) ON CONFLICT(dry_run_id) DO NOTHING;
 SELECT * INTO q FROM retention_previews WHERE dry_run_id=id FOR SHARE;
 IF q.namespace<>ns OR q.policy_version<>p.policy_version OR q.policy<>proposed OR q.policy_digest<>dg OR q.observed_from<>vf OR q.observed_through<>vt OR q.cursor<>cur OR q."limit"<>lim THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='retention preview binding conflict'; END IF;
 RETURN jsonb_build_object('dry_run_id',id,'namespace',ns,'policy_version',p.policy_version::text,'policy',proposed,'policy_digest',dg,'observed_from',vf,'observed_through',vt,'summary',jsonb_build_object('candidates','[]'::jsonb,'protected','{}'::jsonb,'logical_rows','0','logical_bytes','0'),'cursor',cur,'next_cursor',jsonb_build_object('set',false,'value',''),'receipt_digest',rd);
END $$;
CREATE FUNCTION pulse_durable_distributed.retention_candidate_preview(a jsonb) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pulse_durable_distributed,pg_temp AS $$
DECLARE ns text:=retention_policy_auth(a); p retention_policies; id text:=a->>'dry_run_id'; proposed jsonb:=a->'policy'; dg text:=a->>'policy_digest'; vf timestamptz:=(a->>'observed_from')::timestamptz; vt timestamptz:=(a->>'observed_through')::timestamptz; cur jsonb:=a->'cursor'; lim integer:=(a->>'limit')::integer; after_id text:=''; rd text; candidates jsonb:='[]'::jsonb; protected jsonb:='{}'::jsonb; rows bigint:=0; bytes bigint:=0; batch_rows bigint; batch_bytes bigint; r record; reasons jsonb; key text; more boolean:=false;
BEGIN
 IF coalesce(octet_length(id),0) NOT BETWEEN 1 AND 256 OR NOT retention_policy_validate(proposed) OR coalesce(dg,'') !~ '^[0-9a-f]{64}$' OR vf IS NULL OR vt IS NULL OR vt<vf OR jsonb_typeof(cur) IS DISTINCT FROM 'object' OR (SELECT count(*) FROM jsonb_object_keys(cur) AS keys(key))<>2 OR jsonb_typeof(cur->'set') IS DISTINCT FROM 'boolean' OR jsonb_typeof(cur->'value') IS DISTINCT FROM 'string' OR ((cur->>'set')::boolean=false AND cur->>'value'<>'') OR ((cur->>'set')::boolean=true AND coalesce(octet_length(cur->>'value'),0) NOT BETWEEN 1 AND 256) OR lim IS NULL OR lim NOT BETWEEN 1 AND 1000000 THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='invalid retention candidate preview'; END IF;
 IF cur->>'set'='true' THEN after_id:=cur->>'value'; END IF;
 SELECT * INTO STRICT p FROM retention_policies WHERE namespace=ns FOR SHARE;
 IF dg IS DISTINCT FROM encode(sha256(convert_to(replace(replace(proposed::text, ': ', ':'), ', ', ','),'UTF8')),'hex') THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='retention policy digest'; END IF;
 IF (a->>'policy_version')::numeric IS DISTINCT FROM p.policy_version THEN RAISE EXCEPTION USING ERRCODE='PD005',MESSAGE='retention policy version'; END IF;
 rd:=encode(sha256(convert_to(id||':'||ns||':'||p.policy_version::text||':'||dg||':'||vf::text||':'||vt::text||':'||cur::text||':'||lim::text,'UTF8')),'hex');
 FOR r IN SELECT x.* FROM runs x WHERE x.namespace=ns AND x.run_id COLLATE "C">after_id COLLATE "C" AND x.status IN ('succeeded','failed','canceled') AND x.revision>0 AND x.finished_at>=vf AND x.finished_at<=vt ORDER BY x.run_id COLLATE "C" LIMIT lim+1 LOOP
  IF jsonb_array_length(candidates)>=lim THEN more:=true; EXIT; END IF;
  key:=ns||chr(31)||'run'||chr(31)||r.run_id||chr(31)||r.revision::text;
  reasons:=jsonb_build_array('history_retained','tombstone');
  IF EXISTS(SELECT 1 FROM build_pins b WHERE b.namespace=ns AND b.run_id=r.run_id AND NOT b.closed) THEN reasons:=reasons||jsonb_build_array('build_pin'); END IF;
  IF EXISTS(SELECT 1 FROM maintenance_guards g WHERE g.namespace=ns AND g.run_id=r.run_id) OR EXISTS(SELECT 1 FROM mutation_guards g WHERE g.namespace=ns AND g.run_id=r.run_id) THEN reasons:=reasons||jsonb_build_array('guard'); END IF;
  SELECT count(*),coalesce(sum(octet_length(b.wire)),0) INTO batch_rows,batch_bytes FROM batches b WHERE b.namespace=ns AND b.run_id=r.run_id;
  candidates:=candidates||jsonb_build_array(jsonb_build_object('namespace',ns,'kind','run','id',r.run_id,'revision',r.revision,'rows',batch_rows,'bytes',batch_bytes,'protected',reasons));
  protected:=protected||jsonb_build_object(key,reasons);
  rows:=rows+batch_rows; bytes:=bytes+batch_bytes;
 END LOOP;
 RETURN jsonb_build_object('dry_run_id',id,'namespace',ns,'policy_version',p.policy_version::text,'policy',proposed,'policy_digest',dg,'observed_from',vf,'observed_through',vt,'summary',jsonb_build_object('candidates',candidates,'protected',protected,'logical_rows',rows,'logical_bytes',bytes),'cursor',cur,'next_cursor',CASE WHEN more THEN jsonb_build_object('set',true,'value',(candidates->(jsonb_array_length(candidates)-1))->>'id') ELSE jsonb_build_object('set',false,'value','') END,'receipt_digest',rd);
END $$;
REVOKE ALL ON FUNCTION pulse_durable_distributed.retention_candidate_preview(jsonb) FROM PUBLIC;
CREATE FUNCTION pulse_durable_distributed.retention_policy_update(a jsonb) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pulse_durable_distributed,pg_temp AS $$
DECLARE ns text:=retention_policy_auth(a); p retention_policies; q retention_previews; proposed jsonb:=a->'policy'; id text:=a->>'dry_run_id'; t timestamptz:=clock_timestamp();
BEGIN
 SELECT * INTO STRICT p FROM retention_policies WHERE namespace=ns FOR UPDATE;
 IF (a->>'expected_version')::numeric IS DISTINCT FROM p.policy_version THEN RAISE EXCEPTION USING ERRCODE='PD005',MESSAGE='retention policy version'; END IF;
 IF NOT retention_policy_validate(proposed) OR (a->>'policy_digest') IS DISTINCT FROM encode(sha256(convert_to(replace(replace(proposed::text, ': ', ':'), ', ', ','),'UTF8')),'hex') THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='retention policy digest'; END IF;
 IF id IS NOT NULL THEN SELECT * INTO q FROM retention_previews WHERE dry_run_id=id FOR SHARE; IF NOT FOUND OR q.namespace<>ns OR q.policy_version<>p.policy_version OR q.policy_digest<>a->>'policy_digest' OR q.policy<>proposed THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='retention preview binding'; END IF; END IF;
 IF (p.policy->>'Enabled')::boolean=false AND (proposed->>'Enabled')::boolean=true OR EXISTS(SELECT 1 FROM jsonb_each_text(proposed) x WHERE x.key<>'Enabled' AND x.value<>'null' AND ((p.policy->>x.key)='null' OR x.value::bigint < (p.policy->>x.key)::bigint)) THEN IF id IS NULL THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='retention preview required'; END IF; END IF;
 UPDATE retention_policies SET policy_version=policy_version+1,policy=proposed,policy_digest=a->>'policy_digest',enabled=(proposed->>'Enabled')::boolean,terminal_payload_ttl=NULLIF(proposed->>'TerminalPayloadTTL','null')::bigint,replay_history_ttl=NULLIF(proposed->>'ReplayHistoryTTL','null')::bigint,event_ttl=NULLIF(proposed->>'EventTTL','null')::bigint,receipt_ttl=NULLIF(proposed->>'ReceiptTTL','null')::bigint,updated_at=t WHERE namespace=ns RETURNING * INTO p;
 RETURN jsonb_build_object('namespace',ns,'policy_version',p.policy_version::text,'policy',p.policy,'policy_digest',p.policy_digest,'updated_at',p.updated_at);
END $$;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA pulse_durable_distributed FROM PUBLIC;
