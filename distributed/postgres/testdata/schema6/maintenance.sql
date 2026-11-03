-- One invocation is one transaction. No operation here locks a lease or deletes data.
CREATE FUNCTION pulse_durable_distributed.storage_state(r pulse_durable_distributed.runs) RETURNS jsonb LANGUAGE sql VOLATILE SET search_path=pg_catalog,pulse_durable_distributed,pg_temp AS $$
 SELECT jsonb_build_object('generation',r.representation_generation::text,'base_revision',coalesce(s.revision,1)::text,'base_chain',coalesce(s.chain,encode(sha256(convert_to('pulse-durable/chain/seed/v1','UTF8')||decode('00','hex')||decode(r.snapshot_digest,'hex')||int8send(1::bigint)),'hex')),'head_revision',r.revision::text,'head_chain',r.head_chain)
 FROM (SELECT 1) x LEFT JOIN pulse_durable_distributed.storage_snapshots s ON s.namespace=r.namespace AND s.run_id=r.run_id AND s.generation=r.active_snapshot
$$;
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
 IF anchor IS NULL OR through_rev<base_rev OR through_rev>r.revision THEN RAISE EXCEPTION USING ERRCODE='PD005',MESSAGE='invalid path'; END IF;
 RETURN snapshot(r)||jsonb_build_object('revision',through_rev::text,'record',encode(bytes,'base64'),'snapshot_digest',dg,'base_revision',base_rev::text,'base_chain',base_chain,'head_chain',anchor,'batches',coalesce((SELECT jsonb_agg(jsonb_build_object('revision',revision::text,'wire',encode(wire,'base64'),'digest',digest,'parent_chain',parent_chain,'next_chain',next_chain) ORDER BY revision) FROM pulse_durable_distributed.batches WHERE namespace=r.namespace AND run_id=r.run_id AND revision>base_rev AND revision<=through_rev),'[]'::jsonb));
END $$;
CREATE FUNCTION pulse_durable_distributed.maintenance(op text,a jsonb) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pulse_durable_distributed,pg_temp AS $$
DECLARE ns text:=a->>'namespace'; rid text:=a->>'run_id'; mid text:=a->>'mutation_id'; md text:=a->>'mutation_digest'; r pulse_durable_distributed.runs; p pulse_durable_distributed.build_pins; g pulse_durable_distributed.maintenance_guards; t timestamptz; first_sample timestamptz; rev numeric; gen bigint; anchor text; b bytea; j jsonb; answer jsonb; suffix_bytes bigint; suffix_count bigint;
BEGIN
 IF NOT check_schema() THEN RAISE EXCEPTION USING ERRCODE='42501',MESSAGE='unsafe schema or credentials'; END IF;
 IF op NOT IN ('inspect','begin','publish','abort','find') OR op IS NULL THEN RAISE EXCEPTION USING ERRCODE='42501',MESSAGE='unknown maintenance operation'; END IF;
 IF coalesce(octet_length(ns),0) NOT BETWEEN 1 AND 256 OR coalesce(octet_length(rid),0) NOT BETWEEN 1 AND 256 OR NOT EXISTS(SELECT 1 FROM pulse_durable_distributed.access x WHERE x.namespace=ns AND x.permission='maintain' AND pg_has_role(session_user,x.role_name,'MEMBER')) THEN RAISE EXCEPTION USING ERRCODE='42501',MESSAGE='maintenance access denied'; END IF;
 -- Reject owner credentials in the request rather than treating them as authority.
 IF a ?| ARRAY['epoch','worker_id','boot_id','lease_id'] THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='worker token forbidden'; END IF;
 -- Identity lock before run lock; never acquire a lease lock afterwards.
 PERFORM pg_advisory_xact_lock(hashtextextended(jsonb_build_array(ns,rid)::text,5788618723189269578));
 SELECT * INTO r FROM pulse_durable_distributed.runs WHERE namespace=ns AND run_id=rid FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='PD003',MESSAGE='not found'; END IF;
 t:=clock_timestamp(); first_sample:=t;
 IF t<r.updated_at OR t<r.maintenance_watermark OR t<r.receipt_observed_through THEN RAISE EXCEPTION USING ERRCODE='PD007',MESSAGE='clock regression'; END IF;
 IF op='inspect' THEN
  IF r.revision=0 THEN RAISE EXCEPTION USING ERRCODE='PD003',MESSAGE='no checkpoint'; END IF;
  RETURN storage_state(r);
 END IF;
 IF coalesce(octet_length(mid),0) NOT BETWEEN 1 AND 256 OR coalesce(md,'') !~ '^[0-9a-f]{64}$' THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='invalid maintenance mutation'; END IF;
 SELECT * INTO g FROM pulse_durable_distributed.maintenance_guards WHERE namespace=ns AND run_id=rid AND mutation_id=mid;
 IF FOUND AND g.digest IS DISTINCT FROM md THEN RAISE EXCEPTION USING ERRCODE='PD005',MESSAGE='maintenance mutation conflict'; END IF;
 SELECT result INTO answer FROM pulse_durable_distributed.maintenance_receipts WHERE namespace=ns AND run_id=rid AND mutation_id=mid;
 IF FOUND THEN RETURN jsonb_build_object('status','published','result',answer); END IF;
 SELECT * INTO p FROM pulse_durable_distributed.build_pins WHERE namespace=ns AND run_id=rid AND mutation_id=mid;
 IF op='find' THEN
  RETURN jsonb_build_object('status',CASE WHEN g.mutation_id IS NULL THEN 'absent' WHEN p.closed THEN 'closed' WHEN p.expires_at<=t THEN 'expired' ELSE 'building' END,'result',NULL);
 END IF;
 IF op='begin' THEN
  -- A guarded mutation is never restarted, even after cancellation or expiry.
  IF g.mutation_id IS NOT NULL THEN RAISE EXCEPTION USING ERRCODE='PD005',MESSAGE='maintenance mutation already reserved'; END IF;
  rev:=(a->>'through_revision')::numeric; gen:=(a->>'generation')::bigint;
  IF md IS DISTINCT FROM encode(sha256(convert_to('pulse-durable/maintenance/compact/v1','UTF8')||decode('00','hex')||convert_to(octet_length(ns)::text||':'||ns||octet_length(rid)::text||':'||rid||':'||gen::text||':'||rev::text,'UTF8')),'hex') THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='maintenance plan digest'; END IF;
  IF rev IS NULL OR trunc(rev)<>rev OR rev<1 OR rev>r.revision OR gen IS DISTINCT FROM r.representation_generation OR gen=9223372036854775807 OR rev<(storage_state(r)->>'base_revision')::numeric THEN RAISE EXCEPTION USING ERRCODE='PD005',MESSAGE='representation conflict'; END IF;
  IF rev=1 THEN anchor:=encode(sha256(convert_to('pulse-durable/chain/seed/v1','UTF8')||decode('00','hex')||decode(r.snapshot_digest,'hex')||int8send(1::bigint)),'hex'); ELSE SELECT next_chain INTO anchor FROM pulse_durable_distributed.batches WHERE namespace=ns AND run_id=rid AND revision=rev; END IF;
  IF anchor IS NULL THEN RAISE EXCEPTION USING ERRCODE='PD005',MESSAGE='source missing'; END IF;
  INSERT INTO pulse_durable_distributed.maintenance_guards VALUES(ns,rid,mid,md,'compact_storage');
  INSERT INTO pulse_durable_distributed.build_pins VALUES(ns,rid,mid,session_user,gen,r.revision,r.head_chain,rev,anchor,t+interval '10 minutes',false);
  UPDATE pulse_durable_distributed.runs SET maintenance_watermark=t WHERE namespace=ns AND run_id=rid;
  RETURN jsonb_build_object('status','building','image',storage_image(r,rev,true),'state',storage_state(r));
 END IF;
 IF p.mutation_id IS NULL OR p.holder<>session_user OR p.closed OR p.expires_at<=t THEN RAISE EXCEPTION USING ERRCODE='PD005',MESSAGE='build pin unavailable'; END IF;
 IF op='abort' THEN
  UPDATE pulse_durable_distributed.build_pins SET closed=true WHERE namespace=ns AND run_id=rid AND mutation_id=mid;
  UPDATE pulse_durable_distributed.runs SET maintenance_watermark=t WHERE namespace=ns AND run_id=rid;
  RETURN jsonb_build_object('status','closed','result',NULL);
 END IF;
 IF r.representation_generation<>p.generation OR r.revision<p.source_revision OR (r.revision=p.source_revision AND r.head_chain IS DISTINCT FROM p.source_chain) OR (r.revision>p.source_revision AND ((p.source_revision=1 AND p.source_chain IS DISTINCT FROM encode(sha256(convert_to('pulse-durable/chain/seed/v1','UTF8')||decode('00','hex')||decode(r.snapshot_digest,'hex')||int8send(1::bigint)),'hex')) OR (p.source_revision>1 AND NOT EXISTS(SELECT 1 FROM pulse_durable_distributed.batches WHERE namespace=ns AND run_id=rid AND revision=p.source_revision AND next_chain=p.source_chain)))) THEN RAISE EXCEPTION USING ERRCODE='PD005',MESSAGE='source head conflict'; END IF;
 -- source_revision=1 has no batch row; verify its seed when a suffix appeared.
 b:=decode(a->>'record','base64');
 IF b IS NULL OR octet_length(b) NOT BETWEEN 1 AND 16777216 OR encode(sha256(b),'hex') IS DISTINCT FROM a->>'snapshot_digest' OR a->>'chain' IS DISTINCT FROM p.through_chain THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='invalid synthesized snapshot'; END IF;
 IF NOT (convert_from(b,'UTF8') IS JSON OBJECT WITH UNIQUE KEYS) THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='invalid snapshot JSON'; END IF;
 j:=convert_from(b,'UTF8')::jsonb;
 IF (SELECT count(*) FROM jsonb_object_keys(j))<>2 OR NOT (j ?& ARRAY['version','record']) OR j ?| ARRAY['Version','Record'] OR jsonb_typeof(j->'version') IS DISTINCT FROM 'number' OR j->'version' IS DISTINCT FROM '1'::jsonb OR jsonb_typeof(j->'record') IS DISTINCT FROM 'object' OR (SELECT count(*) FROM jsonb_object_keys(j->'record')) NOT BETWEEN 9 AND 12 OR NOT (j->'record' ?& ARRAY['format','run_id','revision','version','definition','inputs','root_scope','scopes','attempts']) OR EXISTS (SELECT 1 FROM jsonb_object_keys(j->'record') k WHERE k NOT IN ('format','run_id','revision','version','definition','inputs','root_scope','scopes','attempts','streams','messages','managed')) OR jsonb_typeof(j->'record'->'format') IS DISTINCT FROM 'number' OR jsonb_typeof(j->'record'->'run_id') IS DISTINCT FROM 'string' OR jsonb_typeof(j->'record'->'revision') IS DISTINCT FROM 'number' OR jsonb_typeof(j->'record'->'version') IS DISTINCT FROM 'string' OR jsonb_typeof(j->'record'->'definition') IS DISTINCT FROM 'string' OR jsonb_typeof(j->'record'->'inputs') IS DISTINCT FROM 'string' OR jsonb_typeof(j->'record'->'root_scope') IS DISTINCT FROM 'string' OR jsonb_typeof(j->'record'->'scopes') IS DISTINCT FROM 'object' OR jsonb_typeof(j->'record'->'attempts') IS DISTINCT FROM 'object' OR j->'record'->>'run_id' IS DISTINCT FROM rid OR j->'record'->>'revision' IS DISTINCT FROM p.through_revision::text OR j->'record'->>'format' IS DISTINCT FROM r.route->>'record_format' OR j->'record'->>'version' IS DISTINCT FROM r.route->>'business_version' OR j->'record'->>'definition' IS DISTINCT FROM r.route->>'definition' OR j->'record'->>'inputs' IS DISTINCT FROM (convert_from(r.task,'UTF8')::jsonb)->>'inputs' OR j->'record'->>'root_scope' IS DISTINCT FROM r.root_scope THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='snapshot identity'; END IF;
 INSERT INTO pulse_durable_distributed.storage_snapshots VALUES(ns,rid,p.generation+1,p.through_revision,p.through_chain,a->>'snapshot_digest',b);
 -- The run row lock also fences Apply. Recount the actual suffix, including
 -- batches committed after begin, before atomically switching base and budget.
 -- All historical batches remain retained; business head/revision/lease/events do not change.
 SELECT coalesce(sum(octet_length(wire)),0),count(*) INTO suffix_bytes,suffix_count FROM pulse_durable_distributed.batches WHERE namespace=ns AND run_id=rid AND revision>p.through_revision AND revision<=r.revision;
 t:=clock_timestamp();
 IF t<first_sample OR t<r.updated_at OR t<r.maintenance_watermark OR t<r.receipt_observed_through THEN RAISE EXCEPTION USING ERRCODE='PD007',MESSAGE='clock regression'; END IF;
 IF p.expires_at<=t THEN RAISE EXCEPTION USING ERRCODE='PD005',MESSAGE='build pin expired'; END IF;
 UPDATE pulse_durable_distributed.runs SET active_snapshot=p.generation+1,representation_generation=p.generation+1,active_log_bytes=suffix_bytes,active_log_count=suffix_count,maintenance_watermark=t WHERE namespace=ns AND run_id=rid RETURNING * INTO r;
 answer:=jsonb_build_object('mutation_id',mid,'mutation_digest',md,'operation','compact_storage','state',storage_state(r),'occurred_at',t);
 INSERT INTO pulse_durable_distributed.maintenance_receipts VALUES(ns,rid,mid,answer);
 UPDATE pulse_durable_distributed.build_pins SET closed=true WHERE namespace=ns AND run_id=rid AND mutation_id=mid;
 RETURN jsonb_build_object('status','published','result',answer);
END $$;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA pulse_durable_distributed FROM PUBLIC;
