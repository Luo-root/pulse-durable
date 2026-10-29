CREATE SCHEMA pulse_durable_distributed;
REVOKE ALL ON SCHEMA pulse_durable_distributed FROM PUBLIC;
CREATE TABLE pulse_durable_distributed.meta(version integer PRIMARY KEY CHECK(version=6), signature text NOT NULL);
CREATE TABLE pulse_durable_distributed.access(role_name name NOT NULL, namespace text NOT NULL, permission text NOT NULL CHECK(permission IN ('query','worker','submit','cancel','maintain','retain','replay')), PRIMARY KEY(role_name,namespace,permission));
CREATE TABLE pulse_durable_distributed.runs(
 namespace text NOT NULL, run_id text NOT NULL, task bytea NOT NULL, digest text NOT NULL, route jsonb NOT NULL,
 status text NOT NULL DEFAULT 'queued' CHECK(status IN ('queued','running','blocked','succeeded','failed','canceled')),
 epoch bigint NOT NULL DEFAULT 0 CHECK(epoch>=0), revision numeric(20,0) NOT NULL DEFAULT 0 CHECK(revision>=0 AND revision<=18446744073709551615),
 record bytea, snapshot_digest text, head_chain text, root_scope text, active_log_bytes bigint NOT NULL DEFAULT 0 CHECK(active_log_bytes BETWEEN 0 AND 268435456), active_log_count bigint NOT NULL DEFAULT 0 CHECK(active_log_count BETWEEN 0 AND 100000), lease_id text, worker_id text, boot_id text, lease_until timestamptz,
 receipt_coverage_floor timestamptz NOT NULL DEFAULT statement_timestamp(), receipt_observed_through timestamptz,
 representation_generation bigint NOT NULL DEFAULT 1 CHECK(representation_generation>0), active_snapshot bigint, maintenance_watermark timestamptz,
 last_mutation_id text, last_mutation_digest text, reason text NOT NULL DEFAULT '',
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(), updated_at timestamptz NOT NULL DEFAULT clock_timestamp(), finished_at timestamptz,
 PRIMARY KEY(namespace,run_id),
 CHECK((status='running' AND lease_id IS NOT NULL AND worker_id IS NOT NULL AND boot_id IS NOT NULL AND lease_until IS NOT NULL) OR (status<>'running' AND lease_id IS NULL AND worker_id IS NULL AND boot_id IS NULL AND lease_until IS NULL)),
 CHECK((revision=0 AND record IS NULL) OR (revision>0 AND record IS NOT NULL)),
 CHECK((status IN ('succeeded','failed','canceled'))=(finished_at IS NOT NULL)),
 CHECK(octet_length(task)<=8388608 AND octet_length(record)<=16777216)
);
CREATE TABLE pulse_durable_distributed.batches(namespace text NOT NULL, run_id text NOT NULL, revision numeric(20,0) NOT NULL, wire bytea NOT NULL CHECK(octet_length(wire) BETWEEN 1 AND 16777216), digest text NOT NULL, parent_chain text NOT NULL, next_chain text NOT NULL, PRIMARY KEY(namespace,run_id,revision), FOREIGN KEY(namespace,run_id) REFERENCES pulse_durable_distributed.runs(namespace,run_id));
-- Immutable synthesized bases; the original runs.record and all batches remain retained.
CREATE TABLE pulse_durable_distributed.storage_snapshots(namespace text NOT NULL, run_id text NOT NULL, generation bigint NOT NULL CHECK(generation>1), revision numeric(20,0) NOT NULL CHECK(revision BETWEEN 1 AND 18446744073709551615), chain text NOT NULL CHECK(chain ~ '^[0-9a-f]{64}$'), digest text NOT NULL CHECK(digest ~ '^[0-9a-f]{64}$'), record bytea NOT NULL CHECK(octet_length(record) BETWEEN 1 AND 16777216), PRIMARY KEY(namespace,run_id,generation), FOREIGN KEY(namespace,run_id) REFERENCES pulse_durable_distributed.runs(namespace,run_id));
ALTER TABLE pulse_durable_distributed.runs ADD FOREIGN KEY(namespace,run_id,active_snapshot) REFERENCES pulse_durable_distributed.storage_snapshots(namespace,run_id,generation);
CREATE TABLE pulse_durable_distributed.maintenance_guards(namespace text NOT NULL, run_id text NOT NULL, mutation_id text NOT NULL, digest text NOT NULL, operation text NOT NULL CHECK(operation='compact_storage'), PRIMARY KEY(namespace,run_id,mutation_id), FOREIGN KEY(namespace,run_id) REFERENCES pulse_durable_distributed.runs(namespace,run_id));
CREATE TABLE pulse_durable_distributed.build_pins(namespace text NOT NULL, run_id text NOT NULL, mutation_id text NOT NULL, holder name NOT NULL, generation bigint NOT NULL, source_revision numeric(20,0) NOT NULL, source_chain text NOT NULL, through_revision numeric(20,0) NOT NULL, through_chain text NOT NULL, expires_at timestamptz NOT NULL, closed boolean NOT NULL DEFAULT false, PRIMARY KEY(namespace,run_id,mutation_id), FOREIGN KEY(namespace,run_id,mutation_id) REFERENCES pulse_durable_distributed.maintenance_guards(namespace,run_id,mutation_id));
CREATE TABLE pulse_durable_distributed.maintenance_receipts(namespace text NOT NULL, run_id text NOT NULL, mutation_id text NOT NULL, result jsonb NOT NULL, PRIMARY KEY(namespace,run_id,mutation_id), FOREIGN KEY(namespace,run_id,mutation_id) REFERENCES pulse_durable_distributed.maintenance_guards(namespace,run_id,mutation_id));
CREATE INDEX queued_route ON pulse_durable_distributed.runs(namespace,route,run_id) WHERE status='queued';
CREATE INDEX expired_runs ON pulse_durable_distributed.runs(namespace,lease_until,run_id) WHERE status='running';
CREATE TABLE pulse_durable_distributed.leases(namespace text NOT NULL, lease_id text NOT NULL, worker_id text NOT NULL, boot_id text NOT NULL, run_id text, epoch bigint, closed boolean NOT NULL DEFAULT true, claimed_at timestamptz NOT NULL DEFAULT clock_timestamp(), PRIMARY KEY(namespace,lease_id), CHECK((run_id IS NULL AND epoch IS NULL AND closed) OR (run_id IS NOT NULL AND epoch>0)));
CREATE TABLE pulse_durable_distributed.events(namespace text NOT NULL, run_id text NOT NULL, event_seq bigint GENERATED ALWAYS AS IDENTITY, operation text NOT NULL, epoch bigint NOT NULL, revision numeric(20,0) NOT NULL, lease_id text, mutation_id text, mutation_digest text, occurred_at timestamptz NOT NULL, PRIMARY KEY(namespace,run_id,event_seq));
-- Permanent identity guards are never collected with events or receipts.
CREATE TABLE pulse_durable_distributed.mutation_guards(namespace text NOT NULL, run_id text NOT NULL, mutation_id text NOT NULL, digest text NOT NULL CHECK(digest ~ '^[0-9a-f]{64}$'), operation text NOT NULL CHECK(operation IN ('initialize','apply','finish','release','block','cancel')), epoch bigint NOT NULL CHECK(epoch>=0), revision numeric(20,0) NOT NULL CHECK(revision BETWEEN 0 AND 18446744073709551615), occurred_at timestamptz NOT NULL, PRIMARY KEY(namespace,run_id,mutation_id), FOREIGN KEY(namespace,run_id) REFERENCES pulse_durable_distributed.runs(namespace,run_id));
CREATE TABLE pulse_durable_distributed.mutation_receipts(namespace text NOT NULL, run_id text NOT NULL, mutation_id text NOT NULL, occurred_at timestamptz NOT NULL, result jsonb NOT NULL, PRIMARY KEY(namespace,run_id,mutation_id), FOREIGN KEY(namespace,run_id,mutation_id) REFERENCES pulse_durable_distributed.mutation_guards(namespace,run_id,mutation_id));
REVOKE ALL ON ALL TABLES IN SCHEMA pulse_durable_distributed FROM PUBLIC;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA pulse_durable_distributed FROM PUBLIC;
CREATE FUNCTION pulse_durable_distributed.snapshot(r pulse_durable_distributed.runs) RETURNS jsonb LANGUAGE sql IMMUTABLE SET search_path=pg_catalog,pulse_durable_distributed,pg_temp AS $$
 SELECT jsonb_build_object('task',encode(r.task,'base64'),'digest',r.digest,'namespace',r.namespace,'run_id',r.run_id,'route',r.route,'status',r.status,'epoch',r.epoch,'revision',r.revision::text,'lease_until',r.lease_until,'created_at',r.created_at,'updated_at',r.updated_at,'reason',r.reason,'lease_id',r.lease_id,'worker_id',r.worker_id,'boot_id',r.boot_id,'record',encode(r.record,'base64'),'mutation_id',r.last_mutation_id,'mutation_digest',r.last_mutation_digest)
$$;
CREATE FUNCTION pulse_durable_distributed.api(op text, a jsonb) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pulse_durable_distributed,pg_temp AS $$
DECLARE
 ns text:=a->>'namespace'; rid text:=a->>'run_id'; r pulse_durable_distributed.runs; l pulse_durable_distributed.leases; t timestamptz;
 first_sample timestamptz; needed text; b bytea; d jsonb; j jsonb; rev numeric; result jsonb:='[]'::jsonb; slot integer:=0; ttl bigint; lid text; changed boolean:=false;
BEGIN
 IF NOT check_schema() THEN RAISE EXCEPTION USING ERRCODE='42501',MESSAGE='unsafe schema or credentials'; END IF;
 needed:=CASE WHEN op IN ('get','list','find','receipt') THEN 'query' WHEN op IN ('resolve','claim','reject','load','renew','initialize','apply','finish','release','block') THEN 'worker' WHEN op='submit' THEN 'submit' WHEN op='cancel' THEN 'cancel' END;
 IF needed IS NULL THEN RAISE EXCEPTION USING ERRCODE='42501',MESSAGE='unknown operation'; END IF;
 IF ns IS NULL OR octet_length(ns) NOT BETWEEN 1 AND 256 OR NOT EXISTS(SELECT 1 FROM pulse_durable_distributed.access x WHERE x.namespace=ns AND x.permission=needed AND pg_has_role(session_user,x.role_name,'MEMBER')) THEN
  RAISE EXCEPTION USING ERRCODE='42501',MESSAGE='distributed access denied';
 END IF;
 IF op='submit' THEN
  b:=decode(a->>'task','base64');
  IF b IS NULL OR octet_length(b) NOT BETWEEN 1 AND 8388608 THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='invalid'; END IF;
  IF NOT (convert_from(b,'UTF8') IS JSON OBJECT WITH UNIQUE KEYS) THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='invalid JSON'; END IF;
  d:=convert_from(b,'UTF8')::jsonb;
  IF jsonb_typeof(d->'identity'->'namespace') IS DISTINCT FROM 'string' OR jsonb_typeof(d->'identity'->'run_id') IS DISTINCT FROM 'string'
   OR jsonb_typeof(d->'route'->'task_type') IS DISTINCT FROM 'string' OR coalesce(octet_length(d->'route'->>'task_type'),0) NOT BETWEEN 1 AND 256
   OR jsonb_typeof(d->'route'->'business_version') IS DISTINCT FROM 'string' OR coalesce(octet_length(d->'route'->>'business_version'),0) NOT BETWEEN 1 AND 256
   OR jsonb_typeof(d->'route'->'definition') IS DISTINCT FROM 'string' OR jsonb_typeof(d->'inputs') IS DISTINCT FROM 'string'
   OR d->'identity'->>'namespace' IS DISTINCT FROM ns OR d->'identity'->>'run_id' IS DISTINCT FROM rid OR rid IS NULL OR octet_length(rid) NOT BETWEEN 1 AND 256
   OR jsonb_typeof(d->'route'->'protocol') IS DISTINCT FROM 'number' OR jsonb_typeof(d->'route'->'record_format') IS DISTINCT FROM 'number' OR jsonb_typeof(d->'route'->'payload_format') IS DISTINCT FROM 'number'
   OR d->'route'->>'protocol' IS DISTINCT FROM '1' OR d->'route'->>'payload_format' IS DISTINCT FROM '1'
   OR d->'route'->>'record_format' IS DISTINCT FROM '2'
   OR d->'route'->>'definition' !~ '^[0-9a-f]{64}$' OR d->>'inputs' !~ '^[0-9a-f]{64}$'
   OR jsonb_typeof(d->'route'->'manifest_digest') IS DISTINCT FROM 'string' OR d->'route'->>'manifest_digest' !~ '^[0-9a-f]{64}$'
   OR jsonb_typeof(d->'payload') IS DISTINCT FROM 'string' OR jsonb_typeof(d->'payload_digest') IS DISTINCT FROM 'string'
   OR octet_length(decode(d->>'payload','base64'))>4194304
   OR encode(sha256(decode(d->>'payload','base64')),'hex') IS DISTINCT FROM d->>'payload_digest'
   OR encode(sha256(b),'hex') IS DISTINCT FROM a->>'digest'
   OR jsonb_typeof(d->'codec_manifest') IS DISTINCT FROM 'array' OR jsonb_array_length(d->'codec_manifest')>4096
  THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='invalid'; END IF;
  t:=clock_timestamp();
  INSERT INTO runs(namespace,run_id,task,digest,route,created_at,updated_at,receipt_coverage_floor) VALUES(ns,rid,b,a->>'digest',d->'route',t,t,t) ON CONFLICT DO NOTHING;
  changed:=FOUND;
  SELECT * INTO STRICT r FROM runs WHERE namespace=ns AND run_id=rid FOR UPDATE;
  IF r.task<>b OR r.digest<>a->>'digest' THEN RAISE EXCEPTION USING ERRCODE='PD002',MESSAGE='submission conflict'; END IF;
  RETURN jsonb_build_object('snapshot',snapshot(r),'deduplicated',NOT changed);
 ELSIF op='receipt' THEN
  -- The run lock serializes writes and future receipt GC/floor advancement.
  SELECT * INTO r FROM runs WHERE namespace=ns AND run_id=rid FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='PD003',MESSAGE='not found'; END IF;
  t:=clock_timestamp();
  IF t<r.created_at OR t<r.updated_at OR t<r.receipt_coverage_floor OR t<r.receipt_observed_through THEN RAISE EXCEPTION USING ERRCODE='PD007',MESSAGE='clock regression'; END IF;
  IF (a->>'window_from')::timestamptz IS NULL OR (a->>'window_through')::timestamptz IS NULL OR (a->>'window_from')::timestamptz>(a->>'window_through')::timestamptz OR (a->>'window_through')::timestamptz>t THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='invalid receipt window'; END IF;
  IF coalesce(octet_length(a->>'mutation_id'),0) NOT BETWEEN 1 AND 256 OR coalesce(a->>'mutation_digest','') !~ '^[0-9a-f]{64}$' OR a->>'operation' NOT IN ('initialize','apply','finish','release','block','cancel') OR a->>'operation' IS NULL THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='invalid receipt query'; END IF;
  SELECT jsonb_build_object('operation',g.operation,'epoch',g.epoch,'revision',g.revision::text,'mutation_id',g.mutation_id,'mutation_digest',g.digest,'occurred_at',m.occurred_at) INTO result
   FROM mutation_guards g LEFT JOIN mutation_receipts m USING(namespace,run_id,mutation_id) WHERE g.namespace=ns AND g.run_id=rid AND g.mutation_id=a->>'mutation_id';
  IF result IS NOT NULL THEN
   IF result->>'mutation_digest' IS DISTINCT FROM a->>'mutation_digest' OR result->>'operation' IS DISTINCT FROM a->>'operation' THEN RAISE EXCEPTION USING ERRCODE='PD005',MESSAGE='mutation identity conflict'; END IF;
   IF (a ? 'expected_epoch' AND (result->>'epoch')::bigint IS DISTINCT FROM (a->>'expected_epoch')::bigint) OR (a ? 'expected_revision' AND (result->>'revision')::numeric IS DISTINCT FROM (a->>'expected_revision')::numeric) THEN RAISE EXCEPTION USING ERRCODE='PD005',MESSAGE='mutation result conflict'; END IF;
   IF result->>'occurred_at' IS NULL THEN
    IF (SELECT occurred_at FROM mutation_guards WHERE namespace=ns AND run_id=rid AND mutation_id=a->>'mutation_id')>=r.receipt_coverage_floor THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='receipt missing inside coverage'; END IF;
    result:=NULL;
   ELSIF EXISTS(SELECT 1 FROM mutation_receipts m JOIN mutation_guards g USING(namespace,run_id,mutation_id) WHERE m.namespace=ns AND m.run_id=rid AND m.mutation_id=a->>'mutation_id' AND (m.occurred_at IS DISTINCT FROM g.occurred_at OR m.result->>'epoch' IS DISTINCT FROM g.epoch::text OR m.result->>'revision' IS DISTINCT FROM g.revision::text)) THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='receipt result corrupt';
   ELSIF (result->>'occurred_at')::timestamptz>t THEN RAISE EXCEPTION USING ERRCODE='PD007',MESSAGE='receipt in future';
   ELSIF (result->>'occurred_at')::timestamptz<(a->>'window_from')::timestamptz OR (result->>'occurred_at')::timestamptz>(a->>'window_through')::timestamptz THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='receipt outside requested window'; END IF;
  END IF;
  UPDATE runs SET receipt_observed_through=t WHERE namespace=ns AND run_id=rid;
  RETURN jsonb_build_object('state',CASE WHEN result IS NOT NULL THEN 'found' WHEN (a->>'window_from')::timestamptz<r.receipt_coverage_floor THEN 'expired' ELSE 'absent_within_window' END,'coverage_from',r.receipt_coverage_floor,'coverage_through',t,'receipt',result);
 ELSIF op IN ('get','list','find') THEN
  IF op='get' THEN
   SELECT * INTO r FROM runs WHERE namespace=ns AND run_id=rid;
   IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='PD003',MESSAGE='not found'; END IF;
   RETURN snapshot(r);
  END IF;
  IF op='list' THEN
   IF a->>'limit' IS NULL OR (a->>'limit')::integer NOT BETWEEN 1 AND 1024 OR (a->>'status'<>'' AND a->>'status' NOT IN ('queued','running','blocked','succeeded','failed','canceled')) THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='invalid'; END IF;
   FOR r IN SELECT * FROM runs WHERE namespace=ns AND run_id COLLATE "C">coalesce(a->>'after','') COLLATE "C" AND (a->>'status'='' OR status=a->>'status') ORDER BY run_id COLLATE "C" LIMIT (a->>'limit')::integer LOOP result:=result||jsonb_build_array(snapshot(r)); END LOOP;
  ELSE
   FOR r IN SELECT * FROM runs WHERE namespace=ns AND lease_id=a->>'lease_id' AND worker_id=a->>'worker_id' AND boot_id=a->>'boot_id' AND status='running' LOOP result:=result||jsonb_build_array(snapshot(r)); END LOOP;
  END IF;
  RETURN result;
 ELSIF op='resolve' THEN
  lid:=a->>'lease_id';
  IF coalesce(lid,'') !~ '^[0-9a-f]{32,128}$' OR length(lid)%2<>0 OR coalesce(octet_length(a->>'worker_id'),0) NOT BETWEEN 1 AND 256 OR coalesce(octet_length(a->>'boot_id'),0) NOT BETWEEN 1 AND 256 THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='invalid resolution'; END IF;
  -- INSERT waits for any uncommitted claimant. An absent ID is permanently
  -- closed before responding, fencing even a Claim delayed before its INSERT.
  INSERT INTO leases(namespace,lease_id,worker_id,boot_id) VALUES(ns,lid,a->>'worker_id',a->>'boot_id') ON CONFLICT DO NOTHING;
  SELECT * INTO STRICT l FROM leases WHERE namespace=ns AND lease_id=lid FOR UPDATE;
  IF l.worker_id IS DISTINCT FROM a->>'worker_id' OR l.boot_id IS DISTINCT FROM a->>'boot_id' THEN RAISE EXCEPTION USING ERRCODE='42501',MESSAGE='lease identity denied'; END IF;
  IF NOT l.closed THEN
   SELECT * INTO r FROM runs WHERE namespace=ns AND run_id=l.run_id FOR UPDATE;
   t:=clock_timestamp();
   IF FOUND AND (t<r.updated_at OR t<r.receipt_observed_through) THEN RAISE EXCEPTION USING ERRCODE='PD007',MESSAGE='clock regression'; END IF;
   IF FOUND AND r.status='running' AND r.lease_id=lid AND r.epoch=l.epoch AND r.worker_id=l.worker_id AND r.boot_id=l.boot_id AND r.lease_until>t THEN
    RETURN jsonb_build_object('claim',snapshot(r),'closed',false);
   END IF;
   UPDATE leases SET closed=true WHERE namespace=ns AND lease_id=lid;
  END IF;
  RETURN jsonb_build_object('claim',NULL,'closed',true);
 ELSIF op='claim' THEN
  ttl:=(a->>'ttl_us')::bigint;
  IF a->>'limit' IS NULL OR jsonb_typeof(a->'lease_ids') IS DISTINCT FROM 'array' OR jsonb_typeof(a->'capabilities') IS DISTINCT FROM 'array' OR ttl IS NULL OR ttl NOT BETWEEN 1000 AND 86400000000 OR (a->>'limit')::integer NOT BETWEEN 1 AND 1024 OR jsonb_array_length(a->'lease_ids')<>(a->>'limit')::integer OR jsonb_array_length(a->'capabilities') NOT BETWEEN 1 AND 4096
   OR coalesce(octet_length(a->>'worker_id'),0) NOT BETWEEN 1 AND 256 OR coalesce(octet_length(a->>'boot_id'),0) NOT BETWEEN 1 AND 256 THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='invalid'; END IF;
  IF EXISTS(SELECT 1 FROM jsonb_array_elements_text(a->'lease_ids') x WHERE x !~ '^[0-9a-f]{32,128}$' OR length(x)%2<>0) OR (SELECT count(DISTINCT x) FROM jsonb_array_elements_text(a->'lease_ids') x)<>jsonb_array_length(a->'lease_ids') THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='invalid leases'; END IF;
  -- Reserve every slot, including unused slots, before locking any run. Sort
  -- IDs globally within the namespace so overlapping batches cannot deadlock.
  FOR lid IN SELECT value FROM jsonb_array_elements_text(a->'lease_ids') ORDER BY value COLLATE "C" LOOP
   INSERT INTO leases(namespace,lease_id,worker_id,boot_id) VALUES(ns,lid,a->>'worker_id',a->>'boot_id') ON CONFLICT DO NOTHING;
   IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='lease reused or closed'; END IF;
  END LOOP;
  FOR r IN SELECT * FROM runs WHERE namespace=ns AND route IN(SELECT value FROM jsonb_array_elements(a->'capabilities')) AND (status='queued' OR (status='running' AND lease_until<=clock_timestamp())) ORDER BY created_at,run_id COLLATE "C" LIMIT (a->>'limit')::integer FOR UPDATE SKIP LOCKED LOOP
   t:=clock_timestamp();
   IF t<r.updated_at OR t<r.receipt_observed_through THEN RAISE EXCEPTION USING ERRCODE='PD007',MESSAGE='clock regression'; END IF;
   IF r.status<>'queued' AND NOT(r.status='running' AND r.lease_until<=t) THEN CONTINUE; END IF;
   IF r.epoch=9223372036854775807 THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='epoch exhausted'; END IF;
   lid:=a->'lease_ids'->>slot; slot:=slot+1;
   UPDATE leases SET run_id=r.run_id,epoch=r.epoch+1,closed=false,claimed_at=t WHERE namespace=ns AND lease_id=lid;
   UPDATE runs SET status='running',epoch=epoch+1,lease_id=lid,worker_id=a->>'worker_id',boot_id=a->>'boot_id',lease_until=t+ttl*interval '1 microsecond',updated_at=t WHERE namespace=ns AND run_id=r.run_id RETURNING * INTO r;
   INSERT INTO events(namespace,run_id,operation,epoch,revision,lease_id,occurred_at) VALUES(ns,r.run_id,'claim',r.epoch,r.revision,lid,t);
   result:=result||jsonb_build_array(snapshot(r));
  END LOOP;
  RETURN result;
 END IF;
 IF op='reject' THEN
  IF coalesce(a->>'reason','') NOT IN ('lease_lost','revision_conflict','terminal','invalid') THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='invalid reason'; END IF;
  SELECT * INTO r FROM runs WHERE namespace=ns AND run_id=rid;
  IF FOUND THEN INSERT INTO events(namespace,run_id,operation,epoch,revision,lease_id,occurred_at) VALUES(ns,rid,'rejected_'||(a->>'reason'),r.epoch,r.revision,a->>'lease_id',clock_timestamp()); END IF;
  RETURN '{}'::jsonb;
 END IF;
 SELECT * INTO r FROM runs WHERE namespace=ns AND run_id=rid FOR UPDATE;
 IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='PD003',MESSAGE='not found'; END IF;
 t:=clock_timestamp();
 IF t<r.updated_at OR t<r.receipt_observed_through THEN RAISE EXCEPTION USING ERRCODE='PD007',MESSAGE='clock regression',DETAIL='phase=owned_first;delta_us='||trunc(extract(epoch FROM (t-r.updated_at))*1000000)::text; END IF;
 first_sample:=t;
 IF op IN ('initialize','apply','finish','release','block','cancel') AND EXISTS(SELECT 1 FROM mutation_guards WHERE namespace=ns AND run_id=rid AND mutation_id=a->>'mutation_id') THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='mutation reused'; END IF;
 IF op='cancel' AND r.status IN ('succeeded','failed','canceled') THEN RETURN snapshot(r); END IF;
 IF op<>'cancel' THEN
  IF r.status IN ('succeeded','failed','canceled') THEN RAISE EXCEPTION USING ERRCODE='PD006',MESSAGE='terminal'; END IF;
  IF r.status<>'running' OR r.epoch IS DISTINCT FROM (a->>'epoch')::bigint OR r.lease_id IS DISTINCT FROM a->>'lease_id' OR r.worker_id IS DISTINCT FROM a->>'worker_id' OR r.boot_id IS DISTINCT FROM a->>'boot_id' OR r.lease_until<=t THEN RAISE EXCEPTION USING ERRCODE='PD004',MESSAGE='lease lost'; END IF;
 END IF;
 IF op='load' THEN
  -- Ownership has already been checked. An uninitialized run has no history
  -- path yet; missing integrity metadata on an existing run is corruption,
  -- not a requested-revision conflict. Let the verified client classify it.
  IF r.revision=0 OR r.head_chain IS NULL OR r.snapshot_digest IS NULL THEN
   RETURN snapshot(r);
  END IF;
  RETURN storage_image(r,r.revision,true);
 END IF;
 IF op='renew' THEN
  ttl:=(a->>'ttl_us')::bigint;
  IF ttl IS NULL OR ttl NOT BETWEEN 1000 AND 86400000000 THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='invalid'; END IF;
  UPDATE runs SET lease_until=t+ttl*interval '1 microsecond',updated_at=t WHERE namespace=ns AND run_id=rid RETURNING * INTO r;
  RETURN snapshot(r);
 END IF;
 IF coalesce(octet_length(a->>'mutation_id'),0) NOT BETWEEN 1 AND 256 OR coalesce(a->>'mutation_digest','') !~ '^[0-9a-f]{64}$' THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='invalid mutation'; END IF;
 -- All successful business mutations publish guard, receipt and event below.
 IF op IN ('initialize','apply','finish') THEN
  rev:=(a->>'expected')::numeric;
  IF rev IS NULL OR rev<0 OR rev>18446744073709551615 OR trunc(rev)<>rev THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='invalid revision'; END IF;
  IF r.revision<>rev THEN RAISE EXCEPTION USING ERRCODE='PD005',MESSAGE='revision conflict'; END IF;
 END IF;
 IF op='apply' THEN
  b:=decode(a->>'wire','base64');
  IF rev=0 OR rev=18446744073709551615 OR r.head_chain IS DISTINCT FROM a->>'parent_chain' THEN RAISE EXCEPTION USING ERRCODE='PD005',MESSAGE='head conflict'; END IF;
  IF b IS NULL OR octet_length(b) NOT BETWEEN 1 AND 16777216 OR encode(sha256(b),'hex') IS DISTINCT FROM a->>'batch_digest' THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='invalid batch'; END IF;
  -- Only batches after the active base consume budget; retained history is unbounded.
  IF r.active_log_bytes+octet_length(b)>268435456 OR r.active_log_count>=100000 THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='checkpoint log budget exhausted'; END IF;
  IF NOT (convert_from(b,'UTF8') IS JSON OBJECT WITH UNIQUE KEYS) THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='invalid batch JSON'; END IF;
  j:=convert_from(b,'UTF8')::jsonb; d:=convert_from(r.task,'UTF8')::jsonb;
  IF j->>'version' IS DISTINCT FROM '1' OR j->'header'->>'batch_format' IS DISTINCT FROM '1' OR j->'header'->>'logical_format' IS DISTINCT FROM '2'
   OR j->'header'->>'run_id' IS DISTINCT FROM rid OR j->'header'->>'business_version' IS DISTINCT FROM d->'route'->>'business_version' OR j->'header'->>'definition_digest' IS DISTINCT FROM d->'route'->>'definition' OR j->'header'->>'inputs_digest' IS DISTINCT FROM d->>'inputs' OR j->'header'->>'root_scope' IS DISTINCT FROM r.root_scope
   OR j->'header'->>'expected_revision' IS DISTINCT FROM rev::text OR j->'header'->>'revision' IS DISTINCT FROM (rev+1)::text
   OR a->>'next_chain' IS DISTINCT FROM encode(sha256(convert_to('pulse-durable/chain/batch/v1','UTF8')||decode('00','hex')||decode(r.head_chain,'hex')||sha256(b)||decode(lpad(to_hex(trunc((rev+1)/4294967296)::bigint),8,'0')||lpad(to_hex(mod(rev+1,4294967296)::bigint),8,'0'),'hex')),'hex') THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='batch identity'; END IF;
  INSERT INTO batches(namespace,run_id,revision,wire,digest,parent_chain,next_chain) VALUES(ns,rid,rev+1,b,a->>'batch_digest',r.head_chain,a->>'next_chain');
  UPDATE runs SET revision=rev+1,head_chain=a->>'next_chain',active_log_bytes=active_log_bytes+octet_length(b),active_log_count=active_log_count+1 WHERE namespace=ns AND run_id=rid;
 ELSIF op='initialize' THEN
  IF rev<>0 OR r.record IS NOT NULL THEN RAISE EXCEPTION USING ERRCODE='PD005',MESSAGE='already initialized'; END IF;
  b:=decode(a->>'record','base64');
  IF b IS NULL OR octet_length(b) NOT BETWEEN 1 AND 16777216 OR rev=18446744073709551615 THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='invalid record'; END IF;
  IF NOT (convert_from(b,'UTF8') IS JSON OBJECT WITH UNIQUE KEYS) THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='invalid JSON'; END IF;
  j:=convert_from(b,'UTF8')::jsonb; d:=convert_from(r.task,'UTF8')::jsonb;
  IF jsonb_typeof(j->'run_id') IS DISTINCT FROM 'string' OR jsonb_typeof(j->'version') IS DISTINCT FROM 'string' OR jsonb_typeof(j->'definition') IS DISTINCT FROM 'string' OR jsonb_typeof(j->'inputs') IS DISTINCT FROM 'string' OR jsonb_typeof(j->'root_scope') IS DISTINCT FROM 'string'
   OR j->>'run_id' IS DISTINCT FROM rid OR j->>'version' IS DISTINCT FROM d->'route'->>'business_version' OR j->>'definition' IS DISTINCT FROM d->'route'->>'definition' OR j->>'inputs' IS DISTINCT FROM d->>'inputs'
   OR j->>'format' IS DISTINCT FROM d->'route'->>'record_format' OR (j->>'revision')::numeric IS DISTINCT FROM rev+1
   OR jsonb_typeof(j->'revision') IS DISTINCT FROM 'number' OR jsonb_typeof(j->'format') IS DISTINCT FROM 'number'
   OR j->>'revision' !~ '^[1-9][0-9]*$' OR coalesce(j->>'root_scope','')='' OR jsonb_typeof(j->'scopes') IS DISTINCT FROM 'object' OR jsonb_typeof(j->'attempts') IS DISTINCT FROM 'object'
  THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='invalid record'; END IF;
  IF (SELECT count(*) FROM jsonb_object_keys(j->'scopes'))<>1 OR j->'scopes'->(j->>'root_scope') IS NULL OR coalesce(j->'attempts','{}'::jsonb)<>'{}'::jsonb OR coalesce(j->'streams','{}'::jsonb)<>'{}'::jsonb OR coalesce(j->'messages','{}'::jsonb)<>'{}'::jsonb OR coalesce(j->'managed','{}'::jsonb)<>'{}'::jsonb THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='noninitial state'; END IF;
  IF coalesce(j->'scopes'->(j->>'root_scope')->>'graph','')='' OR j->'scopes'->(j->>'root_scope')->>'inputs' IS DISTINCT FROM j->>'inputs' OR coalesce(j->'scopes'->(j->>'root_scope')->'nodes','{}'::jsonb)<>'{}'::jsonb THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='noninitial root'; END IF;
  UPDATE runs SET record=b,revision=1,root_scope=j->>'root_scope',snapshot_digest=encode(sha256(b),'hex'),head_chain=encode(sha256(convert_to('pulse-durable/chain/seed/v1','UTF8')||decode('00','hex')||sha256(b)||int8send(1::bigint)),'hex') WHERE namespace=ns AND run_id=rid;
 ELSIF op='finish' THEN
  IF r.record IS NULL OR a->>'outcome' NOT IN ('completed','failed') OR a->>'outcome' IS NULL THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='invalid outcome'; END IF;
  UPDATE runs SET status=CASE WHEN a->>'outcome'='completed' THEN 'succeeded' ELSE 'failed' END,finished_at=t,lease_id=NULL,worker_id=NULL,boot_id=NULL,lease_until=NULL WHERE namespace=ns AND run_id=rid;
 ELSIF op='release' THEN
  UPDATE runs SET status='queued',lease_id=NULL,worker_id=NULL,boot_id=NULL,lease_until=NULL WHERE namespace=ns AND run_id=rid;
 ELSIF op='block' THEN
  IF coalesce(a->>'reason','') NOT IN ('incompatible','invalid_payload','invalid_record','invalid_task','unsupported_format') THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='invalid reason'; END IF;
  UPDATE runs SET status='blocked',reason=a->>'reason',lease_id=NULL,worker_id=NULL,boot_id=NULL,lease_until=NULL WHERE namespace=ns AND run_id=rid;
 ELSIF op='cancel' THEN
  UPDATE runs SET status='canceled',finished_at=t,lease_id=NULL,worker_id=NULL,boot_id=NULL,lease_until=NULL WHERE namespace=ns AND run_id=rid;
 ELSE RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='invalid operation'; END IF;
 -- Re-sample the database clock after validation and immutable batch insertion.
 -- The row lock still holds; an expired owner cannot publish a head/receipt.
 t := clock_timestamp();
 IF t<r.updated_at OR t<r.receipt_observed_through THEN RAISE EXCEPTION USING ERRCODE='PD007',MESSAGE='clock regression',DETAIL='phase=owned_final;delta_us='||trunc(extract(epoch FROM (t-r.updated_at))*1000000)::text||';step_us='||trunc(extract(epoch FROM (t-first_sample))*1000000)::text; END IF;
 IF op<>'cancel' AND r.lease_until<=t THEN RAISE EXCEPTION USING ERRCODE='PD004',MESSAGE='lease lost'; END IF;
 UPDATE runs SET reason=CASE WHEN status='blocked' THEN reason ELSE '' END,lease_id=CASE WHEN status='running' THEN lease_id END,worker_id=CASE WHEN status='running' THEN worker_id END,boot_id=CASE WHEN status='running' THEN boot_id END,lease_until=CASE WHEN status='running' THEN lease_until END,last_mutation_id=a->>'mutation_id',last_mutation_digest=a->>'mutation_digest',updated_at=t WHERE namespace=ns AND run_id=rid RETURNING * INTO r;
 INSERT INTO mutation_guards VALUES(ns,rid,a->>'mutation_id',a->>'mutation_digest',op,r.epoch,r.revision,t);
 INSERT INTO mutation_receipts VALUES(ns,rid,a->>'mutation_id',t,jsonb_build_object('status',r.status,'epoch',r.epoch,'revision',r.revision::text));
 INSERT INTO events(namespace,run_id,operation,epoch,revision,lease_id,mutation_id,mutation_digest,occurred_at) VALUES(ns,rid,op,r.epoch,r.revision,a->>'lease_id',a->>'mutation_id',a->>'mutation_digest',t);
 IF op='apply' THEN RETURN '{}'::jsonb; END IF;
 RETURN snapshot(r);
END $$;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA pulse_durable_distributed FROM PUBLIC;
INSERT INTO pulse_durable_distributed.meta VALUES(6,'');
