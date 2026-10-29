-- Frozen schema5 receipt-relevant layout from 70479aa; intentionally omits
-- unrelated maintenance functions. This is not schema6 with a changed version.
CREATE SCHEMA pulse_durable_distributed;
REVOKE ALL ON SCHEMA pulse_durable_distributed FROM PUBLIC;
CREATE TABLE pulse_durable_distributed.meta(version integer PRIMARY KEY CHECK(version=5), signature text NOT NULL);
CREATE TABLE pulse_durable_distributed.runs(
 namespace text NOT NULL, run_id text NOT NULL, task bytea NOT NULL, digest text NOT NULL, route jsonb NOT NULL,
 status text NOT NULL DEFAULT 'queued' CHECK(status IN ('queued','running','blocked','succeeded','failed','canceled')),
 epoch bigint NOT NULL DEFAULT 0 CHECK(epoch>=0), revision numeric(20,0) NOT NULL DEFAULT 0 CHECK(revision>=0 AND revision<=18446744073709551615),
 record bytea, snapshot_digest text, head_chain text, root_scope text, active_log_bytes bigint NOT NULL DEFAULT 0 CHECK(active_log_bytes BETWEEN 0 AND 268435456), active_log_count bigint NOT NULL DEFAULT 0 CHECK(active_log_count BETWEEN 0 AND 100000), lease_id text, worker_id text, boot_id text, lease_until timestamptz,
 representation_generation bigint NOT NULL DEFAULT 1 CHECK(representation_generation>0), active_snapshot bigint, maintenance_watermark timestamptz,
 last_mutation_id text, last_mutation_digest text, reason text NOT NULL DEFAULT '',
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(), updated_at timestamptz NOT NULL DEFAULT clock_timestamp(), finished_at timestamptz,
 PRIMARY KEY(namespace,run_id),
 CHECK((status='running' AND lease_id IS NOT NULL AND worker_id IS NOT NULL AND boot_id IS NOT NULL AND lease_until IS NOT NULL) OR (status<>'running' AND lease_id IS NULL AND worker_id IS NULL AND boot_id IS NULL AND lease_until IS NULL)),
 CHECK((revision=0 AND record IS NULL) OR (revision>0 AND record IS NOT NULL)),
 CHECK((status IN ('succeeded','failed','canceled'))=(finished_at IS NOT NULL)),
 CHECK(octet_length(task)<=8388608 AND octet_length(record)<=16777216)
);
CREATE TABLE pulse_durable_distributed.events(namespace text NOT NULL, run_id text NOT NULL, event_seq bigint GENERATED ALWAYS AS IDENTITY, operation text NOT NULL, epoch bigint NOT NULL, revision numeric(20,0) NOT NULL, lease_id text, mutation_id text, mutation_digest text, occurred_at timestamptz NOT NULL, PRIMARY KEY(namespace,run_id,event_seq));
CREATE UNIQUE INDEX mutation_guard ON pulse_durable_distributed.events(namespace,run_id,mutation_id) WHERE mutation_id IS NOT NULL;
INSERT INTO pulse_durable_distributed.meta VALUES(5,'legacy-signature');
INSERT INTO pulse_durable_distributed.runs(namespace,run_id,task,digest,route) VALUES('legacy','retained',decode('0001ff00','hex'),'fixture','{}');
REVOKE ALL ON ALL TABLES IN SCHEMA pulse_durable_distributed FROM PUBLIC;
REVOKE ALL ON ALL SEQUENCES IN SCHEMA pulse_durable_distributed FROM PUBLIC;
