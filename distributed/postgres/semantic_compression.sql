-- Distributed semantic compression is a dedicated maintenance mutation.
-- The SQL side reconstructs the current logical record from the authoritative
-- snapshot/batch chain, verifies the submitted wire against that record, and
-- publishes the new batch, ledger, guard and receipt in one transaction.

CREATE FUNCTION pulse_durable_distributed.semantic_quote(v text) RETURNS text
LANGUAGE sql IMMUTABLE STRICT SET search_path=pg_catalog,pulse_durable_distributed,pg_temp AS $$
 SELECT replace(replace(replace(replace(replace(to_json(v)::text,'<','\u003c'),'>','\u003e'),'&','\u0026'),chr(8232),'\u2028'),chr(8233),'\u2029')
$$;

CREATE FUNCTION pulse_durable_distributed.semantic_digest(VARIADIC fields text[]) RETURNS text
LANGUAGE sql IMMUTABLE SET search_path=pg_catalog,pulse_durable_distributed,pg_temp AS $$
 SELECT encode(sha256(convert_to('['||coalesce(string_agg(semantic_quote(v),',' ORDER BY n),'')||']','UTF8')),'hex')
 FROM unnest(fields) WITH ORDINALITY f(v,n)
$$;

CREATE FUNCTION pulse_durable_distributed.semantic_summary_digest(VARIADIC fields text[]) RETURNS text
LANGUAGE sql IMMUTABLE SET search_path=pg_catalog,pulse_durable_distributed,pg_temp AS $$
 SELECT encode(sha256(convert_to(fields[1]||coalesce((SELECT string_agg(chr(0)||v,'' ORDER BY n) FROM unnest(fields[2:]) WITH ORDINALITY f(v,n)),''),'UTF8')),'hex')
$$;

CREATE FUNCTION pulse_durable_distributed.semantic_provenance_digest(run_id text,revision text,record_digest text,compression_id text) RETURNS text
LANGUAGE sql IMMUTABLE SET search_path=pg_catalog,pulse_durable_distributed,pg_temp AS $$
 SELECT encode(sha256(convert_to('{"domain":'||semantic_quote('provenance/v1')||',"values":['||semantic_quote(run_id)||','||revision::numeric::text||','||semantic_quote(record_digest)||','||semantic_quote(compression_id)||']}','UTF8')),'hex')
$$;
CREATE FUNCTION pulse_durable_distributed.semantic_wire_bytes(v jsonb) RETURNS text
LANGUAGE plpgsql IMMUTABLE SET search_path=pg_catalog,pulse_durable_distributed,pg_temp AS $$
DECLARE h text;
BEGIN
 IF jsonb_typeof(v) IS DISTINCT FROM 'array' THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='invalid wire bytes'; END IF;
 IF EXISTS(SELECT 1 FROM jsonb_array_elements(v) AS elem(value) WHERE jsonb_typeof(elem.value)<>'number' OR elem.value::text !~ '^(0|[1-9][0-9]{0,2})$' OR elem.value::numeric>255) THEN
  RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='invalid wire byte';
 END IF;
 SELECT coalesce(string_agg(lpad(to_hex((elem.value #>> '{}')::integer),2,'0'),'' ORDER BY elem.n),'') INTO h
 FROM jsonb_array_elements(v) WITH ORDINALITY AS elem(value,n);
 -- PostgreSQL 的 base64 每 76 字符换行；Go encoding/json 不换行。
 -- 必须去掉换行，否则与权威快照的精确字节比较会误判为 CAS 冲突。
 RETURN replace(encode(decode(h,'hex'),'base64'),E'\n','');
END $$;

-- Convert the Go ChangeBatch wire representation into the snapshot representation.
-- Wire uint64 values are strings and []byte values are integer arrays; snapshots
-- use JSON numbers and base64 strings. This helper is only a decoder, never an
-- authority decision.
CREATE FUNCTION pulse_durable_distributed.semantic_wire_to_snapshot(v jsonb,shape text) RETURNS jsonb
LANGUAGE plpgsql IMMUTABLE SET search_path=pg_catalog,pulse_durable_distributed,pg_temp AS $$
DECLARE out jsonb; child text;
BEGIN
 IF v IS NULL OR v='null'::jsonb THEN RETURN 'null'::jsonb; END IF;
 IF right(shape,1)='?' THEN RETURN semantic_wire_to_snapshot(v,left(shape,length(shape)-1)); END IF;
 IF right(shape,2)='[]' THEN
  SELECT coalesce(jsonb_agg(semantic_wire_to_snapshot(elem.value,left(shape,length(shape)-2))),'[]'::jsonb) INTO out
  FROM jsonb_array_elements(v) AS elem(value);
  RETURN out;
 END IF;
 IF right(shape,3)='map' THEN
  child:=left(shape,length(shape)-3);
  SELECT coalesce(jsonb_object_agg(elem.k,semantic_wire_to_snapshot(elem.v,child)),'{}'::jsonb) INTO out FROM jsonb_each(v) AS elem(k,v);
  RETURN out;
 END IF;
 IF shape='bytes' THEN RETURN to_jsonb(semantic_wire_bytes(v)); END IF;
 IF shape='id' THEN
  RETURN jsonb_build_object('scope',v->'scope','node_id',v->'node_id','key',v->'key','group',v->'group','generation',(v->>'generation')::numeric);
 ELSIF shape='value' THEN
  RETURN jsonb_build_object('name',v->'name','type_id',v->'type_id','version',v->'version','state',v->'state','digest',v->'digest','data',CASE WHEN v->'data'='null'::jsonb THEN 'null'::jsonb ELSE to_jsonb(semantic_wire_bytes(v->'data')) END,'stream',CASE WHEN v->'stream'='null'::jsonb THEN 'null'::jsonb ELSE semantic_wire_to_snapshot(v->'stream','descriptor') END);
 ELSIF shape='descriptor' THEN
  RETURN jsonb_build_object('id',semantic_wire_to_snapshot(v->'id','id'),'graph',v->'graph','path',v->'path','attempt',v->'attempt','type_id',v->'type_id','version',v->'version','source_id',v->'source_id','resumable',v->'resumable','capacity',v->'capacity','terminal',v->'terminal','stopped',v->'stopped');
 ELSIF shape='node' THEN
  RETURN jsonb_build_object('node_id',v->'node_id','attempt',v->'attempt','reason',v->'reason','outputs',semantic_wire_to_snapshot(v->'outputs','value[]?'));
 ELSIF shape='scope' THEN
  RETURN jsonb_build_object('graph',v->'graph','path',v->'path','inputs',v->'inputs','nodes',semantic_wire_to_snapshot(v->'nodes','nodemap?'));
 ELSIF shape='attempt' THEN
  RETURN jsonb_build_object('parent',v->'parent','node_id',v->'node_id','boundary',v->'boundary','index',v->'index','limit',v->'limit','state',v->'state');
 ELSIF shape='stream_header' THEN
  RETURN jsonb_build_object('stream',semantic_wire_to_snapshot(v->'Stream','descriptor'),'last_sequence',(v->>'LastSequence')::numeric,'acked_through',(v->>'AckedThrough')::numeric,'pending','null'::jsonb,'cursor',CASE WHEN v->'Cursor'='null'::jsonb THEN 'null'::jsonb ELSE semantic_wire_to_snapshot(v->'Cursor','value') END);
 ELSIF shape='pending' THEN
  RETURN jsonb_build_object('sequence',(v->>'sequence')::numeric,'value',semantic_wire_to_snapshot(v->'value','value'));
 ELSIF shape='message' THEN
  RETURN jsonb_build_object('scope',v->'scope','parent',v->'parent','node_id',v->'node_id','input',semantic_wire_to_snapshot(v->'input','id'),'sequence',(v->>'sequence')::numeric,'index',v->'index','limit',v->'limit','state',v->'state');
 ELSIF shape='managed_header' THEN
  RETURN jsonb_build_object('scope',v->>'Scope','parent',v->>'Parent','node_id',v->>'NodeID');
 ELSIF shape='history_summary' THEN
  RETURN jsonb_build_object('kind',v->'kind','id',v->'id','source_revision',(v->>'source_revision')::numeric,'source_digest',v->'source_digest','payload',to_jsonb(semantic_wire_bytes(v->'payload')),'digest',v->'digest');
 ELSIF shape='commit_index' THEN
  RETURN jsonb_build_object('revision',(v->>'revision')::numeric,'parent_revision',(v->>'parent_revision')::numeric,'batch_digest',v->'batch_digest');
 ELSIF shape='provenance' THEN
  RETURN jsonb_build_object('origin_run_id',v->'origin_run_id','origin_revision',(v->>'origin_revision')::numeric,'origin_record_digest',v->'origin_record_digest','compression_id',v->'compression_id','digest',v->'digest');
 END IF;
 RETURN v;
END $$;

CREATE FUNCTION pulse_durable_distributed.semantic_array_upsert(a jsonb,field_name text,key_value text,value jsonb,remove_value boolean) RETURNS jsonb
LANGUAGE plpgsql IMMUTABLE SET search_path=pg_catalog,pulse_durable_distributed,pg_temp AS $$
DECLARE e jsonb; out jsonb:='[]'::jsonb; found boolean:=false;
BEGIN
 IF a IS NULL OR a='null'::jsonb THEN a:='[]'::jsonb; END IF;
 FOR e IN SELECT elem.value FROM jsonb_array_elements(a) AS elem(value) LOOP
  IF e->>field_name=key_value THEN
   found:=true;
   IF NOT remove_value THEN out:=out||jsonb_build_array(value); END IF;
  ELSE out:=out||jsonb_build_array(e); END IF;
 END LOOP;
 IF NOT found AND NOT remove_value THEN out:=out||jsonb_build_array(value); END IF;
 RETURN out;
END $$;

CREATE FUNCTION pulse_durable_distributed.semantic_summary_upsert(a jsonb,kind_value text,id_value text,value jsonb,remove_value boolean) RETURNS jsonb
LANGUAGE plpgsql IMMUTABLE SET search_path=pg_catalog,pulse_durable_distributed,pg_temp AS $$
DECLARE e jsonb; out jsonb:='[]'::jsonb; found boolean:=false;
BEGIN
 IF a IS NULL OR a='null'::jsonb THEN a:='[]'::jsonb; END IF;
 FOR e IN SELECT elem.value FROM jsonb_array_elements(a) AS elem(value) LOOP
  IF e->>'kind'=kind_value AND e->>'id'=id_value THEN
   found:=true;
   IF NOT remove_value THEN out:=out||jsonb_build_array(value); END IF;
  ELSE out:=out||jsonb_build_array(e); END IF;
 END LOOP;
 IF NOT found AND NOT remove_value THEN out:=out||jsonb_build_array(value); END IF;
 RETURN out;
END $$;

CREATE FUNCTION pulse_durable_distributed.semantic_apply_wire_batch(base jsonb,b jsonb) RETURNS jsonb
LANGUAGE plpgsql SET search_path=pg_catalog,pulse_durable_distributed,pg_temp AS $$
DECLARE rec jsonb:=base; op jsonb; kind text; key text; expected jsonb; current jsonb; value jsonb; stream jsonb; managed jsonb; arr jsonb; exists_value boolean; expected_null boolean; revision numeric; header jsonb:=b->'header';
BEGIN
 IF jsonb_typeof(b) IS DISTINCT FROM 'object' OR b->>'version' IS DISTINCT FROM '2' OR jsonb_typeof(header) IS DISTINCT FROM 'object' THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='invalid batch envelope'; END IF;
 IF (header->>'expected_revision') IS DISTINCT FROM rec->'record'->>'revision' OR (header->>'revision')::numeric IS DISTINCT FROM (rec->'record'->>'revision')::numeric+1 THEN RAISE EXCEPTION USING ERRCODE='PD005',MESSAGE='batch revision conflict'; END IF;
 IF jsonb_typeof(b->'ops') IS DISTINCT FROM 'array' OR jsonb_array_length(b->'ops')>4096 THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='invalid batch operations'; END IF;
 FOR op IN SELECT elem.value FROM jsonb_array_elements(b->'ops') AS elem(value) LOOP
  kind:=op->>'kind'; key:=op->>'key'; expected:=op->'expected'; expected_null:=expected='null'::jsonb; current:=NULL; exists_value:=false;
  CASE kind
   WHEN 'attempt' THEN current:=rec->'record'->'attempts'->key; exists_value:=current IS NOT NULL; value:=CASE WHEN op->'value'='null'::jsonb THEN NULL ELSE semantic_wire_to_snapshot(op->'value','attempt') END;
   WHEN 'scope' THEN current:=rec->'record'->'scopes'->key; exists_value:=current IS NOT NULL; value:=semantic_wire_to_snapshot(op->'value','scope');
   WHEN 'node' THEN current:=rec->'record'->'scopes'->(op->>'scope')->'nodes'->(op->>'name'); exists_value:=current IS NOT NULL; value:=semantic_wire_to_snapshot(op->'value','node');
   WHEN 'stream' THEN stream:=rec->'record'->'streams'->key; current:=CASE WHEN stream IS NULL THEN NULL ELSE jsonb_build_object('stream',stream->'stream','last_sequence',stream->'last_sequence','acked_through',stream->'acked_through','pending','null'::jsonb,'cursor',stream->'cursor') END; exists_value:=current IS NOT NULL; value:=semantic_wire_to_snapshot(op->'value','stream_header');
   WHEN 'pending' THEN arr:=rec->'record'->'streams'->(op->>'scope')->'pending'; SELECT x.value INTO current FROM jsonb_array_elements(coalesce(arr,'[]'::jsonb)) x WHERE x.value->>'sequence'=(op->>'sequence') LIMIT 1; exists_value:=current IS NOT NULL; value:=CASE WHEN op->'value'='null'::jsonb THEN NULL ELSE semantic_wire_to_snapshot(op->'value','pending') END;
   WHEN 'message' THEN current:=rec->'record'->'messages'->key; exists_value:=current IS NOT NULL; value:=CASE WHEN op->'value'='null'::jsonb THEN NULL ELSE semantic_wire_to_snapshot(op->'value','message') END;
   WHEN 'managed' THEN managed:=rec->'record'->'managed'->key; current:=CASE WHEN managed IS NULL THEN NULL ELSE jsonb_build_object('Scope',managed->'scope','Parent',managed->'parent','NodeID',managed->'node_id') END; exists_value:=current IS NOT NULL; value:=semantic_wire_to_snapshot(op->'value','managed_header');
   WHEN 'managed_entry' THEN managed:=rec->'record'->'managed'->(op->>'scope'); arr:=CASE WHEN op->>'state'='true' THEN managed->'state' ELSE managed->'values' END; SELECT x.value INTO current FROM jsonb_array_elements(coalesce(arr,'[]'::jsonb)) x WHERE x.value->>'name'=(op->>'name') LIMIT 1; exists_value:=current IS NOT NULL; value:=CASE WHEN op->'value'='null'::jsonb THEN NULL ELSE semantic_wire_to_snapshot(op->'value','value') END;
   WHEN 'history_summary' THEN arr:=rec->'record'->'history_summaries'; SELECT x.value INTO current FROM jsonb_array_elements(coalesce(arr,'[]'::jsonb)) x WHERE x.value->>'kind'=op->>'scope' AND x.value->>'id'=op->>'name' LIMIT 1; exists_value:=current IS NOT NULL; value:=CASE WHEN op->'value'='null'::jsonb THEN NULL ELSE semantic_wire_to_snapshot(op->'value','history_summary') END;
   WHEN 'commit_index' THEN arr:=rec->'record'->'commit_index'; SELECT x.value INTO current FROM jsonb_array_elements(coalesce(arr,'[]'::jsonb)) x WHERE x.value->>'revision'=op->>'key' LIMIT 1; exists_value:=current IS NOT NULL; value:=CASE WHEN op->'value'='null'::jsonb THEN NULL ELSE semantic_wire_to_snapshot(op->'value','commit_index') END;
   WHEN 'provenance' THEN current:=rec->'record'->'provenance'; exists_value:=current IS NOT NULL; value:=CASE WHEN op->'value'='null'::jsonb THEN NULL ELSE semantic_wire_to_snapshot(op->'value','provenance') END;
   ELSE RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='unknown batch operation';
  END CASE;
  IF expected_null THEN
   IF exists_value THEN RAISE EXCEPTION USING ERRCODE='PD005',MESSAGE='batch absent precondition'; END IF;
  ELSE
   IF NOT exists_value OR current IS DISTINCT FROM semantic_wire_to_snapshot(expected,CASE kind WHEN 'attempt' THEN 'attempt' WHEN 'scope' THEN 'scope' WHEN 'node' THEN 'node' WHEN 'stream' THEN 'stream_header' WHEN 'pending' THEN 'pending' WHEN 'message' THEN 'message' WHEN 'managed' THEN 'managed_header' WHEN 'managed_entry' THEN 'value' WHEN 'history_summary' THEN 'history_summary' WHEN 'commit_index' THEN 'commit_index' WHEN 'provenance' THEN 'provenance' END) THEN RAISE EXCEPTION USING ERRCODE='PD005',MESSAGE='batch value precondition'; END IF;
  END IF;
  IF kind='attempt' THEN rec:=jsonb_set(rec,'{record,attempts}',CASE WHEN value IS NULL THEN (rec->'record'->'attempts') - key ELSE jsonb_set(rec->'record'->'attempts',ARRAY[key],value,true) END,true);
  ELSIF kind='scope' THEN rec:=jsonb_set(rec,'{record,scopes}',jsonb_set(rec->'record'->'scopes',ARRAY[key],value,true),true);
  ELSIF kind='node' THEN rec:=jsonb_set(rec,'{record,scopes}',jsonb_set(rec->'record'->'scopes',ARRAY[op->>'scope','nodes',op->>'name'],value,true),true);
  ELSIF kind='stream' THEN stream:=rec->'record'->'streams'->key; rec:=jsonb_set(rec,'{record,streams}',jsonb_set(rec->'record'->'streams',ARRAY[key],stream||jsonb_build_object('stream',value->'stream','last_sequence',value->'last_sequence','acked_through',value->'acked_through','cursor',value->'cursor'),true),true);
  ELSIF kind='pending' THEN rec:=jsonb_set(rec,'{record,streams}',jsonb_set(rec->'record'->'streams',ARRAY[op->>'scope','pending'],semantic_array_upsert(arr,op->>'sequence',op->>'sequence',value,value IS NULL),true),true);
  ELSIF kind='message' THEN rec:=jsonb_set(rec,'{record,messages}',CASE WHEN value IS NULL THEN (rec->'record'->'messages') - key ELSE jsonb_set(rec->'record'->'messages',ARRAY[key],value,true) END,true);
  ELSIF kind='managed' THEN managed:=rec->'record'->'managed'->key; rec:=jsonb_set(rec,'{record,managed}',jsonb_set(rec->'record'->'managed',ARRAY[key],managed||jsonb_build_object('scope',value->>'scope','parent',value->>'parent','node_id',value->>'node_id'),true),true);
  ELSIF kind='managed_entry' THEN managed:=rec->'record'->'managed'->(op->>'scope'); rec:=jsonb_set(rec,'{record,managed}',jsonb_set(rec->'record'->'managed',ARRAY[op->>'scope',CASE WHEN op->>'state'='true' THEN 'state' ELSE 'values' END],semantic_array_upsert(arr,op->>'name',op->>'name',value,value IS NULL),true),true);
  ELSIF kind='history_summary' THEN rec:=jsonb_set(rec,'{record,history_summaries}',semantic_summary_upsert(arr,op->>'scope',op->>'name',value,value IS NULL),true);
  ELSIF kind='commit_index' THEN rec:=jsonb_set(rec,'{record,commit_index}',semantic_array_upsert(arr,'revision',op->>'key',value,value IS NULL),true);
  ELSIF kind='provenance' THEN rec:=jsonb_set(rec,'{record,provenance}',value,true);
  END IF;
 END LOOP;
 revision:=(header->>'revision')::numeric;
 RETURN jsonb_set(rec,'{record,revision}',to_jsonb(revision),true);
END $$;

CREATE FUNCTION pulse_durable_distributed.semantic_replay_image(r pulse_durable_distributed.runs,image jsonb) RETURNS jsonb
LANGUAGE plpgsql SET search_path=pg_catalog,pulse_durable_distributed,pg_temp AS $$
DECLARE rec jsonb; entry jsonb; wire bytea; b jsonb; base numeric; expected numeric; chain text; computed text;
BEGIN
 IF image IS NULL OR image->>'record' IS NULL OR image->>'base_revision' IS NULL OR image->>'base_chain' IS NULL THEN RAISE EXCEPTION USING ERRCODE='HG004',MESSAGE='authoritative image missing'; END IF;
 wire:=decode(image->>'record','base64');
 IF wire IS NULL OR encode(sha256(wire),'hex') IS DISTINCT FROM image->>'snapshot_digest' THEN RAISE EXCEPTION USING ERRCODE='HG004',MESSAGE='authoritative image digest'; END IF;
 rec:=convert_from(wire,'UTF8')::jsonb; base:=(image->>'base_revision')::numeric; chain:=image->>'base_chain'; expected:=base+1;
 IF image->>'revision' IS DISTINCT FROM r.revision::text OR image->>'head_chain' IS DISTINCT FROM r.head_chain THEN RAISE EXCEPTION USING ERRCODE='HG004',MESSAGE='authoritative image head'; END IF;
 FOR entry IN SELECT value FROM jsonb_array_elements(coalesce(image->'batches','[]'::jsonb)) LOOP
  IF (entry->>'revision')::numeric IS DISTINCT FROM expected OR entry->>'parent_chain' IS DISTINCT FROM chain THEN RAISE EXCEPTION USING ERRCODE='HG004',MESSAGE='authoritative batch gap'; END IF;
  wire:=decode(entry->>'wire','base64');
  IF wire IS NULL OR encode(sha256(wire),'hex') IS DISTINCT FROM entry->>'digest' THEN RAISE EXCEPTION USING ERRCODE='HG004',MESSAGE='authoritative batch digest'; END IF;
  computed:=encode(sha256(convert_to('pulse-durable/chain/batch/v1','UTF8')||decode('00','hex')||decode(chain,'hex')||decode(entry->>'digest','hex')||decode(lpad(to_hex(trunc(expected/4294967296)::bigint),8,'0')||lpad(to_hex(mod(expected,4294967296)::bigint),8,'0'),'hex')),'hex');
  IF computed IS DISTINCT FROM entry->>'next_chain' THEN RAISE EXCEPTION USING ERRCODE='HG004',MESSAGE='authoritative batch chain'; END IF;
  b:=convert_from(wire,'UTF8')::jsonb; rec:=semantic_apply_wire_batch(rec,b); chain:=computed; expected:=expected+1;
 END LOOP;
 IF expected<>r.revision+1 OR chain IS DISTINCT FROM r.head_chain THEN RAISE EXCEPTION USING ERRCODE='HG004',MESSAGE='authoritative image incomplete'; END IF;
 RETURN rec;
END $$;

CREATE FUNCTION pulse_durable_distributed.semantic_validate_record(v jsonb,r pulse_durable_distributed.runs,expected_revision numeric) RETURNS void
LANGUAGE plpgsql SET search_path=pg_catalog,pulse_durable_distributed,pg_temp AS $$
DECLARE rec jsonb:=v->'record'; s jsonb; p jsonb; e jsonb; n numeric; payload jsonb; expected_source text;
BEGIN
 IF v->>'version' IS DISTINCT FROM '2' OR jsonb_typeof(rec) IS DISTINCT FROM 'object' OR rec->>'run_id' IS DISTINCT FROM r.run_id OR rec->>'revision' IS DISTINCT FROM expected_revision::text OR rec->>'version' IS DISTINCT FROM r.route->>'business_version' OR rec->>'definition' IS DISTINCT FROM r.route->>'definition' OR rec->>'inputs' IS DISTINCT FROM (convert_from(r.task,'UTF8')::jsonb)->>'inputs' OR rec->>'root_scope' IS NULL OR jsonb_typeof(rec->'scopes') IS DISTINCT FROM 'object' OR jsonb_typeof(rec->'attempts') IS DISTINCT FROM 'object' THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='semantic record identity'; END IF;
 IF jsonb_typeof(rec->'history_summaries') IS DISTINCT FROM 'array' OR jsonb_array_length(rec->'history_summaries')=0 OR jsonb_typeof(rec->'commit_index') IS DISTINCT FROM 'array' OR jsonb_array_length(rec->'commit_index')<>expected_revision OR jsonb_typeof(rec->'provenance') IS DISTINCT FROM 'object' THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='semantic evidence incomplete'; END IF;
 FOR e IN SELECT elem.value FROM jsonb_array_elements(rec->'commit_index') AS elem(value) LOOP
  IF jsonb_typeof(e) IS DISTINCT FROM 'object' OR e->>'revision' IS NULL OR e->>'parent_revision' IS NULL OR e->>'batch_digest' !~ '^[0-9a-f]{64}$' THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='semantic commit index invalid'; END IF;
 END LOOP;
 p:=rec->'provenance';
 IF p->>'origin_run_id' IS NULL OR p->>'origin_revision' IS NULL OR p->>'origin_record_digest' !~ '^[0-9a-f]{64}$' OR p->>'compression_id' IS NULL OR p->>'digest' IS DISTINCT FROM semantic_provenance_digest(p->>'origin_run_id',p->>'origin_revision',p->>'origin_record_digest',p->>'compression_id') THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='semantic provenance invalid'; END IF;
 FOR s IN SELECT elem.value FROM jsonb_array_elements(rec->'history_summaries') AS elem(value) LOOP
  IF s->>'kind' NOT IN ('retry_prefix','message_prefix','history_evidence') OR s->>'id' IS NULL OR s->>'source_revision' IS NULL OR s->>'source_digest' !~ '^[0-9a-f]{64}$' OR s->>'digest' !~ '^[0-9a-f]{64}$' OR s->>'payload' IS NULL OR encode(sha256(decode(s->>'payload','base64')),'hex') IS DISTINCT FROM s->>'digest' THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='semantic summary invalid'; END IF;
  payload:=convert_from(decode(s->>'payload','base64'),'UTF8')::jsonb;
  IF s->>'kind'='history_evidence' AND (payload->>'schema' IS DISTINCT FROM 'history-evidence/v1' OR payload->>'source' IS DISTINCT FROM 'committed-chain') THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='history evidence marker invalid'; END IF;
  IF s->>'kind' IN ('retry_prefix','message_prefix') AND (payload->>'applied_revision') IS NULL THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='compression summary application missing'; END IF;
 END LOOP;
END $$;

CREATE FUNCTION pulse_durable_distributed.semantic_validate_candidates(v jsonb,b jsonb) RETURNS bigint
LANGUAGE plpgsql SET search_path=pg_catalog,pulse_durable_distributed,pg_temp AS $$
DECLARE rec jsonb:=v->'record'; s jsonb; payload jsonb; x record; expected jsonb:='[]'::jsonb; through numeric; idx numeric; seen jsonb; last_id text; count_rows bigint; actual bigint; stream jsonb; managed jsonb; seq numeric; maxidx numeric; actual_ids jsonb; kind text; latest_state text; group_limit numeric;
BEGIN
 FOR s IN SELECT elem.value FROM jsonb_array_elements(rec->'history_summaries') AS elem(value) WHERE elem.value->>'kind' IN ('retry_prefix','message_prefix') LOOP
  payload:=convert_from(decode(s->>'payload','base64'),'UTF8')::jsonb; kind:=s->>'kind'; seen:='{}'::jsonb;
  IF kind='retry_prefix' THEN
   through:=(payload->>'through_index')::numeric; last_id:=NULL; count_rows:=0;
   IF through<1 OR through>100000 OR payload->>'boundary' IS NULL OR payload->>'parent' IS NULL OR payload->>'node_id' IS NULL THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='invalid retry candidate'; END IF;
   FOR x IN SELECT e.k AS key, e.v AS value FROM jsonb_each(rec->'attempts') AS e(k,v) LOOP
    IF x.value->>'parent'=payload->>'parent' AND x.value->>'node_id'=payload->>'node_id' AND x.value->>'boundary'=payload->>'boundary' AND (x.value->>'index')::numeric<=through THEN
     IF x.value->>'state' IS DISTINCT FROM 'failed' OR x.value->>'limit' IS DISTINCT FROM payload->>'limit' OR seen ? (x.value->>'index') THEN RAISE EXCEPTION USING ERRCODE='PD005',MESSAGE='retry candidate facts changed'; END IF;
     seen:=seen||jsonb_build_object(x.value->>'index',true); count_rows:=count_rows+1; IF (x.value->>'index')::numeric=through THEN last_id:=x.key; END IF;
     expected:=expected||jsonb_build_array(jsonb_build_object('kind','attempt','key',x.key));
    END IF;
   END LOOP;
   IF count_rows<>through OR last_id IS DISTINCT FROM payload->>'last_pruned_id' THEN RAISE EXCEPTION USING ERRCODE='PD005',MESSAGE='retry candidate set changed'; END IF;
  ELSE
   through:=(payload->>'through_sequence')::numeric;
   IF through<1 OR through>100000 OR payload->>'processing_scope' IS NULL OR payload->>'processing_attempt' IS NULL OR payload->>'node_id' IS NULL OR jsonb_typeof(payload->'input') IS DISTINCT FROM 'object' THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='invalid message candidate'; END IF;
   SELECT e.v INTO stream FROM jsonb_each(rec->'streams') AS e(k,v) WHERE (e.v->'stream'->'id') - 'generation' = (payload->'input') - 'generation' AND (e.v->'stream'->'id'->>'generation')::numeric = (payload->'input'->>'generation')::numeric LIMIT 1;
   IF stream IS NULL OR (stream->>'acked_through')::numeric<through OR EXISTS(SELECT 1 FROM jsonb_array_elements(coalesce(nullif(stream->'pending','null'::jsonb),'[]'::jsonb)) q WHERE (q->>'sequence')::numeric<=through) THEN RAISE EXCEPTION USING ERRCODE='PD005',MESSAGE='message candidate frontier changed'; END IF;
   managed:=rec->'managed'->((payload->>'processing_scope')||'/'||(payload->>'processing_attempt')||'/'||(payload->>'node_id'));
   IF managed IS NULL THEN
    SELECT e.v INTO managed FROM jsonb_each(coalesce(rec->'managed','{}'::jsonb)) AS e(k,v) WHERE e.v->>'scope'=payload->>'processing_scope' AND e.v->>'parent'=payload->>'processing_attempt' AND e.v->>'node_id'=payload->>'node_id' LIMIT 1;
   END IF;
   IF managed IS NULL THEN RAISE EXCEPTION USING ERRCODE='PD005',MESSAGE='message managed witness missing'; END IF;
   FOR seq IN 1..through LOOP
    maxidx:=0; count_rows:=0; seen:='{}'::jsonb; latest_state:=NULL; group_limit:=NULL;
    FOR x IN SELECT e.k AS key, e.v AS value FROM jsonb_each(rec->'messages') AS e(k,v) LOOP
     IF x.value->>'scope'=payload->>'processing_scope' AND x.value->>'parent'=payload->>'processing_attempt' AND x.value->>'node_id'=payload->>'node_id' AND (x.value->'input') - 'generation' = (payload->'input') - 'generation' AND (x.value->>'sequence')::numeric=seq THEN
      -- 独立复验根包 messagePrefixForCompression 的语义不变量，不依赖记录已通过
      -- validateStreams：generation 必须落在 [1, 当前流代际]，组内 limit 一致。
      IF (x.value->'input'->>'generation')::numeric<1 OR (x.value->'input'->>'generation')::numeric>(stream->'stream'->'id'->>'generation')::numeric THEN RAISE EXCEPTION USING ERRCODE='PD005',MESSAGE='message candidate generation out of range'; END IF;
      IF group_limit IS NULL THEN group_limit:=(x.value->>'limit')::numeric; ELSIF group_limit<>(x.value->>'limit')::numeric THEN RAISE EXCEPTION USING ERRCODE='PD005',MESSAGE='message candidate limit changed'; END IF;
      idx:=(x.value->>'index')::numeric; IF seen ? idx::text OR idx<1 OR idx>(x.value->>'limit')::numeric THEN RAISE EXCEPTION USING ERRCODE='PD005',MESSAGE='message candidate budget changed'; END IF;
      seen:=seen||jsonb_build_object(idx::text,true); count_rows:=count_rows+1; IF idx>maxidx THEN maxidx:=idx; latest_state:=x.value->>'state'; END IF;
      IF idx<maxidx AND x.value->>'state' IS DISTINCT FROM 'failed' THEN RAISE EXCEPTION USING ERRCODE='PD005',MESSAGE='message candidate failure changed'; END IF;
      expected:=expected||jsonb_build_array(jsonb_build_object('kind','message','key',x.key));
     END IF;
    END LOOP;
    IF count_rows=0 OR count_rows<>maxidx THEN RAISE EXCEPTION USING ERRCODE='PD005',MESSAGE='message candidate sequence gap'; END IF;
    -- latest 必须已确认：与根包一致，不能把未结算（failed/active）的最新尝试当前缀删除。
    IF latest_state IS DISTINCT FROM 'confirmed' THEN RAISE EXCEPTION USING ERRCODE='PD005',MESSAGE='message candidate latest not confirmed'; END IF;
   END LOOP;
   actual_ids:=coalesce((SELECT jsonb_agg(e.k ORDER BY e.k) FROM jsonb_each(rec->'messages') AS e(k,v) WHERE e.v->>'scope'=payload->>'processing_scope' AND e.v->>'parent'=payload->>'processing_attempt' AND e.v->>'node_id'=payload->>'node_id' AND e.v->>'state'='confirmed'),'[]'::jsonb);
   IF payload->'confirmation_ids' IS DISTINCT FROM actual_ids THEN RAISE EXCEPTION USING ERRCODE='PD005',MESSAGE='message confirmations changed'; END IF;
  END IF;
 END LOOP;
 actual:=(SELECT count(*) FROM jsonb_array_elements(b->'ops') WHERE value->>'kind' IN ('attempt','message') AND value->'value'='null'::jsonb);
 IF actual<>jsonb_array_length(expected) OR EXISTS(SELECT 1 FROM jsonb_array_elements(expected) e WHERE NOT EXISTS(SELECT 1 FROM jsonb_array_elements(b->'ops') o WHERE o.value->>'kind'=e.value->>'kind' AND o.value->>'key'=e.value->>'key' AND o.value->'value'='null'::jsonb)) OR EXISTS(SELECT 1 FROM jsonb_array_elements(b->'ops') o WHERE o.value->>'kind' IN ('attempt','message') AND o.value->'value'='null'::jsonb AND NOT EXISTS(SELECT 1 FROM jsonb_array_elements(expected) e WHERE e.value->>'kind'=o.value->>'kind' AND e.value->>'key'=o.value->>'key')) THEN RAISE EXCEPTION USING ERRCODE='PD005',MESSAGE='semantic deletion set changed'; END IF;
 RETURN actual;
END $$;

-- semantic_canonical_json 按 Go encoding/json 的口径重新序列化一个 jsonb 值：
-- 对象键按**字节字典序**排列（PostgreSQL jsonb 自身按 (长度,字节) 排序，二者不同），
-- 字符串按 Go 的 HTML 转义规则输出，数字保持整数字面量。
-- 只用于重算 CommitIndex 绑定的 content digest；不做任何业务判定。
CREATE FUNCTION pulse_durable_distributed.semantic_canonical_json(v jsonb) RETURNS text
LANGUAGE plpgsql IMMUTABLE SET search_path=pg_catalog,pulse_durable_distributed,pg_temp AS $$
DECLARE typ text; parts text[]; item record; out text;
BEGIN
 typ:=jsonb_typeof(v);
 IF typ='object' THEN
  SELECT coalesce(array_agg(semantic_quote(e.k)||':'||semantic_canonical_json(e.v) ORDER BY convert_to(e.k,'UTF8')),'{}') INTO parts
  FROM jsonb_each(v) AS e(k,v);
  RETURN '{'||array_to_string(parts,',')||'}';
 ELSIF typ='array' THEN
  SELECT coalesce(array_agg(semantic_canonical_json(e.v) ORDER BY e.n),'{}') INTO parts
  FROM jsonb_array_elements(v) WITH ORDINALITY AS e(v,n);
  RETURN '['||array_to_string(parts,',')||']';
 ELSIF typ='string' THEN
  RETURN semantic_quote(v#>>'{}');
 ELSIF typ='null' THEN
  RETURN 'null';
 ELSE
  -- number / boolean：jsonb 的整数字面量与 Go 一致。
  RETURN v::text;
 END IF;
END $$;

-- semantic_content_digest 重算「去掉 commit_index 操作后的 canonical batch」摘要。
-- 顶层与 header/op 信封使用 Go 结构体字段顺序；嵌套 expected/value 使用字典序。
-- 这样 SQL 能独立验证 CommitIndex 的内容绑定，而不信任调用方给出的摘要。
CREATE FUNCTION pulse_durable_distributed.semantic_content_digest(b jsonb) RETURNS text
LANGUAGE plpgsql IMMUTABLE SET search_path=pg_catalog,pulse_durable_distributed,pg_temp AS $$
DECLARE h jsonb:=b->'header'; op jsonb; ops text[]:='{}'; hf text[]; fields text[]; k text; piece text;
BEGIN
 -- header：固定字段顺序，omitempty 字段仅在非空时出现。
 hf:=ARRAY[
  semantic_quote('run_id')||':'||semantic_quote(h->>'run_id'),
  semantic_quote('business_version')||':'||semantic_quote(h->>'business_version'),
  semantic_quote('definition_digest')||':'||semantic_quote(h->>'definition_digest'),
  semantic_quote('inputs_digest')||':'||semantic_quote(h->>'inputs_digest'),
  semantic_quote('root_scope')||':'||semantic_quote(h->>'root_scope'),
  semantic_quote('expected_revision')||':'||semantic_quote(h->>'expected_revision'),
  semantic_quote('revision')||':'||semantic_quote(h->>'revision')];
 FOREACH k IN ARRAY ARRAY['operation','compression_plan_digest','compression_max_objects','compression_max_bytes'] LOOP
  IF coalesce(h->>k,'')<>'' THEN hf:=array_append(hf,semantic_quote(k)||':'||semantic_quote(h->>k)); END IF;
 END LOOP;
 FOR op IN SELECT e.v FROM jsonb_array_elements(b->'ops') WITH ORDINALITY AS e(v,n)
           WHERE e.v->>'kind' IS DISTINCT FROM 'commit_index' ORDER BY e.n LOOP
  fields:=ARRAY[semantic_quote('kind')||':'||semantic_quote(op->>'kind'),
                semantic_quote('key')||':'||semantic_quote(coalesce(op->>'key',''))];
  FOREACH k IN ARRAY ARRAY['scope','name','sequence'] LOOP
   IF coalesce(op->>k,'')<>'' THEN fields:=array_append(fields,semantic_quote(k)||':'||semantic_quote(op->>k)); END IF;
  END LOOP;
  IF coalesce((op->>'state')::boolean,false) THEN fields:=array_append(fields,semantic_quote('state')||':true'); END IF;
  fields:=array_append(fields,semantic_quote('expected')||':'||semantic_canonical_json(op->'expected'));
  fields:=array_append(fields,semantic_quote('value')||':'||semantic_canonical_json(op->'value'));
  ops:=array_append(ops,'{'||array_to_string(fields,',')||'}');
 END LOOP;
 piece:='{'||semantic_quote('version')||':'||(b->>'version')||','||semantic_quote('header')||':{'||array_to_string(hf,',')||'},'||semantic_quote('ops')||':['||array_to_string(ops,',')||']}';
 RETURN encode(sha256(convert_to(piece,'UTF8')),'hex');
END $$;

CREATE FUNCTION pulse_durable_distributed.semantic_compress(a jsonb) RETURNS jsonb LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pulse_durable_distributed,pg_temp AS $$
DECLARE ns text:=a->>'namespace'; rid text:=a->>'run_id'; mid text:=a->>'mutation_id'; md text:=a->>'mutation_digest'; phase text:=a->>'phase'; r pulse_durable_distributed.runs; g pulse_durable_distributed.mutation_guards; guard_found boolean:=false; receipt jsonb; image jsonb; source jsonb; b jsonb; wire bytea; record_wire bytea; oldrev numeric; nextrev numeric; parent text; next_chain text; t timestamptz; source_digest text; batch_digest text; record_digest text; deleted bigint; source_bytes bigint; final_bytes bigint; budget jsonb; result jsonb; expected text; plan_digest text;
BEGIN
 IF jsonb_typeof(a->'snapshot_budget_version') IS DISTINCT FROM 'number' OR a->>'snapshot_budget_version' IS DISTINCT FROM '2' OR phase NOT IN ('prepare','commit') OR coalesce(octet_length(mid),0) NOT BETWEEN 1 AND 256 OR md !~ '^[0-9a-f]{64}$' OR (a->>'expected_revision') !~ '^[1-9][0-9]*$' OR (a->>'max_objects') !~ '^[1-9][0-9]*$' OR (a->>'max_bytes') !~ '^[1-9][0-9]*$' THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='invalid semantic compression request'; END IF;
 r:=storage_lock(a); t:=storage_clock(r,NULL); oldrev:=(a->>'expected_revision')::numeric;
 SELECT * INTO g FROM mutation_guards WHERE namespace=ns AND run_id=rid AND mutation_id=mid;
 guard_found:=FOUND;
 IF guard_found THEN
  IF g.digest IS DISTINCT FROM md OR g.operation IS DISTINCT FROM 'semantic_compress' THEN RAISE EXCEPTION USING ERRCODE='PD005',MESSAGE='semantic compression mutation conflict'; END IF;
  SELECT m.result INTO receipt FROM mutation_receipts m WHERE m.namespace=ns AND m.run_id=rid AND m.mutation_id=mid;
  IF receipt IS NOT NULL THEN RETURN jsonb_build_object('state','found','image',NULL,'source_image_digest',NULL,'receipt',receipt); END IF;
  RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='semantic compression receipt missing';
 ELSIF phase='commit' THEN
  NULL;
 END IF;
 IF r.revision<>oldrev OR r.head_chain IS DISTINCT FROM a->>'expected_head_chain' OR r.record IS NULL OR r.revision=0 THEN RAISE EXCEPTION USING ERRCODE='PD005',MESSAGE='semantic compression head conflict'; END IF;
 image:=storage_image(r,r.revision,true); source:=semantic_replay_image(r,image); PERFORM semantic_validate_record(source,r,r.revision);
 source_digest:=encode(sha256(convert_to(image::text,'UTF8')),'hex'); source_bytes:=snapshot_value_bytes(source,'snapshot',false);
 IF source_bytes>(a->>'max_bytes')::numeric OR source_bytes>16777216 THEN RAISE EXCEPTION USING ERRCODE='PD008',MESSAGE='semantic compression source budget'; END IF;
 IF phase='prepare' THEN
  RETURN jsonb_build_object('state','prepared','image',image,'source_image_digest',source_digest,'receipt',NULL);
 END IF;
 IF a->>'record_digest' IS NULL OR a->>'record_digest' !~ '^[0-9a-f]{64}$' THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='semantic compression record digest missing'; END IF;
 record_wire:=decode(a->>'record','base64'); IF record_wire IS NULL OR encode(sha256(record_wire),'hex') IS DISTINCT FROM a->>'record_digest' THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='semantic compression record digest'; END IF;
 IF a->>'source_image_digest' IS DISTINCT FROM source_digest OR a->>'batch_digest' !~ '^[0-9a-f]{64}$' OR a->>'plan_digest' !~ '^[0-9a-f]{64}$' THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='semantic compression proposal identity'; END IF;
 wire:=decode(a->>'batch','base64');
 IF wire IS NULL OR octet_length(wire) NOT BETWEEN 1 AND 4194304 OR encode(sha256(wire),'hex') IS DISTINCT FROM a->>'batch_digest' THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='semantic compression batch digest'; END IF;
 b:=convert_from(wire,'UTF8')::jsonb; IF b->'header'->>'operation' IS DISTINCT FROM 'history_compression' OR b->'header'->>'compression_plan_digest' IS DISTINCT FROM a->>'plan_digest' OR b->'header'->>'run_id' IS DISTINCT FROM rid OR b->'header'->>'expected_revision' IS DISTINCT FROM oldrev::text OR b->'header'->>'revision' IS DISTINCT FROM (oldrev+1)::text OR b->'header'->>'compression_max_objects' IS DISTINCT FROM a->>'max_objects' OR b->'header'->>'compression_max_bytes' IS DISTINCT FROM a->>'max_bytes' THEN RAISE EXCEPTION USING ERRCODE='PD005',MESSAGE='semantic compression batch header'; END IF;
 deleted:=semantic_validate_candidates(source,b); IF deleted=0 THEN RAISE EXCEPTION USING ERRCODE='PD005',MESSAGE='semantic compression has no deletions'; END IF;
 nextrev:=oldrev+1;
 -- 操作类别白名单：语义压缩只允许删除候选 Attempt/Message、推进可压缩摘要、
 -- 追加唯一一条 CommitIndex。maintain 权限不等于 owner 写权限，因此必须显式
 -- 拒绝任何其它业务修改操作（scope/node/stream/pending/managed/provenance 写入、
 -- 以及非删除的 attempt/message 写入），否则维护凭据可借压缩批次改写业务状态，
 -- 并让只按删除记账的大小账本与真实记录分叉。
 IF EXISTS(SELECT 1 FROM jsonb_array_elements(b->'ops') o WHERE
   (o.value->>'kind' IN ('attempt','message') AND o.value->'value' IS DISTINCT FROM 'null'::jsonb)
   OR (o.value->>'kind' NOT IN ('attempt','message','history_summary','commit_index'))) THEN
  RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='semantic compression operation kind not allowed';
 END IF;
 -- 摘要只能替换既有可压缩前缀，不能新增/删除，也不能触碰只读证据 marker。
 -- 并且必须保持 Kind 与稳定身份不变：只允许推进同一条摘要的应用位置。
 -- 否则维护凭据可以把 retry_prefix 换成 message_prefix（或覆盖 marker），
 -- 结构仍然合法但语义证据被篡改，发布后该 Run 将无法冷恢复。
 IF EXISTS(SELECT 1 FROM jsonb_array_elements(b->'ops') o WHERE o.value->>'kind'='history_summary'
   AND (o.value->'expected'='null'::jsonb OR o.value->'value'='null'::jsonb
        OR o.value->'value'->>'kind' NOT IN ('retry_prefix','message_prefix')
        OR o.value->'value'->>'kind' IS DISTINCT FROM o.value->'expected'->>'kind'
        OR o.value->'value'->>'kind' IS DISTINCT FROM o.value->>'scope'
        OR o.value->'value'->>'id' IS DISTINCT FROM o.value->>'name'
        OR o.value->'value'->>'id' IS DISTINCT FROM o.value->'expected'->>'id'
        OR o.value->'value'->>'source_revision' IS DISTINCT FROM o.value->'expected'->>'source_revision'
        OR o.value->'value'->>'source_digest' IS DISTINCT FROM o.value->'expected'->>'source_digest')) THEN
  RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='semantic summary operation not allowed';
 END IF;
 -- 内容真实性：新 payload 必须与旧 payload 逐字段相同，只把 applied_revision
 -- 推进到本次 applied revision。根包 withApplication 只做这一件事，因此这里
 -- 用「去掉 applied_revision 后完全相等 + applied_revision==nextrev」等价表达，
 -- 不需要在 SQL 里复制任何 Kind 专属语义。
 -- 这同时封死：applied_revision 指向错误 revision、保持未应用（value==expected）、
 -- 以及篡改 through_index/last_pruned_id 等与真实删除前缀不符的内容。
 IF EXISTS(SELECT 1 FROM jsonb_array_elements(b->'ops') o WHERE o.value->>'kind'='history_summary'
   AND (o.value->'value'->'payload' IS NULL OR o.value->'expected'->'payload' IS NULL
        OR convert_from(decode(semantic_wire_bytes(o.value->'value'->'payload'),'base64'),'UTF8')::jsonb->>'applied_revision' IS DISTINCT FROM nextrev::text
        OR (convert_from(decode(semantic_wire_bytes(o.value->'value'->'payload'),'base64'),'UTF8')::jsonb - 'applied_revision')
           IS DISTINCT FROM
           (convert_from(decode(semantic_wire_bytes(o.value->'expected'->'payload'),'base64'),'UTF8')::jsonb - 'applied_revision'))) THEN
  RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='semantic summary payload is not an application advance';
 END IF;
 -- 一一对应：每条可压缩摘要必须恰好被命中一次。只比总数会让两个 op 重复命中
 -- 同一条摘要、另一条在无应用见证的情况下被删除。
 IF EXISTS(SELECT 1 FROM jsonb_array_elements(b->'ops') o WHERE o.value->>'kind'='history_summary'
   GROUP BY o.value->>'scope', o.value->>'name' HAVING count(*)<>1) THEN
  RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='semantic summary target is not unique';
 END IF;
 -- 追加的 CommitIndex 必须严格接在当前 head 之后，且键与值一致。
 IF EXISTS(SELECT 1 FROM jsonb_array_elements(b->'ops') o WHERE o.value->>'kind'='commit_index'
   AND (o.value->>'key' IS DISTINCT FROM nextrev::text
        OR o.value->'expected' IS DISTINCT FROM 'null'::jsonb
        OR o.value->'value'->>'revision' IS DISTINCT FROM nextrev::text
        OR o.value->'value'->>'parent_revision' IS DISTINCT FROM oldrev::text
        OR o.value->'value'->>'batch_digest' IS DISTINCT FROM semantic_content_digest(b))) THEN
  RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='semantic commit index does not extend the head';
 END IF;
 -- 只读 marker 不在可压缩集合里，因此不能成为任何摘要操作的目标。
 IF EXISTS(SELECT 1 FROM jsonb_array_elements(b->'ops') o WHERE o.value->>'kind'='history_summary'
   AND NOT EXISTS(SELECT 1 FROM jsonb_array_elements(source->'record'->'history_summaries') s
     WHERE s.value->>'kind'=o.value->>'scope' AND s.value->>'id'=o.value->>'name'
       AND s.value->>'kind' IN ('retry_prefix','message_prefix'))) THEN
  RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='semantic summary target is not a compressible prefix';
 END IF;
 IF (SELECT count(*) FROM jsonb_array_elements(b->'ops') WHERE value->>'kind'='history_summary')<>(SELECT count(*) FROM jsonb_array_elements(source->'record'->'history_summaries') WHERE value->>'kind' IN ('retry_prefix','message_prefix')) OR (SELECT count(*) FROM jsonb_array_elements(b->'ops') WHERE value->>'kind'='commit_index')<>1 THEN RAISE EXCEPTION USING ERRCODE='PD005',MESSAGE='semantic evidence operation set'; END IF;
 source:=semantic_apply_wire_batch(source,b); nextrev:=oldrev+1; PERFORM semantic_validate_record(source,r,nextrev);
 -- 交叉核对受信服务生成的完整 record proposal：它必须与 SQL 独立重放的结果
 -- 逐字段相等。只比 batch digest 不足以发现服务端投影与后端重放的分叉。
 IF convert_from(record_wire,'UTF8')::jsonb IS DISTINCT FROM source THEN RAISE EXCEPTION USING ERRCODE='PD005',MESSAGE='semantic compression record proposal differs'; END IF;
 final_bytes:=snapshot_value_bytes(source,'snapshot',false); IF final_bytes>(a->>'max_bytes')::numeric OR final_bytes>16777216 THEN RAISE EXCEPTION USING ERRCODE='PD008',MESSAGE='semantic compression result budget'; END IF;
 budget:=jsonb_build_object('version',2,'header',jsonb_build_object('run_id',rid,'business_version',r.route->>'business_version','definition_digest',r.route->>'definition','inputs_digest',(convert_from(r.task,'UTF8')::jsonb)->>'inputs','root_scope',r.root_scope,'expected_revision',oldrev::text,'revision',nextrev::text),'ops',coalesce((SELECT jsonb_agg(jsonb_build_object('kind',value->>'kind','key',value->>'key','expected',value->'expected','value',NULL) ORDER BY value->>'kind',value->>'key') FROM jsonb_array_elements(b->'ops') WHERE value->>'kind' IN ('attempt','message') AND value->'value'='null'::jsonb),'[]'::jsonb));
 PERFORM snapshot_budget_apply(r,budget); UPDATE runs SET snapshot_bytes=final_bytes,snapshot_fixed_bytes=final_bytes-(SELECT coalesce(sum(snapshot_container_bytes(is_nil,entry_count,entry_bytes)) FILTER(WHERE kind IN ('scopes','attempts','streams','messages','managed')),0) FROM snapshot_containers WHERE namespace=ns AND run_id=rid),active_log_bytes=active_log_bytes+octet_length(wire),active_log_count=active_log_count+1,revision=nextrev,head_chain=encode(sha256(convert_to('pulse-durable/chain/batch/v1','UTF8')||decode('00','hex')||decode(r.head_chain,'hex')||sha256(wire)||decode(lpad(to_hex(trunc(nextrev/4294967296)::bigint),8,'0')||lpad(to_hex(mod(nextrev,4294967296)::bigint),8,'0'),'hex')),'hex'),updated_at=t,maintenance_watermark=t,last_mutation_id=mid,last_mutation_digest=md WHERE namespace=ns AND run_id=rid RETURNING head_chain INTO next_chain;
 parent:=r.head_chain; result:=jsonb_build_object('identity',jsonb_build_object('namespace',ns,'run_id',rid),'mutation',jsonb_build_object('id',mid,'digest',md),'expected_revision',oldrev::text,'applied_revision',nextrev::text,'expected_head_chain',parent,'applied_head_chain',next_chain,'deleted_objects',deleted::text,'source_bytes',source_bytes::text,'occurred_at',t,'receipt_digest',''); result:=result||jsonb_build_object('epoch',r.epoch,'revision',nextrev::text,'receipt_digest',semantic_digest('pulse-durable/distributed/semantic-compression/receipt/v1',ns,rid,mid,md,oldrev::text,nextrev::text,parent,next_chain,deleted::text,source_bytes::text,trunc(extract(epoch FROM t)*1000000)::numeric(30,0)::text));
 INSERT INTO mutation_guards(namespace,run_id,mutation_id,digest,operation,epoch,revision,occurred_at) VALUES(ns,rid,mid,md,'semantic_compress',r.epoch,nextrev,t);
 INSERT INTO mutation_receipts(namespace,run_id,mutation_id,occurred_at,result) VALUES(ns,rid,mid,t,result);
 INSERT INTO batches(namespace,run_id,revision,wire,digest,parent_chain,next_chain) VALUES(ns,rid,nextrev,wire,a->>'batch_digest',parent,next_chain); INSERT INTO events(namespace,run_id,operation,epoch,revision,mutation_id,mutation_digest,occurred_at) VALUES(ns,rid,'semantic_compress',r.epoch,nextrev,mid,md,t);
 RETURN jsonb_build_object('state','committed','image',NULL,'source_image_digest',NULL,'receipt',result);
END $$;
REVOKE ALL ON FUNCTION pulse_durable_distributed.semantic_compress(jsonb) FROM PUBLIC;
