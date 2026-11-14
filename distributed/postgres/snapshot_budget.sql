-- 大小账本只保存当前对象；不可变历史仍由 batches 保存。
CREATE TABLE pulse_durable_distributed.snapshot_objects(
 namespace text NOT NULL, run_id text NOT NULL, kind text NOT NULL CHECK(kind IN ('attempt','scope','node','stream','pending','message','managed','managed_entry')),
 key1 text NOT NULL, key2 text NOT NULL DEFAULT '', key3 text NOT NULL DEFAULT '', bytes bigint NOT NULL CHECK(bytes>=0),
 PRIMARY KEY(namespace,run_id,kind,key1,key2,key3), FOREIGN KEY(namespace,run_id) REFERENCES pulse_durable_distributed.runs(namespace,run_id)
);
CREATE TABLE pulse_durable_distributed.snapshot_containers(
 namespace text NOT NULL, run_id text NOT NULL, kind text NOT NULL CHECK(kind IN ('scopes','attempts','streams','messages','managed','nodes','pending','managed_state','managed_values')),
 key1 text NOT NULL DEFAULT '', is_nil boolean NOT NULL, entry_count bigint NOT NULL CHECK(entry_count>=0), entry_bytes bigint NOT NULL CHECK(entry_bytes>=0),
 PRIMARY KEY(namespace,run_id,kind,key1), FOREIGN KEY(namespace,run_id) REFERENCES pulse_durable_distributed.runs(namespace,run_id),
 CHECK(NOT is_nil OR (entry_count=0 AND entry_bytes=0)), CHECK(entry_count<>0 OR entry_bytes=0)
);
REVOKE ALL ON pulse_durable_distributed.snapshot_objects,pulse_durable_distributed.snapshot_containers FROM PUBLIC;

-- PostgreSQL 的 JSON 字符串转义与 Go 相同，另补 Go 默认的 HTML 和行分隔符转义。
CREATE FUNCTION pulse_durable_distributed.snapshot_string_bytes(v text) RETURNS bigint LANGUAGE sql IMMUTABLE STRICT SET search_path=pg_catalog,pulse_durable_distributed,pg_temp AS $$
 SELECT octet_length(to_json(v)::text)::bigint
  +5*((length(v)-length(replace(v,'<','')))+(length(v)-length(replace(v,'>','')))+(length(v)-length(replace(v,'&',''))))
  +3*((length(v)-length(replace(v,chr(8232),'')))+(length(v)-length(replace(v,chr(8233),''))))
$$;
CREATE FUNCTION pulse_durable_distributed.snapshot_container_bytes(is_nil boolean,n bigint,s bigint) RETURNS bigint LANGUAGE sql IMMUTABLE STRICT SET search_path=pg_catalog,pulse_durable_distributed,pg_temp AS $$
 SELECT CASE WHEN is_nil THEN 4 ELSE 2+s+greatest(n-1,0) END
$$;

-- 显式描述当前快照/线格式；字段必须齐全且大小写精确。新增字段必须先升级预算协议。
CREATE FUNCTION pulse_durable_distributed.snapshot_record_bytes(v jsonb,wire boolean) RETURNS bigint LANGUAGE plpgsql IMMUTABLE SET search_path=pg_catalog,pulse_durable_distributed,pg_temp AS $$
DECLARE required jsonb:='{"run_id":"text","revision":"uint","version":"text","definition":"text","inputs":"text","root_scope":"text","scopes":"scopemap?","attempts":"attemptmap?","streams":"streammap?","messages":"messagemap?","managed":"managedmap?"}'::jsonb; k text; spec text; source text; total bigint:=2; n bigint:=0;
BEGIN
 IF jsonb_typeof(v)<>'object' THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='invalid snapshot record'; END IF;
 IF EXISTS(SELECT 1 FROM jsonb_object_keys(v) x WHERE NOT (required ? x OR x IN ('history_summaries','commit_index','provenance'))) THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='unknown snapshot record field'; END IF;
 FOR k,spec IN SELECT key,value FROM jsonb_each_text(required) LOOP
  source:=k;
  IF NOT (v ? source) THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='missing snapshot record field'; END IF;
  total:=total+snapshot_string_bytes(k)+1+snapshot_value_bytes(v->source,spec,wire); n:=n+1;
 END LOOP;
 IF v ? 'history_summaries' THEN total:=total+snapshot_string_bytes('history_summaries')+1+snapshot_value_bytes(v->'history_summaries','summary[]',wire); n:=n+1; END IF;
 IF v ? 'commit_index' THEN total:=total+snapshot_string_bytes('commit_index')+1+snapshot_value_bytes(v->'commit_index','commit_entry[]',wire); n:=n+1; END IF;
 IF v ? 'provenance' THEN total:=total+snapshot_string_bytes('provenance')+1+snapshot_value_bytes(v->'provenance','provenance',wire); n:=n+1; END IF;
 RETURN total+greatest(n-1,0);
END $$;

CREATE FUNCTION pulse_durable_distributed.snapshot_value_bytes(v jsonb,shape text,wire boolean) RETURNS bigint LANGUAGE plpgsql IMMUTABLE SET search_path=pg_catalog,pulse_durable_distributed,pg_temp AS $$
DECLARE fields jsonb; source text; k text; spec text; x jsonb; n bigint; total bigint:=2; txt text; typ text:=jsonb_typeof(v); item record;
BEGIN
 IF v IS NULL THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='missing snapshot field'; END IF;
 IF shape='record' THEN RETURN snapshot_record_bytes(v,wire); END IF;
 IF shape='summary' THEN fields:='{"kind":"text","id":"text","source_revision":"uint","source_digest":"text","payload":"bytes","digest":"text"}'::jsonb;
 ELSIF shape='commit_entry' THEN fields:='{"revision":"uint","parent_revision":"uint","batch_digest":"text"}'::jsonb;
 ELSIF shape='provenance' THEN fields:='{"origin_run_id":"text","origin_revision":"uint","origin_record_digest":"text","compression_id":"text","digest":"text"}'::jsonb;
 END IF;
 IF right(shape,1)='?' THEN
  IF typ='null' THEN RETURN 4; END IF;
  RETURN snapshot_value_bytes(v,left(shape,length(shape)-1),wire);
 END IF;
 IF right(shape,2)='[]' THEN
  IF typ<>'array' THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='invalid snapshot array'; END IF;
  n:=jsonb_array_length(v); total:=2+greatest(n-1,0);
  FOR x IN SELECT value FROM jsonb_array_elements(v) LOOP total:=total+snapshot_value_bytes(x,left(shape,length(shape)-2),wire); END LOOP;
  RETURN total;
 END IF;
 IF right(shape,3)='map' THEN
  IF typ<>'object' THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='invalid snapshot map'; END IF;
  n:=0;
  FOR item IN SELECT key,value FROM jsonb_each(v) LOOP
   total:=total+snapshot_string_bytes(item.key)+1+snapshot_value_bytes(item.value,left(shape,length(shape)-3),wire); n:=n+1;
  END LOOP;
  RETURN total+greatest(n-1,0);
 END IF;
 IF shape='text' THEN
  IF typ<>'string' THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='invalid snapshot string'; END IF;
  RETURN snapshot_string_bytes(v#>>'{}');
 ELSIF shape='bool' THEN
  IF typ<>'boolean' THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='invalid snapshot boolean'; END IF;
  RETURN octet_length(v::text);
 ELSIF shape IN ('int','uint') THEN
  IF typ IS DISTINCT FROM (CASE WHEN shape='uint' AND wire THEN 'string' ELSE 'number' END) THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='invalid snapshot integer'; END IF;
  txt:=v#>>'{}';
  IF shape='uint' THEN
   IF txt !~ '^(0|[1-9][0-9]*)$' OR length(txt)>20 OR txt::numeric>18446744073709551615 THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='invalid snapshot uint64'; END IF;
  ELSIF txt !~ '^-?(0|[1-9][0-9]*)$' OR length(txt)>20 OR txt::numeric NOT BETWEEN -9223372036854775808 AND 9223372036854775807 THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='invalid snapshot int'; END IF;
  RETURN octet_length((txt::numeric)::text);
 ELSIF shape='bytes' THEN
  IF typ='null' THEN RETURN 4; END IF;
  IF wire THEN
   IF typ<>'array' THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='invalid wire bytes'; END IF;
   IF EXISTS(SELECT 1 FROM jsonb_array_elements(v) e WHERE jsonb_typeof(e)<>'number' OR e::text !~ '^(0|[1-9][0-9]{0,2})$' OR (e::text)::numeric>255) THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='invalid wire byte'; END IF;
   n:=jsonb_array_length(v);
  ELSE
   IF typ<>'string' THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='invalid snapshot bytes'; END IF;
   txt:=v#>>'{}'; n:=octet_length(decode(txt,'base64'));
   IF replace(encode(decode(txt,'base64'),'base64'),E'\n','')<>txt THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='noncanonical snapshot bytes'; END IF;
  END IF;
  RETURN 2+4*((n+2)/3);
 END IF;
 IF fields IS NULL THEN fields:=CASE shape
  WHEN 'attempt' THEN '{"parent":"text","node_id":"text","boundary":"text","index":"int","limit":"int","state":"text"}'::jsonb
  WHEN 'scope' THEN '{"graph":"text","path":"text","inputs":"text","nodes":"nodemap?"}'::jsonb
  WHEN 'node' THEN '{"node_id":"text","attempt":"text","reason":"text","outputs":"value[]?"}'::jsonb
  WHEN 'value' THEN '{"name":"text","type_id":"text","version":"text","state":"text","digest":"text","data":"bytes","stream":"descriptor?"}'::jsonb
  WHEN 'id' THEN '{"scope":"text","node_id":"text","key":"text","group":"text","generation":"uint"}'::jsonb
  WHEN 'descriptor' THEN '{"id":"id","graph":"text","path":"text","attempt":"text","type_id":"text","version":"text","source_id":"text","resumable":"bool","capacity":"int","terminal":"text","stopped":"bool"}'::jsonb
  WHEN 'pending' THEN '{"sequence":"uint","value":"value"}'::jsonb
  WHEN 'stream' THEN '{"stream":"descriptor","last_sequence":"uint","acked_through":"uint","pending":"pending[]?","cursor":"value?"}'::jsonb
  WHEN 'stream_header' THEN '{"stream":"descriptor","last_sequence":"uint","acked_through":"uint","cursor":"value?"}'::jsonb
  WHEN 'message' THEN '{"scope":"text","parent":"text","node_id":"text","input":"id","sequence":"uint","index":"int","limit":"int","state":"text"}'::jsonb
  WHEN 'managed' THEN '{"scope":"text","parent":"text","node_id":"text","state":"value[]?","values":"value[]?"}'::jsonb
  WHEN 'managed_header' THEN '{"scope":"text","parent":"text","node_id":"text"}'::jsonb
  WHEN 'snapshot' THEN '{"version":"int","record":"record"}'::jsonb
 END; END IF;
 IF fields IS NULL OR typ<>'object' OR (SELECT count(*) FROM jsonb_object_keys(v))<>(SELECT count(*) FROM jsonb_object_keys(fields)) THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='invalid snapshot object'; END IF;
 FOR k,spec IN SELECT key,value FROM jsonb_each_text(fields) LOOP
  source:=k;
  IF wire AND shape='stream_header' THEN source:=CASE k WHEN 'stream' THEN 'Stream' WHEN 'last_sequence' THEN 'LastSequence' WHEN 'acked_through' THEN 'AckedThrough' WHEN 'cursor' THEN 'Cursor' END;
  ELSIF wire AND shape='managed_header' THEN source:=CASE k WHEN 'scope' THEN 'Scope' WHEN 'parent' THEN 'Parent' WHEN 'node_id' THEN 'NodeID' END; END IF;
  total:=total+snapshot_string_bytes(k)+1+snapshot_value_bytes(v->source,spec,wire);
 END LOOP;
 RETURN total+(SELECT count(*)-1 FROM jsonb_object_keys(fields));
END $$;

-- 初始化只解析一次实际存储的快照。当前初始记录只允许一个空 root。
CREATE FUNCTION pulse_durable_distributed.snapshot_budget_initialize(ns text,rid text,j jsonb,actual_bytes bigint) RETURNS void LANGUAGE plpgsql SET search_path=pg_catalog,pulse_durable_distributed,pg_temp AS $$
DECLARE rec jsonb:=j->'record'; k text; root text:=rec->>'root_scope'; n bigint; total bigint; top_bytes bigint:=0; value jsonb;
BEGIN
 total:=snapshot_value_bytes(j,'snapshot',false);
 IF j->>'version' IS DISTINCT FROM '2' OR total IS DISTINCT FROM actual_bytes THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='noncanonical initial snapshot'; END IF;
 IF total>16777216 THEN RAISE EXCEPTION USING ERRCODE='PD008',MESSAGE='snapshot budget exhausted'; END IF;
 FOR k IN SELECT unnest(ARRAY['scopes','attempts','streams','messages','managed']) LOOP
  value:=rec->k; n:=CASE WHEN value='null'::jsonb THEN 0 ELSE (SELECT count(*) FROM jsonb_object_keys(value)) END;
  INSERT INTO snapshot_containers VALUES(ns,rid,k,'',value='null'::jsonb,n,CASE WHEN k='scopes' THEN snapshot_string_bytes(root)+1+snapshot_value_bytes(rec->'scopes'->root,'scope',false) ELSE 0 END);
  top_bytes:=top_bytes+snapshot_container_bytes(value='null'::jsonb,n,CASE WHEN k='scopes' THEN snapshot_string_bytes(root)+1+snapshot_value_bytes(rec->'scopes'->root,'scope',false) ELSE 0 END);
 END LOOP;
 INSERT INTO snapshot_objects VALUES(ns,rid,'scope',root,'','',snapshot_value_bytes(rec->'scopes'->root,'scope',false)-2);
 INSERT INTO snapshot_containers VALUES(ns,rid,'nodes',root,false,0,0);
 UPDATE runs SET snapshot_bytes=total,snapshot_fixed_bytes=total-top_bytes,snapshot_budget_version=2 WHERE namespace=ns AND run_id=rid;
END $$;

-- JSON 对象键采用数组编码，避免用户键中的分隔符产生复合键碰撞。
CREATE FUNCTION pulse_durable_distributed.snapshot_add_delta(ds jsonb,kind text,key1 text,dc bigint,db bigint) RETURNS jsonb LANGUAGE sql IMMUTABLE SET search_path=pg_catalog,pulse_durable_distributed,pg_temp AS $$
 SELECT jsonb_set(ds,ARRAY[jsonb_build_array(kind,key1)::text],jsonb_build_object('kind',kind,'key1',key1,'count',coalesce((ds->(jsonb_build_array(kind,key1)::text)->>'count')::bigint,0)+dc,'bytes',coalesce((ds->(jsonb_build_array(kind,key1)::text)->>'bytes')::bigint,0)+db))
$$;

-- snapshot_evidence_budget 按与快照编码相同的口径，累加语义证据组件的字节差额。
--
-- 证据组件（history_summaries / commit_index / provenance）不是可删除的业务
-- 对象，不进入 snapshot_objects；它们的字节属于 snapshot_fixed_bytes。
-- 这里只处理这三类操作，并精确计入：
--   1. 数组元素本身的字节；
--   2. 元素键（摘要用 kind/id 复合键，索引/来源无键）与逗号；
--   3. 字段从 nil 变为存在时新增的 "字段名": 开销，以及容器由 null(4) 变 [] (2)。
-- 调用方必须已持有 run 行锁；本函数不校验业务语义，只做字节记账。
CREATE FUNCTION pulse_durable_distributed.snapshot_evidence_budget(r pulse_durable_distributed.runs,j jsonb) RETURNS bigint LANGUAGE plpgsql SET search_path=pg_catalog,pulse_durable_distributed,pg_temp AS $$
DECLARE op jsonb; kind text; field text; shape text; e jsonb; v jsonb; delta bigint:=0; old_n bigint; new_n bigint; existing_n bigint; after_n bigint; had_field boolean; has_field boolean; before_bytes bigint; after_bytes bigint; entry_old bigint; entry_new bigint; key_bytes bigint; ops jsonb;
BEGIN
 ops:=coalesce(j->'ops','[]'::jsonb);
 IF ops='null'::jsonb THEN ops:='[]'::jsonb; END IF;
 IF NOT EXISTS(SELECT 1 FROM jsonb_array_elements(ops) x WHERE x.value->>'kind' IN ('history_summary','commit_index','provenance')) THEN RETURN 0; END IF;
 -- 当前记录必须已经存在（initialize 之后），否则证据字段状态无从判断。
 IF r.record IS NULL THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='evidence budget requires a checkpoint'; END IF;
 DECLARE rec jsonb:=convert_from(r.record,'UTF8')::jsonb->'record';
 BEGIN
  FOREACH field IN ARRAY ARRAY['history_summaries','commit_index','provenance'] LOOP
   kind:=CASE field WHEN 'history_summaries' THEN 'history_summary' WHEN 'commit_index' THEN 'commit_index' ELSE 'provenance' END;
   shape:=CASE field WHEN 'history_summaries' THEN 'summary' WHEN 'commit_index' THEN 'commit_entry' ELSE 'provenance' END;
   had_field:=(rec ? field) AND rec->field<>'null'::jsonb;
   -- 逐操作计算元素字节与键/逗号差额。
   entry_old:=0; entry_new:=0; old_n:=0; new_n:=0;
   FOR op IN SELECT x.value FROM jsonb_array_elements(ops) x WHERE x.value->>'kind'=kind LOOP
    e:=op->'expected'; v:=op->'value';
    IF field='provenance' THEN
     IF e<>'null'::jsonb THEN entry_old:=entry_old+snapshot_value_bytes(e,shape,true); old_n:=1; END IF;
     IF v<>'null'::jsonb THEN entry_new:=entry_new+snapshot_value_bytes(v,shape,true); new_n:=1; END IF;
    ELSE
     -- 数组元素：摘要键是 kind+id 两个字符串，索引键是 revision 字符串。
     IF e<>'null'::jsonb THEN
      key_bytes:=CASE field WHEN 'history_summaries' THEN snapshot_string_bytes(e->>'kind')+snapshot_string_bytes(e->>'id') ELSE 0 END;
      entry_old:=entry_old+snapshot_value_bytes(e,shape,true)+key_bytes; old_n:=old_n+1;
     END IF;
     IF v<>'null'::jsonb THEN
      key_bytes:=CASE field WHEN 'history_summaries' THEN snapshot_string_bytes(v->>'kind')+snapshot_string_bytes(v->>'id') ELSE 0 END;
      entry_new:=entry_new+snapshot_value_bytes(v,shape,true)+key_bytes; new_n:=new_n+1;
     END IF;
    END IF;
   END LOOP;
   IF old_n=0 AND new_n=0 THEN CONTINUE; END IF;
   -- 既有元素数决定逗号基准。runs.record 是基础快照、不随普通 apply 更新，
   -- 因此不能拿它的数组长度当当前 head 的元素数：
   --   * commit_index 的长度由不变量恒等于 revision，可精确取 r.revision；
   --   * history_summaries 的条数没有权威增量来源，本片只支持等量替换
   --     （压缩只推进 AppliedRevision），新增/删除摘要必须显式拒绝，
   --     等 distributed 物化入口落地后再按容器账本支持。
   existing_n:=0;
   IF field='commit_index' THEN
    existing_n:=r.revision;
   ELSIF field='provenance' THEN
    IF had_field THEN existing_n:=1; END IF;
   ELSE
    IF old_n<>new_n THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='summary count change requires a materialization entry'; END IF;
    IF had_field THEN existing_n:=jsonb_array_length(rec->field); END IF;
   END IF;
   after_n:=existing_n-old_n+new_n;
   has_field:=after_n>0;
   IF field='provenance' THEN
    before_bytes:=CASE WHEN had_field THEN entry_old ELSE 4 END;
    after_bytes:=CASE WHEN has_field THEN entry_new ELSE 4 END;
   ELSE
    -- 数组容器：null=4；非空=2+元素字节+(元素数-1) 个逗号。
    -- 未变元素的字节在两侧相同，相减后只剩本批差额与逗号差额。
    before_bytes:=CASE WHEN had_field THEN 2+entry_old+greatest(existing_n-1,0) ELSE 4 END;
    after_bytes:=CASE WHEN has_field THEN 2+entry_new+greatest(after_n-1,0) ELSE 4 END;
   END IF;
   -- 字段首次出现要计入 "字段名": 与一个分隔逗号。
   IF has_field AND NOT had_field THEN
    after_bytes:=after_bytes+snapshot_string_bytes(field)+1+1;
   ELSIF had_field AND NOT has_field THEN
    before_bytes:=before_bytes+snapshot_string_bytes(field)+1+1;
   END IF;
   delta:=delta+after_bytes-before_bytes;
  END LOOP;
 END;
 RETURN delta;
END $$;

CREATE FUNCTION pulse_durable_distributed.snapshot_budget_apply(r pulse_durable_distributed.runs,j jsonb) RETURNS bigint LANGUAGE plpgsql SET search_path=pg_catalog,pulse_durable_distributed,pg_temp AS $$
<<budget>>
DECLARE evidence_delta bigint; op jsonb; v jsonb; e jsonb; seen jsonb:='{}'; deltas jsonb:='{}'; delta jsonb; kind text; shape text; k1 text; k2 text; k3 text; ck text; cp text; state boolean; old_bytes bigint; new_bytes bigint; entry_delta bigint; dc bigint; initial_children bigint; n bigint; s bigint; was_nil boolean; now_nil boolean; before_bytes bigint; after_bytes bigint; candidate bigint:=r.snapshot_bytes+length((r.revision+1)::text)-length(r.revision::text); old_exists boolean; tuple text; c snapshot_containers;
BEGIN
 IF r.snapshot_budget_version<>2 OR r.snapshot_bytes<1 OR r.snapshot_bytes>16777216 THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='invalid snapshot ledger'; END IF;
 IF jsonb_typeof(j)<>'object' OR (SELECT count(*) FROM jsonb_object_keys(j))<>3 OR NOT(j ?& ARRAY['version','header','ops']) OR jsonb_typeof(j->'version') IS DISTINCT FROM 'number' OR j->>'version'<>'2' OR jsonb_typeof(j->'header') IS DISTINCT FROM 'object' OR (SELECT count(*) FROM jsonb_object_keys(j->'header')) NOT IN (7,11) OR NOT(j->'header' ?& ARRAY['run_id','business_version','definition_digest','inputs_digest','root_scope','expected_revision','revision']) OR EXISTS(SELECT 1 FROM jsonb_each(j->'header') x WHERE jsonb_typeof(x.value)<>'string') OR jsonb_typeof(j->'ops') NOT IN ('array','null') THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='invalid budget wire envelope'; END IF;
 -- 语义证据组件（摘要/索引/来源）不进入逐对象账本，它们的字节属于
 -- snapshot_fixed_bytes；这里单独按同一编码口径累加差额。
 evidence_delta:=snapshot_evidence_budget(r,j);
 candidate:=candidate+evidence_delta;
 IF j->'ops'='null'::jsonb THEN j:=jsonb_set(j,'{ops}','[]'::jsonb); END IF;
 IF jsonb_array_length(j->'ops')>4096 THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='too many budget operations'; END IF;
 -- 先建父对象，子操作与 wire 中的排序无关。
 FOR op IN SELECT value FROM jsonb_array_elements(j->'ops') ORDER BY CASE value->>'kind' WHEN 'scope' THEN 0 WHEN 'stream' THEN 0 WHEN 'managed' THEN 0 ELSE 1 END LOOP
  IF jsonb_typeof(op)<>'object' OR NOT(op ?& ARRAY['kind','key','expected','value']) OR EXISTS(SELECT 1 FROM jsonb_object_keys(op) k WHERE k NOT IN ('kind','key','scope','name','sequence','state','expected','value')) OR jsonb_typeof(op->'kind') IS DISTINCT FROM 'string' OR jsonb_typeof(op->'key') IS DISTINCT FROM 'string' OR (op ? 'scope' AND jsonb_typeof(op->'scope')<>'string') OR (op ? 'name' AND jsonb_typeof(op->'name')<>'string') OR (op ? 'sequence' AND jsonb_typeof(op->'sequence')<>'string') OR (op ? 'state' AND jsonb_typeof(op->'state')<>'boolean') THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='invalid budget operation'; END IF;
  kind:=op->>'kind'; k1:=op->>'key'; k2:=''; k3:=''; cp:=''; initial_children:=0; state:=coalesce((op->>'state')::boolean,false); v:=op->'value'; e:=op->'expected';
  -- 证据操作已由 snapshot_evidence_budget 记账，逐对象账本必须跳过它们，
  -- 否则会把摘要/索引误当成可删除的业务对象。
  IF kind IN ('history_summary','commit_index','provenance') THEN CONTINUE; END IF;
  IF kind IN ('node','pending','managed_entry') THEN
   IF k1<>'' OR coalesce(op->>'scope','')='' THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='invalid compound budget key'; END IF;
   k1:=op->>'scope'; cp:=k1;
  ELSIF k1='' OR coalesce(op->>'scope','')<>'' OR coalesce(op->>'name','')<>'' OR coalesce(op->>'sequence','')<>'' OR state THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='invalid simple budget key'; END IF;
  CASE kind
   WHEN 'attempt' THEN shape:='attempt'; ck:='attempts';
   WHEN 'scope' THEN shape:='scope'; ck:='scopes';
   WHEN 'node' THEN shape:='node'; ck:='nodes'; k2:=op->>'name';
    IF coalesce(k2,'')='' OR coalesce(op->>'sequence','')<>'' OR state THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='invalid node budget key'; END IF;
   WHEN 'stream' THEN shape:='stream_header'; ck:='streams';
   WHEN 'pending' THEN shape:='pending'; ck:='pending'; k2:=op->>'sequence';
    IF coalesce(k2,'') !~ '^(0|[1-9][0-9]*)$' OR length(k2)>20 OR k2::numeric>18446744073709551615 OR coalesce(op->>'name','')<>'' OR state THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='invalid pending budget key'; END IF;
   WHEN 'message' THEN shape:='message'; ck:='messages';
   WHEN 'managed' THEN shape:='managed_header'; ck:='managed';
   WHEN 'managed_entry' THEN shape:='value'; ck:=CASE WHEN state THEN 'managed_state' ELSE 'managed_values' END; k2:=op->>'name'; k3:=CASE WHEN state THEN 'state' ELSE 'values' END;
    IF coalesce(k2,'')='' OR coalesce(op->>'sequence','')<>'' OR NOT (seen ? jsonb_build_array('managed',k1,'','')::text) THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='invalid managed budget key'; END IF;
   ELSE RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='unknown budget operation';
  END CASE;
  tuple:=jsonb_build_array(kind,k1,k2,k3)::text;
  IF seen ? tuple THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='duplicate budget operation'; END IF;
  seen:=seen||jsonb_build_object(tuple,true);
  SELECT bytes INTO old_bytes FROM snapshot_objects WHERE namespace=r.namespace AND run_id=r.run_id AND snapshot_objects.kind=budget.kind AND key1=k1 AND key2=k2 AND key3=k3;
  old_exists:=FOUND;
  IF old_exists=(e='null'::jsonb) THEN RAISE EXCEPTION USING ERRCODE='PD005',MESSAGE='budget object precondition'; END IF;
  IF e<>'null'::jsonb THEN PERFORM snapshot_value_bytes(e,shape,true); END IF;
  IF kind='scope' AND (e<>'null'::jsonb OR v->'nodes' NOT IN ('null'::jsonb,'{}'::jsonb)) THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='invalid new scope'; END IF;
  IF kind NOT IN ('pending','managed_entry','attempt','message') AND v='null'::jsonb THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='invalid budget removal'; END IF;
  IF e='null'::jsonb AND v='null'::jsonb THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='absent budget removal'; END IF;
  new_bytes:=CASE WHEN v='null'::jsonb THEN 0 ELSE snapshot_value_bytes(v,shape,true) END;
  IF kind='scope' THEN
   new_bytes:=new_bytes-snapshot_value_bytes(v->'nodes','nodemap?',true); initial_children:=2;
   INSERT INTO snapshot_containers VALUES(r.namespace,r.run_id,'nodes',k1,false,0,0);
  ELSIF kind='stream' THEN
   new_bytes:=new_bytes+octet_length(',"pending":');
   IF NOT old_exists THEN initial_children:=4; INSERT INTO snapshot_containers VALUES(r.namespace,r.run_id,'pending',k1,true,0,0); END IF;
  ELSIF kind='managed' THEN
   new_bytes:=new_bytes+octet_length(',"state":,"values":');
   IF NOT old_exists THEN initial_children:=8; INSERT INTO snapshot_containers VALUES(r.namespace,r.run_id,'managed_state',k1,true,0,0),(r.namespace,r.run_id,'managed_values',k1,true,0,0); END IF;
  END IF;
  IF kind='pending' AND ((v<>'null'::jsonb AND v->>'sequence' IS DISTINCT FROM k2) OR (e<>'null'::jsonb AND e->>'sequence' IS DISTINCT FROM k2)) THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='pending budget sequence mismatch'; END IF;
  IF kind='managed_entry' AND ((v<>'null'::jsonb AND v->>'name' IS DISTINCT FROM k2) OR (e<>'null'::jsonb AND e->>'name' IS DISTINCT FROM k2)) THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='managed budget name mismatch'; END IF;
  dc:=CASE WHEN v='null'::jsonb THEN -1 WHEN NOT old_exists THEN 1 ELSE 0 END;
  entry_delta:=new_bytes-coalesce(old_bytes,0)+initial_children;
  IF ck IN ('scopes','attempts','streams','messages','managed','nodes') THEN entry_delta:=entry_delta+dc*(snapshot_string_bytes(CASE WHEN ck='nodes' THEN k2 ELSE k1 END)+1); END IF;
  deltas:=snapshot_add_delta(deltas,ck,cp,dc,entry_delta);
  IF v='null'::jsonb THEN DELETE FROM snapshot_objects WHERE namespace=r.namespace AND run_id=r.run_id AND snapshot_objects.kind=budget.kind AND key1=k1 AND key2=k2 AND key3=k3;
  ELSE INSERT INTO snapshot_objects VALUES(r.namespace,r.run_id,kind,k1,k2,k3,new_bytes) ON CONFLICT ON CONSTRAINT snapshot_objects_pkey DO UPDATE SET bytes=EXCLUDED.bytes; END IF;
 END LOOP;
 -- 每个子容器只计算一次逗号/nil 的变化，然后只传播一次父 delta。
 FOR delta IN SELECT value FROM jsonb_each(deltas) WHERE value->>'kind' IN ('nodes','pending','managed_state','managed_values') LOOP
  ck:=delta->>'kind'; cp:=delta->>'key1';
  SELECT * INTO c FROM snapshot_containers WHERE namespace=r.namespace AND run_id=r.run_id AND snapshot_containers.kind=ck AND key1=cp;
  IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='PD005',MESSAGE='missing budget parent'; END IF;
  n:=c.entry_count+(delta->>'count')::bigint; s:=c.entry_bytes+(delta->>'bytes')::bigint;
  IF n<0 OR s<0 OR (n=0 AND s<>0) THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='invalid budget container'; END IF;
  now_nil:=CASE WHEN ck IN ('managed_state','managed_values') THEN n=0 ELSE false END;
  before_bytes:=snapshot_container_bytes(c.is_nil,c.entry_count,c.entry_bytes); after_bytes:=snapshot_container_bytes(now_nil,n,s);
  UPDATE snapshot_containers SET is_nil=now_nil,entry_count=n,entry_bytes=s WHERE namespace=r.namespace AND run_id=r.run_id AND snapshot_containers.kind=ck AND key1=cp;
  deltas:=snapshot_add_delta(deltas,CASE ck WHEN 'nodes' THEN 'scopes' WHEN 'pending' THEN 'streams' ELSE 'managed' END,'',0,after_bytes-before_bytes);
 END LOOP;
 FOR delta IN SELECT value FROM jsonb_each(deltas) WHERE value->>'kind' IN ('scopes','attempts','streams','messages','managed') LOOP
  ck:=delta->>'kind';
  SELECT * INTO c FROM snapshot_containers WHERE namespace=r.namespace AND run_id=r.run_id AND snapshot_containers.kind=ck AND key1='';
  IF NOT FOUND THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='missing top budget container'; END IF;
  n:=c.entry_count+(delta->>'count')::bigint; s:=c.entry_bytes+(delta->>'bytes')::bigint;
  IF n<0 OR s<0 OR (n=0 AND s<>0) THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='invalid top budget container'; END IF;
  before_bytes:=snapshot_container_bytes(c.is_nil,c.entry_count,c.entry_bytes); after_bytes:=snapshot_container_bytes(false,n,s);
  candidate:=candidate+after_bytes-before_bytes;
  UPDATE snapshot_containers SET is_nil=false,entry_count=n,entry_bytes=s WHERE namespace=r.namespace AND run_id=r.run_id AND snapshot_containers.kind=ck AND key1='';
 END LOOP;
 IF candidate>16777216 THEN RAISE EXCEPTION USING ERRCODE='PD008',MESSAGE='snapshot budget exhausted'; END IF;
 IF candidate<1 THEN RAISE EXCEPTION USING ERRCODE='PD001',MESSAGE='invalid snapshot total'; END IF;
 UPDATE runs SET snapshot_bytes=candidate,snapshot_fixed_bytes=snapshot_fixed_bytes+length((r.revision+1)::text)-length(r.revision::text)+evidence_delta WHERE namespace=r.namespace AND run_id=r.run_id;
 RETURN candidate;
END $$;

-- 只在 Load 边界扫描当前账本，校验容器汇总及对象/父容器对应关系。
CREATE FUNCTION pulse_durable_distributed.snapshot_budget_summary(r pulse_durable_distributed.runs) RETURNS jsonb LANGUAGE sql STABLE SET search_path=pg_catalog,pulse_durable_distributed,pg_temp AS $$
 WITH entries AS (
  SELECT CASE o.kind WHEN 'attempt' THEN 'attempts' WHEN 'scope' THEN 'scopes' WHEN 'node' THEN 'nodes' WHEN 'stream' THEN 'streams' WHEN 'pending' THEN 'pending' WHEN 'message' THEN 'messages' WHEN 'managed' THEN 'managed' WHEN 'managed_entry' THEN CASE o.key3 WHEN 'state' THEN 'managed_state' ELSE 'managed_values' END END AS kind,
   CASE WHEN o.kind IN ('node','pending','managed_entry') THEN o.key1 ELSE '' END AS key1,
   o.bytes+CASE WHEN o.kind IN ('attempt','scope','node','stream','message','managed') THEN snapshot_string_bytes(CASE WHEN o.kind='node' THEN o.key2 ELSE o.key1 END)+1 ELSE 0 END
   +CASE WHEN o.kind IN ('scope','stream','managed') THEN coalesce((SELECT sum(snapshot_container_bytes(c.is_nil,c.entry_count,c.entry_bytes)) FROM snapshot_containers c WHERE c.namespace=r.namespace AND c.run_id=r.run_id AND c.key1=o.key1 AND ((o.kind='scope' AND c.kind='nodes') OR (o.kind='stream' AND c.kind='pending') OR (o.kind='managed' AND c.kind IN ('managed_state','managed_values')))),0) ELSE 0 END AS bytes
  FROM snapshot_objects o WHERE o.namespace=r.namespace AND o.run_id=r.run_id
 ), sums AS (SELECT kind,key1,count(*) n,sum(bytes) s FROM entries GROUP BY kind,key1), containers AS (SELECT * FROM snapshot_containers WHERE namespace=r.namespace AND run_id=r.run_id), totals AS (
  SELECT r.snapshot_fixed_bytes+coalesce(sum(snapshot_container_bytes(is_nil,entry_count,entry_bytes)) FILTER(WHERE kind IN ('scopes','attempts','streams','messages','managed')),0) AS bytes FROM containers
 )
 SELECT jsonb_build_object('snapshot_ledger_bytes',totals.bytes::text,'snapshot_ledger_valid',
  (SELECT count(*)=5 FROM containers WHERE kind IN ('scopes','attempts','streams','messages','managed') AND key1='')
  AND NOT EXISTS(SELECT 1 FROM containers c FULL JOIN sums s USING(kind,key1) WHERE c.kind IS NULL OR coalesce(s.n,0) IS DISTINCT FROM c.entry_count OR coalesce(s.s,0) IS DISTINCT FROM c.entry_bytes)
  AND (SELECT count(*) FROM containers WHERE kind IN ('nodes','pending','managed_state','managed_values'))=(SELECT coalesce(sum(CASE kind WHEN 'scope' THEN 1 WHEN 'stream' THEN 1 WHEN 'managed' THEN 2 ELSE 0 END),0) FROM snapshot_objects WHERE namespace=r.namespace AND run_id=r.run_id)
  AND NOT EXISTS(SELECT 1 FROM snapshot_objects o WHERE namespace=r.namespace AND run_id=r.run_id AND kind IN ('scope','stream','managed') AND NOT EXISTS(SELECT 1 FROM containers c WHERE c.key1=o.key1 AND c.kind=CASE o.kind WHEN 'scope' THEN 'nodes' WHEN 'stream' THEN 'pending' ELSE 'managed_state' END))
  AND totals.bytes=r.snapshot_bytes,
  'snapshot_ledger',jsonb_build_object('fixed_bytes',r.snapshot_fixed_bytes::text,
   'objects',coalesce((SELECT jsonb_agg(jsonb_build_array(o.kind,o.key1,o.key2,o.key3,o.bytes::text) ORDER BY o.kind COLLATE "C",o.key1 COLLATE "C",o.key2 COLLATE "C",o.key3 COLLATE "C") FROM snapshot_objects o WHERE o.namespace=r.namespace AND o.run_id=r.run_id),'[]'::jsonb),
   'containers',coalesce((SELECT jsonb_agg(jsonb_build_array(c.kind,c.key1,c.is_nil,c.entry_count::text,c.entry_bytes::text) ORDER BY c.kind COLLATE "C",c.key1 COLLATE "C") FROM containers c),'[]'::jsonb)))
 FROM totals
$$;
REVOKE ALL ON ALL FUNCTIONS IN SCHEMA pulse_durable_distributed FROM PUBLIC;
