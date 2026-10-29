CREATE FUNCTION pulse_durable_distributed.check_schema() RETURNS boolean
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pulse_durable_distributed,pg_temp AS $check$
DECLARE actual text; expected text; n integer; v integer;
BEGIN
 SELECT count(*),min(version),min(signature) INTO n,v,expected FROM pulse_durable_distributed.meta;
 IF n<>1 OR v<>4 THEN RETURN false; END IF;
 SELECT encode(sha256(convert_to(s,'UTF8')),'hex') INTO actual FROM (SELECT coalesce(string_agg(x,E'\n' ORDER BY x),'') FROM (
 SELECT 'relation:'||c.relname||':'||c.relkind::text||':'||c.relrowsecurity::text||':'||c.relforcerowsecurity::text AS x FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='pulse_durable_distributed'
 UNION ALL SELECT 'column:'||c.relname||':'||a.attname||':'||a.attnum||':'||format_type(a.atttypid,a.atttypmod)||':'||a.attnotnull||':'||a.attidentity::text||':'||a.attgenerated::text||':'||coalesce(pg_get_expr(ad.adbin,ad.adrelid),'') FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid JOIN pg_namespace n ON n.oid=c.relnamespace LEFT JOIN pg_attrdef ad ON ad.adrelid=c.oid AND ad.adnum=a.attnum WHERE n.nspname='pulse_durable_distributed' AND a.attnum>0 AND NOT a.attisdropped
 UNION ALL SELECT 'constraint:'||c.relname||':'||conname||':'||pg_get_constraintdef(k.oid,true) FROM pg_constraint k JOIN pg_class c ON c.oid=k.conrelid WHERE connamespace='pulse_durable_distributed'::regnamespace
 UNION ALL SELECT 'index:'||indexdef FROM pg_indexes WHERE schemaname='pulse_durable_distributed'
 UNION ALL SELECT 'function:'||pg_get_functiondef(p.oid) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='pulse_durable_distributed'
 UNION ALL SELECT 'trigger:'||pg_get_triggerdef(t.oid) FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='pulse_durable_distributed' AND NOT t.tgisinternal
 UNION ALL SELECT 'policy:'||policyname FROM pg_policies WHERE schemaname='pulse_durable_distributed'
) q) AS catalog(s);
 IF actual IS DISTINCT FROM expected THEN RETURN false; END IF;
 IF pg_is_in_recovery() OR current_setting('transaction_isolation')<>'read committed' THEN RETURN false; END IF;
 IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname=session_user AND (rolsuper OR rolcreaterole OR rolcreatedb OR rolbypassrls))
  OR pg_has_role(session_user,(SELECT nspowner FROM pg_namespace WHERE nspname='pulse_durable_distributed'),'MEMBER')
  OR has_schema_privilege(session_user,'pulse_durable_distributed','CREATE') THEN RETURN false; END IF;
 IF EXISTS(SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='pulse_durable_distributed' AND (pg_has_role(session_user,p.proowner,'MEMBER') OR p.proowner<>n.nspowner)) THEN RETURN false; END IF;
 IF EXISTS(SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
  WHERE n.nspname='pulse_durable_distributed' AND c.relkind IN ('r','p','S') AND
   (pg_has_role(session_user,c.relowner,'MEMBER') OR
    (c.relkind<>'S' AND (has_table_privilege(session_user,c.oid,'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER'))) OR
    (c.relkind='S' AND has_sequence_privilege(session_user,c.oid,'USAGE,SELECT,UPDATE')))) THEN RETURN false; END IF;
 IF EXISTS(SELECT 1 FROM pg_namespace n CROSS JOIN LATERAL aclexplode(coalesce(n.nspacl,acldefault('n',n.nspowner))) a WHERE n.nspname='pulse_durable_distributed' AND a.grantee=0)
 OR EXISTS(SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace CROSS JOIN LATERAL aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) a WHERE n.nspname='pulse_durable_distributed' AND a.grantee=0)
 OR EXISTS(SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace CROSS JOIN LATERAL aclexplode(coalesce(c.relacl,acldefault(CASE WHEN c.relkind='S' THEN 's'::"char" ELSE 'r'::"char" END,c.relowner))) a WHERE n.nspname='pulse_durable_distributed' AND a.grantee=0) THEN RETURN false; END IF;
 RETURN true;
END $check$;
REVOKE ALL ON FUNCTION pulse_durable_distributed.check_schema() FROM PUBLIC;
