package postgres

// check_schema runs as the migration owner to inspect objects without exposing
// table contents. session_user always names the authenticated login, including
// when this function is called through SECURITY DEFINER api.
var checkSQL = `CREATE FUNCTION pulse_durable_distributed.check_schema() RETURNS boolean
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pulse_durable_distributed,pg_temp AS $check$
DECLARE actual text; expected text; n integer; v integer;
BEGIN
 SELECT count(*),min(version),min(signature) INTO n,v,expected FROM pulse_durable_distributed.meta;
 IF n<>1 OR v<>2 THEN RETURN false; END IF;
 SELECT encode(sha256(convert_to(s,'UTF8')),'hex') INTO actual FROM (` + catalogSQL + `) AS catalog(s);
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
REVOKE ALL ON FUNCTION pulse_durable_distributed.check_schema() FROM PUBLIC;`
