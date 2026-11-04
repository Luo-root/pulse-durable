package postgres

// check_schema 以迁移所有者身份检查对象，而不暴露
// 表内容。session_user 始终指向已认证的登录身份，
// 即使此函数经 SECURITY DEFINER api 调用也如此。
var checkSQL = `CREATE FUNCTION pulse_durable_distributed.check_schema() RETURNS boolean
LANGUAGE plpgsql SECURITY DEFINER SET search_path=pg_catalog,pulse_durable_distributed,pg_temp AS $check$
DECLARE actual text; expected text; n integer; marker text;
BEGIN
 SELECT count(*),min(layout_marker),min(signature) INTO n,marker,expected FROM pulse_durable_distributed.meta;
 IF n<>1 OR marker<>'pulse-durable-distributed-layout-v1' THEN RETURN false; END IF;
 SELECT encode(sha256(convert_to(s,'UTF8')),'hex') INTO actual FROM (` + catalogSQL + `) AS catalog(s);
 IF actual IS DISTINCT FROM expected THEN RETURN false; END IF;
 -- 当前开发布局必须具备历史 pin 表与五个独立入口，不接受旧布局自签名。
 IF to_regclass('pulse_durable_distributed.history_pins') IS NULL OR to_regclass('pulse_durable_distributed.pin_guards') IS NULL OR to_regclass('pulse_durable_distributed.pin_receipts') IS NULL OR
 (SELECT count(*) FROM pg_proc WHERE pronamespace='pulse_durable_distributed'::regnamespace AND proname IN ('pin_create','pin_renew','pin_release','pin_find','pin_find_mutation') AND proargtypes='3802'::oidvector AND prosecdef)<>5 THEN RETURN false; END IF;
 IF EXISTS(SELECT 1 FROM pg_proc WHERE pronamespace='pulse_durable_distributed'::regnamespace AND proname LIKE 'pin_%' AND proname NOT IN ('pin_create','pin_renew','pin_release','pin_find','pin_find_mutation') AND has_function_privilege(session_user,oid,'EXECUTE')) THEN RETURN false; END IF;
 IF NOT (` + layoutSafetySQL + `) THEN RETURN false; END IF;
 IF pg_is_in_recovery() OR current_setting('transaction_isolation')<>'read committed' THEN RETURN false; END IF;
 IF EXISTS(SELECT 1 FROM pg_roles WHERE rolname=session_user AND (rolsuper OR rolcreaterole OR rolcreatedb OR rolbypassrls))
  OR pg_has_role(session_user,(SELECT nspowner FROM pg_namespace WHERE nspname='pulse_durable_distributed'),'MEMBER')
  OR has_schema_privilege(session_user,'pulse_durable_distributed','CREATE') THEN RETURN false; END IF;
 IF EXISTS(SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
  WHERE n.nspname='pulse_durable_distributed' AND
   (pg_has_role(session_user,c.relowner,'MEMBER') OR
    (c.relkind='S' AND has_sequence_privilege(session_user,c.oid,'USAGE,SELECT,UPDATE')) OR
    (c.relkind<>'S' AND has_table_privilege(session_user,c.oid,'SELECT,INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER')))) THEN RETURN false; END IF;
 RETURN true;
END $check$;
REVOKE ALL ON FUNCTION pulse_durable_distributed.check_schema() FROM PUBLIC;`
