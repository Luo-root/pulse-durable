package postgres

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

//go:embed schema.sql
var schemaSQL string

//go:embed maintenance.sql
var maintenanceSQL string

// Grant 为已有数据库角色授权，范围严格限于一个命名空间。权限
// 包括 query、submit、worker、cancel、maintain、retain、replay。仅 maintain
// 授权物理合成；retain/replay 为预留权限，尚不授予任何 API。
// 普通工作进程的恢复查询还需授予 query。
// 调用方必须配置独立的非所有者、非超级用户登录角色。
type Grant struct {
	Role        string
	Namespace   string
	Permissions []string
}

// Migrate 安装新的物理模式，或精确验证已安装的模式。
// 拒绝旧模式，不修改其数据或 DDL。逻辑记录
// 仍为格式 2；物理模式版本 6 并非逻辑记录升级。
// 拒绝版本 1–5；此版本绝不重写已安装模式。
// 此显式管理员入口可授予 API 执行与命名空间
// 权限，但绝不授予表访问权限。须使用专用所有者凭据运行。
func Migrate(ctx context.Context, dsn string, grants ...Grant) error {
	if ctx == nil {
		return errors.New("distributed postgres: nil context")
	}
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	p, e := connect(ctx, dsn)
	if e != nil {
		return e
	}
	defer p.Close()
	tx, e := p.Begin(ctx)
	if e != nil {
		return scrub(e, false)
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = tx.Rollback(cleanup)
	}()
	if _, e = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(5788618723189269577); SET LOCAL search_path=pg_catalog,pulse_durable_distributed,pg_temp"); e != nil {
		return scrub(e, false)
	}
	var version int
	if e = tx.QueryRow(ctx, "SELECT current_setting('server_version_num')::integer").Scan(&version); e != nil {
		return fmt.Errorf("distributed postgres: version query failed: %w", scrub(e, false))
	}
	if version < 160000 {
		return errors.New("distributed postgres: PostgreSQL 16+ required")
	}
	var exists bool
	if e = tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname='pulse_durable_distributed')").Scan(&exists); e != nil {
		return scrub(e, false)
	}
	if !exists {
		if _, e = tx.Exec(ctx, schemaSQL+maintenanceSQL); e != nil {
			return scrub(e, false)
		}
		if _, e = tx.Exec(ctx, checkSQL); e != nil {
			return scrub(e, false)
		}
		sig, e := signature(ctx, tx)
		if e != nil {
			return e
		}
		if _, e = tx.Exec(ctx, "UPDATE pulse_durable_distributed.meta SET signature=$1", sig); e != nil {
			return scrub(e, false)
		}
	} else {
		var installed int
		if e = tx.QueryRow(ctx, "SELECT version FROM pulse_durable_distributed.meta").Scan(&installed); e != nil {
			return scrub(e, false)
		}
		if installed != 6 {
			return errors.New("distributed postgres: unsupported physical schema; use a new database (old data unchanged)")
		}
		if e = validate(ctx, tx, false); e != nil {
			return e
		}
	}
	if _, e = tx.Exec(ctx, `REVOKE ALL ON FUNCTION pulse_durable_distributed.api(text,jsonb),pulse_durable_distributed.maintenance(text,jsonb),pulse_durable_distributed.check_schema() FROM PUBLIC`); e != nil {
		return scrub(e, false)
	}
	for _, g := range grants {
		if !validText(g.Role) || !validText(g.Namespace) || len(g.Permissions) == 0 {
			return errors.New("distributed postgres: invalid grant")
		}
		var safe bool
		e = tx.QueryRow(ctx, `SELECT NOT rolsuper AND NOT rolcreaterole AND NOT rolcreatedb AND NOT rolbypassrls AND NOT pg_has_role($1,(SELECT nspowner FROM pg_namespace WHERE nspname='pulse_durable_distributed'),'MEMBER') FROM pg_roles WHERE rolname=$1`, g.Role).Scan(&safe)
		if e != nil || !safe {
			return errors.New("distributed postgres: grant requires isolated non-owner role")
		}
		role := pgx.Identifier{g.Role}.Sanitize()
		if _, e = tx.Exec(ctx, "GRANT USAGE ON SCHEMA pulse_durable_distributed TO "+role+"; GRANT EXECUTE ON FUNCTION pulse_durable_distributed.api(text,jsonb),pulse_durable_distributed.maintenance(text,jsonb),pulse_durable_distributed.check_schema() TO "+role); e != nil {
			return scrub(e, false)
		}
		for _, permission := range g.Permissions {
			switch permission {
			case "query", "worker", "submit", "cancel", "maintain", "retain", "replay":
			default:
				return errors.New("distributed postgres: invalid permission")
			}
			if _, e = tx.Exec(ctx, "INSERT INTO pulse_durable_distributed.access VALUES($1,$2,$3) ON CONFLICT DO NOTHING", g.Role, g.Namespace, permission); e != nil {
				return scrub(e, false)
			}
		}
	}
	// 授权改变 ACL，但不改变模式签名；签名有意排除
	// 角色特定的 ACL 内容。另行检查 PUBLIC 及工作进程有效权限。
	if e = tx.Commit(ctx); e != nil {
		return scrub(e, true)
	}
	return nil
}

type querier interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

// 签名覆盖精确的列、默认值、约束、索引、函数
// （包括 SECURITY DEFINER 及固定 search_path）、触发器、策略和
// 对象数量。ACL 单独检查，以允许显式角色授权。
const catalogSQL = `SELECT coalesce(string_agg(x,E'\n' ORDER BY x),'') FROM (
 SELECT 'relation:'||c.relname||':'||c.relkind::text||':'||c.relrowsecurity::text||':'||c.relforcerowsecurity::text AS x FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='pulse_durable_distributed'
 UNION ALL SELECT 'column:'||c.relname||':'||a.attname||':'||a.attnum||':'||format_type(a.atttypid,a.atttypmod)||':'||a.attnotnull||':'||a.attidentity::text||':'||a.attgenerated::text||':'||coalesce(pg_get_expr(ad.adbin,ad.adrelid),'') FROM pg_attribute a JOIN pg_class c ON c.oid=a.attrelid JOIN pg_namespace n ON n.oid=c.relnamespace LEFT JOIN pg_attrdef ad ON ad.adrelid=c.oid AND ad.adnum=a.attnum WHERE n.nspname='pulse_durable_distributed' AND a.attnum>0 AND NOT a.attisdropped
 UNION ALL SELECT 'constraint:'||c.relname||':'||conname||':'||pg_get_constraintdef(k.oid,true) FROM pg_constraint k JOIN pg_class c ON c.oid=k.conrelid WHERE connamespace='pulse_durable_distributed'::regnamespace
 UNION ALL SELECT 'index:'||indexdef FROM pg_indexes WHERE schemaname='pulse_durable_distributed'
 UNION ALL SELECT 'function:'||pg_get_functiondef(p.oid) FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace WHERE n.nspname='pulse_durable_distributed'
 UNION ALL SELECT 'trigger:'||pg_get_triggerdef(t.oid) FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='pulse_durable_distributed' AND NOT t.tgisinternal
 UNION ALL SELECT 'policy:'||policyname FROM pg_policies WHERE schemaname='pulse_durable_distributed'
) q`

func signature(ctx context.Context, q querier) (string, error) {
	var s string
	if e := q.QueryRow(ctx, catalogSQL).Scan(&s); e != nil {
		return "", scrub(e, false)
	}
	return digest([]byte(s)), nil
}
func validate(ctx context.Context, q querier, worker bool) error {
	var version int
	if e := q.QueryRow(ctx, "SELECT current_setting('server_version_num')::integer").Scan(&version); e != nil {
		return fmt.Errorf("distributed postgres: version query failed: %w", scrub(e, false))
	}
	if version < 160000 {
		return errors.New("distributed postgres: PostgreSQL 16+ required")
	}
	var n, v int
	var sig string
	if e := q.QueryRow(ctx, "SELECT count(*),coalesce(min(version),0),coalesce(min(signature),'') FROM pulse_durable_distributed.meta").Scan(&n, &v, &sig); e != nil {
		// 工作进程角色有意不能 SELECT meta。使用定义者身份的验证
		// 函数，避免授予哪怕附带的表读取权限。
		if worker {
			return validateWorker(ctx, q)
		}
		return scrub(e, false)
	}
	if n != 1 || v != 6 {
		return errors.New("distributed postgres: incompatible schema version")
	}
	current, e := signature(ctx, q)
	if e != nil {
		return e
	}
	if sig != current {
		return errors.New("distributed postgres: incompatible schema DDL")
	}
	if worker {
		return validateWorker(ctx, q)
	}
	return nil
}
func validateWorker(ctx context.Context, q querier) error {
	// 自洽的旧模式可能由其自身旧检查器返回 true。
	// 无须授予工作进程对 meta 的 SELECT 即可读取 pg_proc。先固定
	// 检查器函数体，再信任其对目录及权限的验证。
	var current bool
	body := strings.Split(checkSQL, "$check$")[1]
	if e := q.QueryRow(ctx, `SELECT prosrc=$1 FROM pg_proc WHERE oid='pulse_durable_distributed.check_schema()'::regprocedure`, body).Scan(&current); e != nil {
		return scrub(e, false)
	}
	if !current {
		return errors.New("distributed postgres: incompatible schema version")
	}
	var ok bool
	if e := q.QueryRow(ctx, "SELECT pulse_durable_distributed.check_schema()").Scan(&ok); e != nil {
		return scrub(e, false)
	}
	if !ok {
		return fmt.Errorf("distributed postgres: incompatible schema or unsafe worker privileges")
	}
	return nil
}
