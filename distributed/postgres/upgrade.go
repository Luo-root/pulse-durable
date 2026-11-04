package postgres

import (
	"context"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"strings"
)

// 此版本有意只读旧物理模式。不尝试 DDL 或
// 记录转换，即使旧目录签名正确。
func upgradeV1(context.Context, pgx.Tx) error {
	return errors.New("distributed postgres: unsupported physical schema; use a new database (old data unchanged)")
}

func replaceCreate(sql, name string) (string, error) {
	head := "CREATE FUNCTION pulse_durable_distributed." + name + "("
	i := strings.Index(sql, head)
	if i < 0 {
		return "", errors.New("distributed postgres: missing migration function")
	}
	end := strings.Index(sql[i:], "$$;")
	if end < 0 {
		return "", errors.New("distributed postgres: incomplete migration function")
	}
	return strings.Replace(sql[i:i+end+3], "CREATE FUNCTION", "CREATE OR REPLACE FUNCTION", 1), nil
}
func migrationError(stage string, err error) error {
	return fmt.Errorf("distributed postgres: migration %s: %w", stage, scrub(err, false))
}
