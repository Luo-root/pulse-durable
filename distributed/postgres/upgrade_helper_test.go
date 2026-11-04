package postgres

import (
	"errors"
	"strings"
)

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
