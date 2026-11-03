package postgres

import (
	"strings"
	"testing"
)

func TestExistingLayoutValidationIncludesSafetyContract(t *testing.T) {
	if !strings.Contains(layoutMetaSQL, "layout_marker") || !strings.Contains(layoutMetaSQL, "signature") {
		t.Fatal("existing-layout validation lost marker or signature query")
	}
	for _, fragment := range []string{
		"p.proowner<>n.nspowner",
		"p.prosecdef",
		"p.proconfig",
		"c.relowner<>n.nspowner",
		"a.grantee=0",
	} {
		if !strings.Contains(layoutSafetySQL, fragment) {
			t.Fatalf("missing layout safety rule %q", fragment)
		}
	}
	for _, fragment := range []string{"p.proconfig", "c.relowner<>n.nspowner", "a.grantee=0"} {
		if !strings.Contains(checkSQL, fragment) {
			t.Fatalf("check_schema is missing shared safety rule %q", fragment)
		}
	}
	if strings.Contains(layoutSafetySQL, "c.relkind IN ('r','p','S')") || strings.Contains(checkSQL, "c.relkind IN ('r','p','S')") {
		t.Fatal("relation owner safety contract still narrows pg_class by relkind")
	}
	if !strings.Contains(layoutSafetyError.Error(), "unsafe schema owners, SECURITY DEFINER settings or ACL") {
		t.Fatal("missing unsafe-layout rejection error")
	}
}

func TestReplacePreservesFunction(t *testing.T) {
	s, e := replaceCreate(schemaSQL, "api")
	if e != nil || !strings.HasPrefix(s, "CREATE OR REPLACE FUNCTION") || strings.Contains(s, "DROP FUNCTION") || !strings.HasSuffix(s, "$$;") {
		t.Fatalf("unsafe function replacement: %v", e)
	}
	if _, e = replaceCreate("", "missing"); e == nil {
		t.Fatal("missing migration body accepted")
	}
}
