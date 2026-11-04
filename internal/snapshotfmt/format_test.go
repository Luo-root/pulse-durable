package snapshotfmt

import (
	"encoding/json"
	"math"
	"reflect"
	"strings"
	"testing"
)

func TestQuotedSizeMatchesJSON(t *testing.T) {
	for _, s := range []string{"", "simple", "\"\\\b\f\n\r\t\x00\x1f<>&\u2028\u2029中文😀", "\ufffd"} {
		encoded, err := json.Marshal(s)
		if err != nil {
			t.Fatal(err)
		}
		got, err := QuotedSize(s)
		if err != nil || got != int64(len(encoded)) {
			t.Fatalf("quoted size=%d want=%d err=%v", got, len(encoded), err)
		}
	}
	if _, err := QuotedSize(string([]byte{0xff})); err == nil {
		t.Fatal("invalid UTF-8 accepted")
	}
}

func TestSizeMatchesAllFieldEncoding(t *testing.T) {
	type sample struct {
		Name     string   `json:"name"`
		Empty    []byte   `json:"empty,omitempty"`
		Values   []string `json:"values"`
		Flag     bool     `json:"flag"`
		Number   int      `json:"number"`
		Revision uint64   `json:"revision"`
	}
	for _, data := range [][]byte{nil, {}, {0}, {0, 255}, {0, 1, 255}, {0, 1, 2, 255}} {
		x := sample{Name: "<&>中文", Empty: data, Values: []string{}, Number: -1, Revision: math.MaxUint64}
		b, err := Encode(x)
		if err != nil {
			t.Fatal(err)
		}
		got, err := RecordSize(x)
		if err != nil || got != int64(len(b)) {
			t.Fatalf("size=%d want=%d err=%v", got, len(b), err)
		}
		if !strings.Contains(string(b), `"empty":`) {
			t.Fatal("omitempty removed a required snapshot field")
		}
	}
	// 冻结信封、字段排序与 all-field 表示，不能只比较共用遍历的两条路径。
	b, err := Encode(struct {
		N int    `json:"n"`
		B []byte `json:"b,omitempty"`
	}{})
	if err != nil || string(b) != `{"record":{"b":null,"n":0},"version":1}` {
		t.Fatalf("wire changed: %s %v", b, err)
	}
}

func TestUnsupportedShapesAndOverflow(t *testing.T) {
	// 动态构造冲突字段，保留非法 schema 负控而不把静态 vet 错误带入测试包。
	duplicateFields := reflect.StructOf([]reflect.StructField{
		{Name: "A", Type: reflect.TypeFor[string](), Tag: `json:"same"`},
		{Name: "B", Type: reflect.TypeFor[string](), Tag: `json:"same"`},
	})
	duplicate := reflect.New(duplicateFields).Elem().Interface()
	for _, x := range []any{make(chan int), map[int]int{}, struct{ F float64 }{}, duplicate, struct{ private string }{}} {
		if _, err := Size(x); err == nil {
			t.Fatalf("unsupported type accepted: %T", x)
		}
		if _, err := Encode(x); err == nil {
			t.Fatalf("unsupported encoder type accepted: %T", x)
		}
	}
	if _, err := Add(math.MaxInt64, 1); err == nil {
		t.Fatal("positive overflow")
	}
	if _, err := Add(math.MinInt64, -1); err == nil {
		t.Fatal("negative overflow")
	}
	for _, c := range []struct {
		nil bool
		n   int
	}{{true, 1}, {false, -1}} {
		if _, err := ContainerOverhead(c.nil, c.n); err == nil {
			t.Fatal("invalid container")
		}
	}
	if FieldName(reflect.TypeFor[struct {
		X string `json:"x,omitempty"`
	}]().Field(0)) != "x" {
		t.Fatal("field name")
	}
}
