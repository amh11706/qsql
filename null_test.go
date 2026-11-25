package qsql

import (
	"database/sql/driver"
	"testing"
)

func TestNullIfZero(t *testing.T) {
	for _, c := range []struct {
		name string
		got  driver.Valuer
		want driver.Value
	}{
		{"zero int", NullIfZero(int64(0)), nil},
		{"int", NullIfZero(int64(42)), int64(42)},
		{"empty string", NullIfZero(""), nil},
		{"string", NullIfZero("bob"), "bob"},
	} {
		v, err := c.got.Value()
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		if v != c.want {
			t.Errorf("%s: got %#v, want %#v", c.name, v, c.want)
		}
	}
}
