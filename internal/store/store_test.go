package store

import (
	"context"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"testing"
)

func TestDurableOwner(t *testing.T) {
	m := miniredis.RunT(t)
	r := redis.NewClient(&redis.Options{Addr: m.Addr()})
	defer r.Close()
	ctx := context.Background()
	one := New(r, "a:")
	two := New(r, "a:")
	if e := one.Claim(ctx); e != nil {
		t.Fatal(e)
	}
	if e := two.Claim(ctx); e == nil {
		t.Fatal("second instance acquired owner")
	}
	if e := two.Release(ctx); e == nil {
		t.Fatal("nonowner released")
	}
	if e := one.Check(ctx); e != nil {
		t.Fatal(e)
	}
	if e := one.Mark(ctx, "hash", "commit"); e != nil {
		t.Fatal(e)
	}
	if e := one.Release(ctx); e == nil {
		t.Fatal("released with pending mutation")
	}
	if e := one.Remove(ctx, "hash"); e != nil {
		t.Fatal(e)
	}
	if e := one.Release(ctx); e != nil {
		t.Fatal(e)
	}
	if e := two.Claim(ctx); e != nil {
		t.Fatal(e)
	}
}
