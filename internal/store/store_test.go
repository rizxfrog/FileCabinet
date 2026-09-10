package store

import (
	"context"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"testing"
)

func TestClaimOrReclaim(t *testing.T) {
	m := miniredis.RunT(t)
	r := redis.NewClient(&redis.Options{Addr: m.Addr()})
	defer r.Close()
	ctx := context.Background()

	// First claim succeeds.
	one := New(r, "a:")
	if e := one.ClaimOrReclaim(ctx); e != nil {
		t.Fatal(e)
	}

	// Second instance with same prefix fails because pending is alive.
	// Simulate a pending mutation.
	if e := one.Mark(ctx, "hash", "commit"); e != nil {
		t.Fatal(e)
	}
	two := New(r, "a:")
	if e := two.ClaimOrReclaim(ctx); e == nil {
		t.Fatal("expected ErrOwner with pending mutations")
	}

	// Clear pending by saving (which HDELs pending).
	if e := one.Save(ctx, File{Hash: "hash", Size: 1, ExpiresAt: 0}); e != nil {
		t.Fatal(e)
	}

	// Now second instance should be able to reclaim.
	three := New(r, "a:")
	if e := three.ClaimOrReclaim(ctx); e != nil {
		t.Fatal(e)
	}
	// Verify three is now the owner.
	if e := three.Check(ctx); e != nil {
		t.Fatal(e)
	}
	// The old instance (one) should now fail Check.
	if e := one.Check(ctx); e == nil {
		t.Fatal("old instance should no longer be owner after reclaim")
	}
}

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
