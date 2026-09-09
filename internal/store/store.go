package store

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

var ErrMissing = errors.New("文件不存在或已过期")
var ErrOwner = errors.New("storage ownership unavailable; another instance or unreconciled shutdown holds the durable owner claim (see README recovery)")

type File struct {
	Hash      string `json:"hash"`
	Name      string `json:"name"`
	Size      int64  `json:"size"`
	ExpiresAt int64  `json:"expires_at"`
	CreatedAt int64  `json:"created_at"`
}
type Code struct {
	Code      string `json:"code"`
	Hash      string `json:"hash"`
	ExpiresAt int64  `json:"expires_at"`
}
type Store struct {
	R             *redis.Client
	Prefix, Owner string
}

func New(r *redis.Client, prefix string) *Store { return &Store{R: r, Prefix: prefix, Owner: Random()} }
func Random() string {
	b := make([]byte, 24)
	if _, e := rand.Read(b); e != nil {
		panic(e)
	}
	return hex.EncodeToString(b)
}
func (s *Store) Claim(ctx context.Context) error {
	ok, e := s.R.SetNX(ctx, s.Prefix+"owner", s.Owner, 0).Result()
	if e != nil {
		return e
	}
	if !ok {
		return ErrOwner
	}
	return nil
}
func (s *Store) Check(ctx context.Context) error {
	v, e := s.R.Get(ctx, s.Prefix+"owner").Result()
	if e != nil {
		return e
	}
	if v != s.Owner {
		return ErrOwner
	}
	return nil
}
func (s *Store) Release(ctx context.Context) error {
	n, e := s.R.Eval(ctx, `if redis.call('get',KEYS[1]) ~= ARGV[1] then return 0 end if redis.call('hlen',KEYS[2]) ~= 0 then return 0 end return redis.call('del',KEYS[1])`, []string{s.Prefix + "owner", s.Prefix + "pending"}, s.Owner).Int()
	if e != nil {
		return e
	}
	if n != 1 {
		return ErrOwner
	}
	return nil
}
func (s *Store) Get(ctx context.Context, h string) (File, error) {
	var f File
	b, e := s.R.Get(ctx, s.Prefix+"file:"+h).Bytes()
	if e == redis.Nil {
		return f, ErrMissing
	}
	if e != nil {
		return f, e
	}
	e = json.Unmarshal(b, &f)
	return f, e
}
func (s *Store) Save(ctx context.Context, f File) error {
	b, e := json.Marshal(f)
	if e != nil {
		return e
	}
	_, e = s.R.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.Set(ctx, s.Prefix+"file:"+f.Hash, b, 0)
		if f.ExpiresAt == 0 {
			p.ZRem(ctx, s.Prefix+"expires", f.Hash)
		} else {
			p.ZAdd(ctx, s.Prefix+"expires", redis.Z{Score: float64(f.ExpiresAt), Member: f.Hash})
		}
		p.HDel(ctx, s.Prefix+"pending", f.Hash)
		return nil
	})
	return e
}
func (s *Store) Pending(ctx context.Context, h string) (string, error) {
	v, e := s.R.HGet(ctx, s.Prefix+"pending", h).Result()
	if e == redis.Nil {
		return "", nil
	}
	return v, e
}
func (s *Store) Mark(ctx context.Context, h, action string) error {
	return s.R.HSet(ctx, s.Prefix+"pending", h, action).Err()
}
func (s *Store) Remove(ctx context.Context, h string) error {
	_, e := s.R.TxPipelined(ctx, func(p redis.Pipeliner) error {
		p.Del(ctx, s.Prefix+"file:"+h)
		p.ZRem(ctx, s.Prefix+"expires", h)
		p.HDel(ctx, s.Prefix+"pending", h)
		return nil
	})
	return e
}
func (s *Store) Due(ctx context.Context, now int64) ([]string, error) {
	return s.R.ZRangeByScore(ctx, s.Prefix+"expires", &redis.ZRangeBy{Min: "-inf", Max: fmt.Sprint(now), Offset: 0, Count: 100}).Result()
}
func (s *Store) Retry(ctx context.Context, h string, at int64) error {
	return s.R.ZAdd(ctx, s.Prefix+"expires", redis.Z{Score: float64(at), Member: h}).Err()
}
func (s *Store) AddCode(ctx context.Context, c Code, now int64) (bool, error) {
	b, e := json.Marshal(c)
	if e != nil {
		return false, e
	}
	return s.R.SetNX(ctx, s.Prefix+"code:"+c.Code, b, time.Duration(c.ExpiresAt-now)*time.Second).Result()
}
func (s *Store) Code(ctx context.Context, code string) (Code, error) {
	var c Code
	b, e := s.R.Get(ctx, s.Prefix+"code:"+code).Bytes()
	if e == redis.Nil {
		return c, ErrMissing
	}
	if e != nil {
		return c, e
	}
	e = json.Unmarshal(b, &c)
	return c, e
}
func (s *Store) PutSession(ctx context.Context, id string, f File) error {
	b, e := json.Marshal(f)
	if e != nil {
		return e
	}
	return s.R.Set(ctx, s.Prefix+"session:"+id, b, time.Hour).Err()
}
func (s *Store) Session(ctx context.Context, id string) (File, error) {
	var f File
	b, e := s.R.Get(ctx, s.Prefix+"session:"+id).Bytes()
	if e == redis.Nil {
		return f, ErrMissing
	}
	if e != nil {
		return f, e
	}
	e = json.Unmarshal(b, &f)
	return f, e
}
func (s *Store) EndSession(ctx context.Context, id string) error {
	return s.R.Del(ctx, s.Prefix+"session:"+id).Err()
}
