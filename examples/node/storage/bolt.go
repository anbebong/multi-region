package storage

import (
	"context"
	"fmt"

	"go.etcd.io/bbolt"
	"google.golang.org/protobuf/proto"

	nodepb "github.com/anbebong/multi-region/proto"
)

var bucketName = []byte("envelopes")

type BoltStorage struct {
	db *bbolt.DB
}

func NewBoltStorage(path string) (*BoltStorage, error) {
	db, err := bbolt.Open(path, 0600, nil)
	if err != nil {
		return nil, fmt.Errorf("open bolt db: %w", err)
	}
	err = db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(bucketName)
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("create bucket: %w", err)
	}
	return &BoltStorage{db: db}, nil
}

func (s *BoltStorage) Save(ctx context.Context, env *nodepb.Envelope) error {
	data, err := proto.Marshal(env)
	if err != nil {
		return fmt.Errorf("marshal envelope: %w", err)
	}
	return s.db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(bucketName).Put([]byte(env.Id), data)
	})
}

func (s *BoltStorage) Query(ctx context.Context, filter QueryFilter) ([]*nodepb.Envelope, error) {
	var results []*nodepb.Envelope
	err := s.db.View(func(tx *bbolt.Tx) error {
		return tx.Bucket(bucketName).ForEach(func(k, v []byte) error {
			env := &nodepb.Envelope{}
			if err := proto.Unmarshal(v, env); err != nil {
				return fmt.Errorf("unmarshal envelope: %w", err)
			}
			if filter.Kind != "" && env.Kind != filter.Kind {
				return nil
			}
			if filter.Since != 0 && env.Timestamp < filter.Since {
				return nil
			}
			results = append(results, env)
			return nil
		})
	})
	return results, err
}

func (s *BoltStorage) Delete(ctx context.Context, ids []string) error {
	return s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketName)
		for _, id := range ids {
			if err := b.Delete([]byte(id)); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *BoltStorage) Close() error {
	return s.db.Close()
}
