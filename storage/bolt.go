package storage

import (
	"context"
	"fmt"

	"go.etcd.io/bbolt"
	"google.golang.org/protobuf/proto"

	nodepb "github.com/lancsnet/multi-region/proto"
)

var bucketName = []byte("logs")

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

func (s *BoltStorage) Save(ctx context.Context, entry *nodepb.LogEntry) error {
	data, err := proto.Marshal(entry)
	if err != nil {
		return fmt.Errorf("marshal entry: %w", err)
	}
	return s.db.Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(bucketName).Put([]byte(entry.Id), data)
	})
}

func (s *BoltStorage) Query(ctx context.Context, filter QueryFilter) ([]*nodepb.LogEntry, error) {
	var results []*nodepb.LogEntry
	err := s.db.View(func(tx *bbolt.Tx) error {
		return tx.Bucket(bucketName).ForEach(func(k, v []byte) error {
			entry := &nodepb.LogEntry{}
			if err := proto.Unmarshal(v, entry); err != nil {
				return fmt.Errorf("unmarshal entry: %w", err)
			}
			if filter.NodeID != "" && entry.NodeId != filter.NodeID {
				return nil
			}
			if filter.Since != 0 && entry.Timestamp < filter.Since {
				return nil
			}
			results = append(results, entry)
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
