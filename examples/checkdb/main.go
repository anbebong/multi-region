// Command checkdb is a throwaway inspection tool: it opens examples/node's
// BoltDB storage file and prints every Envelope record found. Used to
// manually verify that a log entry submitted on one node actually reached
// another.
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/anbebong/multi-region/examples/node/storage"
)

func main() {
	s, err := storage.NewBoltStorage(os.Args[1])
	if err != nil {
		panic(err)
	}
	defer s.Close()
	entries, err := s.Query(context.Background(), storage.QueryFilter{})
	if err != nil {
		panic(err)
	}
	for _, e := range entries {
		fmt.Printf("id=%s kind=%s payload=%s\n", e.Id, e.Kind, string(e.Payload))
	}
	fmt.Printf("total=%d\n", len(entries))
}
