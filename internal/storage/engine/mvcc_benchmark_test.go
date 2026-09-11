package engine

import (
	"context"
	"fmt"
	"testing"

	"github.com/rivetdb/rivetdb/internal/storage"
	"github.com/rivetdb/rivetdb/internal/testutil"
)

func BenchmarkMVCCGetAtVersionDepth(b *testing.B) {
	for _, depth := range []int{1, 10, 100, 1000} {
		b.Run(fmt.Sprintf("versions-%d", depth), func(b *testing.B) {
			e, err := Open(Options{Directory: testutil.BenchmarkDir(b), Mode: ModeReplicatedMVCC, MemTableBytes: 16 << 20})
			if err != nil {
				b.Fatal(err)
			}
			defer e.Close(context.Background())
			for index := 1; index <= depth; index++ {
				value := []byte(fmt.Sprintf("v-%04d", index))
				if err := e.ApplyCommittedMVCC(context.Background(), uint64(index), 1, uint64(index), value, storage.Mutation{Kind: storage.KindValue, Key: []byte("hot"), Value: value}); err != nil {
					b.Fatal(err)
				}
			}
			target := uint64(max(1, depth/2))
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if _, err := e.GetAt(context.Background(), []byte("hot"), target); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkMVCCScanAt(b *testing.B) {
	e, err := Open(Options{Directory: testutil.BenchmarkDir(b), Mode: ModeReplicatedMVCC, MemTableBytes: 16 << 20})
	if err != nil {
		b.Fatal(err)
	}
	defer e.Close(context.Background())
	for i := 1; i <= 1000; i++ {
		key := []byte(fmt.Sprintf("k-%04d", i))
		if err := e.ApplyCommittedMVCC(context.Background(), uint64(i), 1, uint64(i), key, storage.Mutation{Kind: storage.KindValue, Key: key, Value: key}); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		if _, err := e.ScanAt(context.Background(), nil, nil, 1000); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkMVCCGetLatestRecentOld(b *testing.B) {
	e, err := Open(Options{Directory: testutil.BenchmarkDir(b), Mode: ModeReplicatedMVCC, MemTableBytes: 16 << 20})
	if err != nil {
		b.Fatal(err)
	}
	defer e.Close(context.Background())
	for index := 1; index <= 1000; index++ {
		value := []byte(fmt.Sprintf("v-%04d", index))
		if err := e.ApplyCommittedMVCC(context.Background(), uint64(index), 1, uint64(index), value, storage.Mutation{Kind: storage.KindValue, Key: []byte("hot"), Value: value}); err != nil {
			b.Fatal(err)
		}
	}
	for _, test := range []struct {
		name string
		read func() error
	}{{"latest", func() error { _, err := e.Get(context.Background(), []byte("hot")); return err }}, {"recent", func() error { _, err := e.GetAt(context.Background(), []byte("hot"), 999); return err }}, {"old", func() error { _, err := e.GetAt(context.Background(), []byte("hot"), 1); return err }}} {
		b.Run(test.name, func(b *testing.B) {
			b.ReportAllocs()
			for range b.N {
				if err := test.read(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
