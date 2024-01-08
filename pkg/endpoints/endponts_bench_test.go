package endpoints

import (
	"context"
	"testing"
)

func BenchmarkEndpointGroup(b *testing.B) {
	e := newEndpointGroup()
	e.setIPs(map[string]struct{}{"10.0.0.1": {}}, map[string]int32{"testPort": 1})
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := e.getBestHost(context.Background(), "testPort")
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
