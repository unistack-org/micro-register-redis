package redis

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"go.unistack.org/micro/v5/register"
)

func TestRegister(t *testing.T) {
	ctx := context.Background()
	s := miniredis.RunT(t)

	r := NewRegister(register.Addrs(s.Addr()))
	if err := r.Init(); err != nil {
		t.Fatal(err)
	}
	if err := r.Connect(ctx); err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := r.Disconnect(ctx); err != nil {
			t.Fatal(err)
		}
	}()

	nodes := []*register.Node{
		{ID: "1", Address: "11.22.33.44"},
	}
	svc := &register.Service{Name: "test", Version: "1.2.0", Nodes: nodes}
	if err := r.Register(ctx, svc, register.RegisterTTL(100*time.Millisecond)); err != nil {
		t.Fatal(err)
	}

	svcs, err := r.ListServices(ctx)
	if err != nil {
		t.Fatal(err)
	} else if len(svcs) == 0 {
		t.Fatalf("no services registered")
	}
	if svcs[0].Name != "test" || svcs[0].Version != "1.2.0" {
		t.Fatalf("invalid service %#+v", svcs[0])
	}

	svcs, err = r.LookupService(ctx, "test")
	if err != nil {
		t.Fatal(err)
	} else if len(svcs) == 0 {
		t.Fatalf("no services registered")
	}
	if svcs[0].Name != "test" || svcs[0].Version != "1.2.0" {
		t.Fatalf("invalid service %d %#+v", len(svcs), svcs[0])
	}

	s.FastForward(200 * time.Millisecond)

	svcs, err = r.ListServices(ctx)
	if err != nil {
		t.Fatal(err)
	} else if len(svcs) != 0 {
		t.Fatalf("have registered services")
	}
}

func TestDeregister(t *testing.T) {
	ctx := context.Background()
	s := miniredis.RunT(t)

	r := NewRegister(register.Addrs(s.Addr()))
	if err := r.Init(); err != nil {
		t.Fatal(err)
	}
	if err := r.Connect(ctx); err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := r.Disconnect(ctx); err != nil {
			t.Fatal(err)
		}
	}()

	nodes := []*register.Node{
		{ID: "1", Address: "11.22.33.44"},
	}
	svc := &register.Service{Name: "test", Version: "1.2.0", Nodes: nodes}
	if err := r.Register(ctx, svc, register.RegisterTTL(500*time.Millisecond)); err != nil {
		t.Fatal(err)
	}

	svcs, err := r.LookupService(ctx, "test")
	if err != nil {
		t.Fatal(err)
	} else if len(svcs) == 0 {
		t.Fatalf("no services registered")
	}
	if svcs[0].Name != "test" || svcs[0].Version != "1.2.0" {
		t.Fatalf("invalid service %#+v", svcs[0])
	}

	if err := r.Deregister(ctx, svc); err != nil {
		t.Fatal(err)
	}

	svcs, err = r.LookupService(ctx, "test")
	if err == nil {
		t.Fatalf("service not deregistered")
	} else if len(svcs) != 0 {
		t.Fatalf("service not deregistered")
	}
}

func TestWatch(t *testing.T) {
	ctx := context.Background()
	s := miniredis.RunT(t)

	r := NewRegister(register.Addrs(s.Addr()))
	if err := r.Init(); err != nil {
		t.Fatal(err)
	}
	if err := r.Connect(ctx); err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := r.Disconnect(ctx); err != nil {
			t.Fatal(err)
		}
	}()

	w, err := r.Watch(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Stop()

	cherr := make(chan error)
	go func() {
		for {
			res, err := w.Next()
			if err != nil {
				cherr <- err
			}
			if res.Action != register.EventCreate {
				cherr <- fmt.Errorf("invalid event %#+v", res)
			}
			if res.Service.Name != "test" {
				cherr <- fmt.Errorf("invalid event %#+v", res)
			}
			if res.Service.Version != "1.2.0" {
				cherr <- fmt.Errorf("invalid event %#+v", res)
			}
			if len(res.Service.Nodes) != 1 {
				cherr <- fmt.Errorf("invalid event %#+v", res)
			}
			if res.Service.Nodes[0].ID != "1" {
				cherr <- fmt.Errorf("invalid event %#+v", res)
			}
			if res.Service.Nodes[0].Address != "11.22.33.44" {
				cherr <- fmt.Errorf("invalid event %#+v", res)
			}
			cherr <- nil
			break
		}
	}()

	nodes := []*register.Node{
		{ID: "1", Address: "11.22.33.44"},
	}

	go func() {
		for {
			svc := &register.Service{Name: "test", Version: "1.2.0", Nodes: nodes}
			if err := r.Register(ctx, svc, register.RegisterTTL(500*time.Millisecond)); err != nil {
				cherr <- err
			}
			time.Sleep(300 * time.Millisecond)
		}
	}()

	if err = <-cherr; err != nil {
		t.Fatal(err)
	}
}
