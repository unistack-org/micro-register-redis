package redis

import (
	"context"
	"strings"
	"sync"
	"time"

	"github.com/docker/docker/registry"
	goredis "github.com/redis/go-redis/v9"
	"go.unistack.org/micro/v3/codec"
	"go.unistack.org/micro/v3/register"
	"go.unistack.org/micro/v3/util/id"
	pool "go.unistack.org/micro/v3/util/xpool"
)

const (
	separator = "/"
)

var (
	_ register.Register = (*Redis)(nil)
	_ register.Watcher  = (*Watcher)(nil)
)

type Redis struct {
	cli      goredis.UniversalClient
	opts     register.Options
	watchers map[string]*Watcher
	services map[string][]*registry.Service
	db       int
	pool     *pool.StringsPool
	sync.RWMutex
}

// NewRegister returns an initialized in-memory register
func NewRegister(opts ...register.Option) *Redis {
	r := &Redis{
		opts:     register.NewOptions(opts...),
		watchers: make(map[string]*Watcher),
		services: make(map[string][]*registry.Service),
	}

	return r
}

func (r *Redis) Connect(ctx context.Context) error {
	return nil
}

func (r *Redis) Disconnect(ctx context.Context) error {
	return nil
}

func (r *Redis) Init(opts ...register.Option) error {
	for _, o := range opts {
		o(&r.opts)
	}

	redisOptions := DefaultOptions

	if r.opts.Context != nil {
		if c, ok := r.opts.Context.Value(configKey{}).(*goredis.UniversalOptions); ok && c != nil {
			redisOptions = c
		}
	}

	if len(r.opts.Addrs) > 0 {
		redisOptions.Addrs = r.opts.Addrs
	}

	if r.opts.TLSConfig != nil {
		redisOptions.TLSConfig = r.opts.TLSConfig
	}

	c := goredis.NewUniversalClient(redisOptions)
	setTracing(c, r.opts.Tracer)
	r.pool = pool.NewStringsPool(50)
	r.db = redisOptions.DB
	r.cli = c
	r.statsMeter()

	return nil
}

func (r *Redis) Options() register.Options {
	return r.opts
}

func (r *Redis) Register(ctx context.Context, s *register.Service, opts ...register.RegisterOption) error {
	options := register.NewRegisterOptions(opts...)
	b := r.pool.Get()
	defer func() {
		b.Reset()
		r.pool.Put(b)
	}()

	for _, n := range s.Nodes {
		buf, err := r.opts.Codec.Marshal(n)
		if err != nil {
			return err
		}
		if err = r.cli.Set(ctx, r.getKey(b, options.Namespace, s.Name, s.Version, n.ID), buf, options.TTL).Err(); err != nil {
			return err
		}
	}

	evt := &register.Event{
		Timestamp: time.Now(),
		Type:      register.EventCreate,
		Service:   s,
		ID:        id.MustNew(),
	}

	buf, err := r.opts.Codec.Marshal(evt)
	if err != nil {
		return err
	}

	if err = r.cli.Publish(ctx, options.Namespace+separator+"events", buf).Err(); err != nil {
		return err
	}

	return nil
}

func (r *Redis) Deregister(ctx context.Context, s *register.Service, opts ...register.DeregisterOption) error {
	options := register.NewDeregisterOptions(opts...)
	b := r.pool.Get()
	defer func() {
		b.Reset()
		r.pool.Put(b)
	}()
	var err error
	for _, n := range s.Nodes {
		if err = r.cli.Del(ctx, r.getKey(b, options.Namespace, s.Name, s.Version, n.ID)).Err(); err != nil && err != goredis.Nil {
			return err
		}
	}

	evt := &register.Event{
		Timestamp: time.Now(),
		Type:      register.EventDelete,
		Service:   s,
		ID:        id.MustNew(),
	}

	buf, err := r.opts.Codec.Marshal(evt)
	if err != nil {
		return err
	}

	if err = r.cli.Publish(ctx, options.Namespace+separator+"events", buf).Err(); err != nil {
		return err
	}

	return nil
}

func (r *Redis) LookupService(ctx context.Context, name string, opts ...register.LookupOption) ([]*register.Service, error) {
	options := register.NewLookupOptions(opts...)
	b := r.pool.Get()
	defer func() {
		b.Reset()
		r.pool.Put(b)
	}()

	keys, err := r.cli.Keys(ctx, r.getKey(b, options.Namespace, name, "*", "")).Result()
	if err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		return nil, register.ErrNotFound
	}

	vals, err := r.cli.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, err
	}
	if len(vals) == 0 {
		return nil, nil
	}

	servicesMap := make(map[string]*register.Service)
	for idx := range keys {
		eidx := strings.LastIndex(keys[idx], separator)
		sidx := strings.LastIndex(keys[idx][:eidx], separator)
		if string(keys[idx][sidx]) == separator {
			sidx++
		}
		name := keys[idx][sidx:eidx]
		svc, ok := servicesMap[name]
		if !ok {
			p := strings.Split(name, "-")
			svc = &register.Service{Name: p[0], Version: p[1]}
		}

		switch v := vals[idx].(type) {
		case string:
			node := &register.Node{}
			if err = r.opts.Codec.Unmarshal([]byte(v), node); err != nil {
				return nil, err
			}
			svc.Nodes = append(svc.Nodes, node)
		case []byte:
			node := &register.Node{}
			if err = r.opts.Codec.Unmarshal(v, node); err != nil {
				return nil, err
			}
			svc.Nodes = append(svc.Nodes, node)
		}

		servicesMap[name] = svc
	}

	svcs := make([]*register.Service, 0, len(servicesMap))
	for _, svc := range servicesMap {
		svcs = append(svcs, svc)
	}

	return svcs, nil
}

func (r *Redis) ListServices(ctx context.Context, opts ...register.ListOption) ([]*register.Service, error) {
	options := register.NewListOptions(opts...)
	b := r.pool.Get()
	defer func() {
		b.Reset()
		r.pool.Put(b)
	}()

	// TODO: replace Keys with Scan
	keys, err := r.cli.Keys(ctx, r.getKey(b, options.Namespace, "*", "", "")).Result()
	if err != nil {
		return nil, err
	}
	if len(keys) == 0 {
		return nil, nil
	}

	vals, err := r.cli.MGet(ctx, keys...).Result()
	if err != nil {
		return nil, err
	}
	if len(vals) == 0 {
		return nil, nil
	}

	servicesMap := make(map[string]*register.Service)
	for idx := range keys {
		eidx := strings.LastIndex(keys[idx], separator)
		sidx := strings.LastIndex(keys[idx][:eidx], separator)
		if string(keys[idx][sidx]) == separator {
			sidx++
		}
		name := keys[idx][sidx:eidx]
		svc, ok := servicesMap[name]
		if !ok {
			p := strings.Split(name, "-")
			svc = &register.Service{Name: p[0], Version: p[1]}
		}

		switch v := vals[idx].(type) {
		case string:
			node := &register.Node{}
			if err = r.opts.Codec.Unmarshal([]byte(v), node); err != nil {
				return nil, err
			}
			svc.Nodes = append(svc.Nodes, node)
		case []byte:
			node := &register.Node{}
			if err = r.opts.Codec.Unmarshal(v, node); err != nil {
				return nil, err
			}
			svc.Nodes = append(svc.Nodes, node)
		}

		servicesMap[name] = svc
	}

	svcs := make([]*register.Service, 0, len(servicesMap))
	for _, svc := range servicesMap {
		svcs = append(svcs, svc)
	}

	return svcs, nil
}

func (r *Redis) Watch(ctx context.Context, opts ...register.WatchOption) (register.Watcher, error) {
	id, err := id.New()
	if err != nil {
		return nil, err
	}
	wo := register.NewWatchOptions(opts...)

	w := &Watcher{
		exit:  make(chan bool),
		res:   make(chan *register.Result),
		id:    id,
		wo:    wo,
		sub:   r.cli.Subscribe(ctx, wo.Namespace+separator+"events"),
		codec: r.opts.Codec,
	}

	r.Lock()
	r.watchers[w.id] = w
	r.Unlock()

	return w, nil
}

func (r *Redis) Name() string {
	return r.opts.Name
}

func (r *Redis) String() string {
	return "redis"
}

type Watcher struct {
	res   chan *register.Result
	exit  chan bool
	wo    register.WatchOptions
	id    string
	codec codec.Codec
	sub   *goredis.PubSub
}

func (w *Watcher) Next() (*register.Result, error) {
	var err error
	for {
		select {
		case msg := <-w.sub.Channel():
			evt := &register.Event{}
			if err = w.codec.Unmarshal([]byte(msg.Payload), evt); err != nil {
				return nil, err
			}

			if evt.Service == nil {
				continue
			}

			if len(w.wo.Service) > 0 && w.wo.Service != evt.Service.Name {
				continue
			}

			namespace := register.DefaultNamespace
			if evt.Service.Namespace != "" {
				namespace = evt.Service.Namespace
			}

			// only send the event if watching the wildcard or this specific domain
			if w.wo.Namespace == register.WildcardNamespace || w.wo.Namespace == namespace {
				return &register.Result{Service: evt.Service, Action: evt.Type}, nil
			}

		case <-w.exit:
			return nil, register.ErrWatcherStopped
		}
	}
}

func (w *Watcher) Stop() {
	select {
	case <-w.exit:
		return
	default:
		w.sub.Close()
		close(w.exit)
	}
}

func (r *Redis) getKey(b *strings.Builder, opNamespace string, name string, version string, nid string) string {
	if opNamespace != "" {
		b.WriteString(opNamespace)
		b.WriteString(separator)
	}

	if name != "" {
		b.WriteString(name)
	}

	if version != "" {
		b.WriteString("-")
		b.WriteString(version)
	}

	if nid != "" {
		b.WriteString(separator)
		b.WriteString(nid)
	}
	return b.String()
}
