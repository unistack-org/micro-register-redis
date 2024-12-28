package redis

import (
	"time"

	goredis "github.com/redis/go-redis/v9"
	"go.unistack.org/micro/v3/register"
)

var (
	DefaultSeparator = "/"
	DefaultOptions   = &goredis.UniversalOptions{
		Username:        "",
		Password:        "", // no password set
		DB:              0,  // use default DB
		MaxRetries:      2,
		MaxRetryBackoff: 256 * time.Millisecond,
		DialTimeout:     1 * time.Second,
		ReadTimeout:     1 * time.Second,
		WriteTimeout:    1 * time.Second,
		PoolTimeout:     1 * time.Second,
		MinIdleConns:    10,
	}
)

type configKey struct{}

func Config(c *goredis.UniversalOptions) register.Option {
	return register.SetOption(configKey{}, c)
}
