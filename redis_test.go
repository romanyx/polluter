package polluter

import (
	"bytes"
	"log"
	"testing"

	"github.com/go-redis/redis"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func Test_redisEngine_build(t *testing.T) {
	tests := []struct {
		name   string
		input  []byte
		expect commands
	}{
		{
			name:  "example input",
			input: []byte(`{"count":1,"values":[1,2],"obj":{"key":"value"}}`),
			expect: commands{
				command{
					q: "count",
					args: []interface{}{
						[]byte(`1`),
					},
				},
				command{
					q: "values",
					args: []interface{}{
						[]byte(`[1,2]`),
					},
				},
				command{
					q: "obj",
					args: []interface{}{
						[]byte(`{"key":"value"}`),
					},
				},
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			obj, err := jsonParser{}.parse(bytes.NewReader(tt.input))
			if err != nil {
				assert.Nil(t, err)
			}

			e := redisEngine{}
			got := e.build(obj)
			assert.Equal(t, tt.expect, got)
		})
	}
}

func Test_redisEngine_exec(t *testing.T) {
	tests := []struct {
		name    string
		args    []command
		wantErr bool
	}{
		{
			name: "valid query",
			args: []command{
				{
					q: "count",
					args: []interface{}{
						"1",
					},
				},
			},
		},
	}

	for i, tt := range tests {
		i := i
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			cli, teardown := prepareRedisDB(t, i)
			defer teardown()
			e := redisEngine{cli}

			err := e.exec(tt.args)

			if tt.wantErr && err == nil {
				assert.NotNil(t, err)
				return
			}

			if !tt.wantErr && err != nil {
				assert.Nil(t, err)
			}
		})
	}
}

func prepareRedisDB(t *testing.T, db int) (cli *redis.Client, teardown func() error) {
	cli = redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       db,
	})

	_, err := cli.Ping().Result()
	if err != nil {
		log.Fatalf("ping redis: %s", err)
	}

	return cli, func() error {
		if err := cli.FlushDB().Err(); err != nil {
			return errors.Wrap(err, "flush db")
		}
		return nil
	}
}
