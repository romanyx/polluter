package polluter

import (
	"encoding/json"

	"github.com/go-redis/redis"
	"github.com/pkg/errors"
	"github.com/romanyx/jwalk"
)

type redisEngine struct {
	cli *redis.Client
}

func (e redisEngine) exec(cmds []command) error {
	for _, cmd := range cmds {
		if err := e.cli.Set(cmd.q, cmd.args[0], 0).Err(); err != nil {
			return errors.Wrap(err, "failed to set")
		}
	}
	return nil
}

func (e redisEngine) build(obj jwalk.ObjectWalker) commands {
	cmds := make(commands, 0)

	obj.Walk(func(key string, value interface{}) {
		data, err := json.Marshal(value)
		if err != nil {
			panic(err)
		}

		cmds = append(cmds, command{key, []interface{}{data}})
	})

	return cmds
}
