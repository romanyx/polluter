package polluter

import (
	"database/sql"
	"io"

	"github.com/go-redis/redis"
	"github.com/pkg/errors"
	"github.com/romanyx/jwalk"
)

var (
	// ErrEngineNotSpecified causes if no engine option was used
	// with the factory method.
	ErrEngineNotSpecified = errors.New("specify database engine with the factory method option")
)

// Polluter pollutes database with given input.
type Polluter interface {
	Pollute(io.Reader) error
}

type parser interface {
	parse(io.Reader) (jwalk.ObjectWalker, error)
}

type execer interface {
	exec([]command) error
}

type commands []command

type command struct {
	q    string
	args []interface{}
}

type builder interface {
	build(jwalk.ObjectWalker) (commands, error)
}

type dbEngine interface {
	builder
	execer
}

type polluter struct {
	dbEngine
	parser
}

func (p *polluter) Pollute(r io.Reader) error {
	obj, err := p.parser.parse(r)
	if err != nil {
		return errors.Wrap(err, "parse failed")
	}

	commands, err := p.dbEngine.build(obj)
	if err != nil {
		return errors.Wrap(err, "build commands failed")
	}
	if err := p.dbEngine.exec(commands); err != nil {
		return errors.Wrap(err, "exec failed")
	}

	return nil
}

// Option defines options for polluter.
type Option func(*polluter)

// MySQLEngine option enables MySQL
// engine for poluter.
func MySQLEngine(db *sql.DB) Option {
	return func(p *polluter) {
		p.dbEngine = mysqlEngine{db}
	}
}

// PostgresEngine option enables
// Postgres engine for poluter.
func PostgresEngine(db *sql.DB) Option {
	return func(p *polluter) {
		p.dbEngine = postgresEngine{db}
	}
}

// RedisEngine option enables
// Redis engine for poluter.
func RedisEngine(cli *redis.Client) Option {
	return func(p *polluter) {
		p.dbEngine = redisEngine{cli}
	}
}

// JSONParser option enambles JSON
// parsing engine for seeding.
func JSONParser(p *polluter) {
	p.parser = jsonParser{}
}

// YAMLParser option enambles YAML
// parsing engine for seeding.
func YAMLParser(p *polluter) {
	p.parser = yamlParser{}
}

// New factory method returns initialized
// Polluter.
// For example to seed MySQL database with
// JSON input use:
//		p := New(MySQLEngine(db))
// To seed Postgres database with YAML input
// use:
// 		p := New(PostgresEngine(db), YAMLParser)
func New(options ...Option) Polluter {
	p := polluter{
		parser:   yamlParser{},
		dbEngine: errorEngine{},
	}

	for i := range options {
		options[i](&p)
	}

	return &p
}

type errorEngine struct{}

func (e errorEngine) build(_ jwalk.ObjectWalker) (commands, error) {
	return commands{
		command{},
	}, ErrEngineNotSpecified
}

func (e errorEngine) exec(_ []command) error {
	return ErrEngineNotSpecified
}
