package polluter

import (
	"database/sql"
	"go.mongodb.org/mongo-driver/mongo"
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

// Polluter pollutes database with given input.
type Polluter struct {
	dbEngine
	parser
}

// Pollute parses input from the reader and
// tries to exec generated commands on a database.
// Use New factory function to generate.
func (p *Polluter) Pollute(r io.Reader) error {
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

// Option defines options for Polluter.
type Option func(*Polluter)

// MySQLEngine option enables MySQL
// engine for poluter.
func MySQLEngine(db *sql.DB) Option {
	return func(p *Polluter) {
		p.dbEngine = mysqlEngine{db}
	}
}

// PostgresEngine option enables
// Postgres engine for Polluter.
func PostgresEngine(db *sql.DB) Option {
	return func(p *Polluter) {
		p.dbEngine = postgresEngine{db}
	}
}

// RedisEngine option enables
// Redis engine for Polluter.
func RedisEngine(cli *redis.Client) Option {
	return func(p *Polluter) {
		p.dbEngine = redisEngine{cli}
	}
}

// MongoEngine option enables
// Mongo engine for Polluter.
func MongoEngine(db *mongo.Database) Option {
	return func(p *Polluter) {
		p.dbEngine = mongoEngine{db}
	}
}

// JSONParser option enables JSON
// parsing engine for seeding.
func JSONParser(p *Polluter) {
	p.parser = jsonParser{}
}

// YAMLParser option enables YAML
// parsing engine for seeding.
func YAMLParser(p *Polluter) {
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
func New(options ...Option) *Polluter {
	p := Polluter{
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
