package polluter

import (
	"database/sql"
	"io"

	"github.com/pkg/errors"
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

type collections []collection

type collection struct {
	name    string
	records []record
}

type record []field

type field struct {
	name string
	val  interface{}
}

type parser interface {
	parse(io.Reader) (collections, error)
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
	build(collections) commands
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
	input, err := p.parser.parse(r)
	if err != nil {
		return errors.Wrap(err, "parse failed")
	}

	commands := p.dbEngine.build(input)
	if err := p.dbEngine.exec(commands); err != nil {
		return errors.Wrap(err, "exec failed")
	}

	return nil
}

// Option defines options for polluter.
type Option func(*polluter)

// MySQLEngine options enables MySQL
// engine for poluter.
func MySQLEngine(db *sql.DB) Option {
	return func(p *polluter) {
		p.dbEngine = mysqlEngine{db}
	}
}

// PostgresEngine options enables
// Postgres engine for poluter.
func PostgresEngine(db *sql.DB) Option {
	return func(p *polluter) {
		p.dbEngine = postgresEngine{db}
	}
}

// JSONParser options enambles JSON
// parsing engine for seeding.
func JSONParser(p *polluter) {
	p.parser = jsonParser{}
}

// YAMLParser options enambles YAML
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

func (e errorEngine) build(_ collections) commands {
	return commands{
		command{},
	}
}

func (e errorEngine) exec(_ []command) error {
	return ErrEngineNotSpecified
}
