package polluter

import (
	"database/sql"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

const (
	input = `users:
- id: 1
  name: Roman
- id: 2
  name: Dmitry`
)

func TestNew(t *testing.T) {
	p := New()
	err := p.Pollute(strings.NewReader(input))
	assert.NotNil(t, err)
}

type parserFunc func(io.Reader) (collections, error)

func (f parserFunc) parse(r io.Reader) (collections, error) {
	return f(r)
}

type dbEngineFunc func([]command) error

func (f dbEngineFunc) exec(cmds []command) error {
	return f(cmds)
}

func (f dbEngineFunc) build(colls collections) commands {
	return commands{
		command{
			q: "INSERT INTO",
			args: []interface{}{
				1,
			},
		},
	}
}

func Test_polluterPollute(t *testing.T) {
	tests := []struct {
		name     string
		parser   parser
		dbEngine dbEngine
		wantErr  bool
	}{
		{
			name: "parsing error",
			parser: parserFunc(func(r io.Reader) (collections, error) {
				return nil, errors.New("mocked error")
			}),
			wantErr: true,
		},
		{
			name: "insert error",
			parser: parserFunc(func(r io.Reader) (collections, error) {
				return make(collections, 0), nil
			}),
			dbEngine: dbEngineFunc(func(_ []command) error {
				return errors.New("mocked error")
			}),
			wantErr: true,
		},
		{
			name: "without errors",
			parser: parserFunc(func(r io.Reader) (collections, error) {
				return make(collections, 0), nil
			}),
			dbEngine: dbEngineFunc(func(_ []command) error {
				return nil
			}),
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &polluter{
				parser:   tt.parser,
				dbEngine: tt.dbEngine,
			}

			err := p.Pollute(nil)

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

func TestIntegrationPollute(t *testing.T) {
	tests := []struct {
		name      string
		dbOption  func(db *sql.DB) Option
		dbBuilder func(t *testing.T) (*sql.DB, func() error)
		input     io.Reader
	}{
		{
			name: "mysql",
			dbOption: func(db *sql.DB) Option {
				return MySQLEngine(db)
			},
			dbBuilder: prepareMySQLDB,
			input:     strings.NewReader(input),
		},
		{
			name: "postgres",
			dbOption: func(db *sql.DB) Option {
				return PostgresEngine(db)
			},
			dbBuilder: preparePostgresDB,
			input:     strings.NewReader(input),
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db, teardown := tt.dbBuilder(t)
			defer teardown()

			options := []Option{
				tt.dbOption(db),
			}
			p := New(options...)
			err := p.Pollute(tt.input)
			assert.Nil(t, err)

			var count int
			row := db.QueryRow("SELECT COUNT(*) FROM users;")
			if err := row.Scan(&count); err != nil {
				t.Fatalf("failed to query users: %s", err)
			}

			assert.Equal(t, 2, count)
		})
	}
}
