package polluter

import (
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/romanyx/jwalk"
	"github.com/stretchr/testify/assert"
)

const (
	input = `users:
- id: 1
  name: Roman
- id: 2
  name: Dmitry`
	pgInput = input + `
all:
- group: first
- group: second
  `
)

func TestNew(t *testing.T) {
	p := New()
	err := p.Pollute(strings.NewReader(input))
	assert.NotNil(t, err)
}

type parserFunc func(io.Reader) (jwalk.ObjectWalker, error)

func (f parserFunc) parse(r io.Reader) (jwalk.ObjectWalker, error) {
	return f(r)
}

type dbEngineFunc func([]command) error

func (f dbEngineFunc) exec(cmds []command) error {
	return f(cmds)
}

func (f dbEngineFunc) build(obj jwalk.ObjectWalker) (commands, error) {
	return commands{
		command{
			q: "INSERT INTO",
			args: []interface{}{
				1,
			},
		},
	}, nil
}

type objectWalker struct{}

func (o objectWalker) Walk(fn func(name string, value interface{}) error) error {
	return nil
}

func (o objectWalker) MarshalJSON() ([]byte, error) {
	return make([]byte, 0), nil
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
			parser: parserFunc(func(r io.Reader) (jwalk.ObjectWalker, error) {
				return nil, errors.New("mocked error")
			}),
			wantErr: true,
		},
		{
			name: "insert error",
			parser: parserFunc(func(r io.Reader) (jwalk.ObjectWalker, error) {
				return new(objectWalker), nil
			}),
			dbEngine: dbEngineFunc(func(_ []command) error {
				return errors.New("mocked error")
			}),
			wantErr: true,
		},
		{
			name: "without errors",
			parser: parserFunc(func(r io.Reader) (jwalk.ObjectWalker, error) {
				return new(objectWalker), nil
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

			p := &Polluter{
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

func TestPollute(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	tests := []struct {
		name   string
		option func(t *testing.T) (Option, func() error)
		input  io.Reader
	}{
		{
			name: "postgres",
			option: func(t *testing.T) (Option, func() error) {
				db, teardown := preparePostgresDB(t)
				return PostgresEngine(db), teardown
			},
			input: strings.NewReader(pgInput),
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			option, teardown := tt.option(t)
			defer teardown()

			options := []Option{
				option,
			}

			p := New(options...)
			err := p.Pollute(tt.input)
			assert.Nil(t, err)
		})
	}
}
