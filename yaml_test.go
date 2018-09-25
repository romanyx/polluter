package polluter

import (
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

const (
	yamlInput = `users:
- id: 1
  name: Roman
c: c
b: b
a: a
`
)

func Test_yamlParser_parse(t *testing.T) {
	tests := []struct {
		name    string
		arg     io.Reader
		order   []string
		wantErr bool
	}{
		{
			name: "valid input",
			arg:  strings.NewReader(yamlInput),
			order: []string{
				"users",
				"c",
				"b",
				"a",
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := yamlParser{}
			w, err := p.parse(tt.arg)

			if tt.wantErr && err == nil {
				assert.NotNil(t, err)
				return
			}

			if !tt.wantErr {
				if err != nil {
					assert.Nil(t, err)
					return
				}

				var gotOrder []string

				w.Walk(func(key string, _ interface{}) error {
					gotOrder = append(gotOrder, key)
					return nil
				})

				assert.Equal(t, tt.order, gotOrder)
			}
		})
	}
}
