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
`
)

func Test_yamlParser_parse(t *testing.T) {
	tests := []struct {
		name    string
		arg     io.Reader
		wantErr bool
		expect  collections
	}{
		{
			name:    "valid input",
			arg:     strings.NewReader(yamlInput),
			wantErr: false,
			expect: collections{
				collection{
					name: "users",
					records: []record{
						record{
							field{"id", float64(1)}, // JSON parses numbers as float64 to interface.
							field{"name", "Roman"},
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := yamlParser{}
			data, err := p.parse(tt.arg)

			if tt.wantErr && err == nil {
				assert.NotNil(t, err)
				return
			}

			if !tt.wantErr && err != nil {
				assert.Nil(t, err)
			}

			assert.Equal(t, tt.expect, data)
		})
	}
}
