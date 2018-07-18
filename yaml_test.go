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
	}{
		{
			name: "valid input",
			arg:  strings.NewReader(yamlInput),
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := yamlParser{}
			_, err := p.parse(tt.arg)

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
