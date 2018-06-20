package polluter

import (
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_jsonParser_parse(t *testing.T) {
	tests := []struct {
		name    string
		arg     io.Reader
		wantErr bool
	}{
		{
			name: "valid input",
			arg: strings.NewReader(`{
				"users": [{
					"id": 1,
					"name": "Roman"
				}]
			}`),
		},
		{
			name: "invalid input",
			arg: strings.NewReader(`{
				"users": [{
					"id": 1,
					"name": "Roman",
					"role_id": 1,
				}]
			}`),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := jsonParser{}
			_, err := s.parse(tt.arg)

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
