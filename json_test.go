package polluter

import (
	"encoding/json"
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
		expect  collections
	}{
		{
			name: "valid input",
			arg: strings.NewReader(`{
				"users": [{
					"id": 1,
					"name": "Roman"
				}]
			}`),
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

			s := jsonParser{}
			data, err := s.parse(tt.arg)

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

func Test_collectionsUnmarshalJSON(t *testing.T) {
	tests := []struct {
		name   string
		input  string
		expect collections
	}{
		{
			name: "simple collection",
			input: `{
				"users": [{
					"id": 1,
					"name": "Roman"
				}]
			}`,
			expect: collections{
				collection{
					name: "users",
					records: []record{
						record{
							field{"id", float64(1)},
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
			var c collections

			if err := json.Unmarshal([]byte(tt.input), &c); err != nil {
				assert.Nil(t, err)
			}

			assert.Equal(t, tt.expect, c)
		})
	}
}

func Benchmark_collectionsUnmarshalJSON(b *testing.B) {
	data := []byte(`{
		"users": [{
			"id": 1,
			"name": "Roman"
		}, {
			"id": 2,
			"name": "Dmitry"
		}],
		"roles": [{
			"id": 1,
			"name": "User",
		}]
	}`)

	for i := 0; i < b.N; i++ {
		var c collection
		json.Unmarshal(data, &c)
	}
}
