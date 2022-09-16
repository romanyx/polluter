package polluter

import (
	"bytes"
	"database/sql"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_postgresEngine_build(t *testing.T) {
	tests := []struct {
		name   string
		input  []byte
		expect commands
	}{
		{
			name:  "example input",
			input: []byte(`{"users":[{"id":1,"name":"Roman"},{"id":2,"name":"Dmitry"}],"roles":[{"id":2,"role_ids":[1,2]}]}`),
			expect: commands{
				command{
					q: `INSERT INTO "users" ("id", "name") VALUES ($1, $2);`,
					args: []interface{}{
						float64(1),
						"Roman",
					},
				},
				command{
					q: `INSERT INTO "users" ("id", "name") VALUES ($1, $2);`,
					args: []interface{}{
						float64(2),
						"Dmitry",
					},
				},
				command{
					q: `INSERT INTO "roles" ("id", "role_ids") VALUES ($1, $2);`,
					args: []interface{}{
						float64(2),
						[]interface{}{
							float64(1),
							float64(2),
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

			obj, err := jsonParser{}.parse(bytes.NewReader(tt.input))
			if err != nil {
				assert.Nil(t, err)
			}

			e := postgresEngine{}
			got, err := e.build(obj)
			assert.Nil(t, err)
			assert.Equal(t, tt.expect, got)
		})
	}
}

func Test_postgresEngine_exec(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode")
	}

	tests := []struct {
		name    string
		args    []command
		wantErr bool
	}{
		{
			name: "valid query",
			args: []command{
				{
					q: `INSERT INTO "users" ("id", "name") VALUES ($1, $2);`,
					args: []interface{}{
						1,
						"Roman",
					},
				},
			},
		},
		{
			name: "invalid query",
			args: []command{
				{
					q: `INSERT INTO "roles" ("id", "name") VALUES ($1, $2);`,
					args: []interface{}{
						1,
						"User",
					},
				},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			db, teardown := preparePostgresDB(t)
			defer teardown()
			e := postgresEngine{db}

			err := e.exec(tt.args)

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

func preparePostgresDB(t *testing.T) (db *sql.DB, teardown func() error) {
	dbName := fmt.Sprintf("db_%d", time.Now().UnixNano())
	db, err := sql.Open("pgsqltx", dbName)

	if err != nil {
		log.Fatalf("open mysql connection: %s", err)
	}

	return db, db.Close
}
