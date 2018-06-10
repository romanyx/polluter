package polluter

import (
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
		arg    collections
		expect commands
	}{
		{
			name: "multiple",
			arg: collections{
				collection{
					name: "users",
					records: []record{
						record{
							field{"id", 1},
							field{"name", "Roman"},
						},
						record{
							field{"id", 2},
							field{"name", "Dmitry"},
						},
					},
				},
				collection{
					name: "roles",
					records: []record{
						record{
							field{"id", 1},
							field{"name", "User"},
						},
					},
				},
			},
			expect: commands{
				command{
					q: "INSERT INTO users (id, name) VALUES ($1, $2);",
					args: []interface{}{
						1,
						"Roman",
					},
				},
				command{
					q: "INSERT INTO users (id, name) VALUES ($1, $2);",
					args: []interface{}{
						2,
						"Dmitry",
					},
				},
				command{
					q: "INSERT INTO roles (id, name) VALUES ($1, $2);",
					args: []interface{}{
						1,
						"User",
					},
				},
			},
		},
		{
			name: "single",
			arg: collections{
				collection{
					name: "roles",
					records: []record{
						record{
							field{"id", 1},
						},
					},
				},
			},
			expect: commands{
				command{
					q: "INSERT INTO roles (id) VALUES ($1);",
					args: []interface{}{
						1,
					},
				},
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := postgresEngine{}
			got := e.build(tt.arg)
			assert.Equal(t, tt.expect, got)
		})
	}
}

func Test_postgresEngine_exec(t *testing.T) {
	tests := []struct {
		name    string
		args    []command
		wantErr bool
	}{
		{
			name: "valid query",
			args: []command{
				command{
					q: "INSERT INTO users (id, name) VALUES ($1, $2);",
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
				command{
					q: "INSERT INTO roles (id, name) VALUES ($1, $2);",
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
