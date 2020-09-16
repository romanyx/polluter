package polluter

import (
	"bytes"
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"
	"testing"
	"time"
)

func Test_mongoEngineBuild(t *testing.T) {
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
					q: "users",
					args: []interface{}{
						bson.D{
							bson.E{
								Key:   "id",
								Value: int32(1),
							},
							bson.E{
								Key:   "name",
								Value: "Roman",
							},
						},
						bson.D{
							bson.E{
								Key:   "id",
								Value: int32(2),
							},
							bson.E{
								Key:   "name",
								Value: "Dmitry",
							},
						},
					},
				},
				command{
					q: "roles",
					args: []interface{}{
						bson.D{
							bson.E{
								Key:   "id",
								Value: int32(2),
							},
							bson.E{
								Key:   "role_ids",
								Value: bson.A{int32(1), int32(2)},
							},
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

			e := mongoEngine{}
			got, err := e.build(obj)
			assert.Nil(t, err)
			assert.Equal(t, tt.expect, got)
		})
	}
}

func Test_mongoEngine_exec(t *testing.T) {
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
			args: []command{},
		}, {
			name: "invalid query",
			args: []command{},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			db, teardown := prepareMongoDB(t)
			defer teardown()
			e := mongoEngine{db}

			err := e.exec(tt.args)

			if tt.wantErr && err != nil {
				assert.NotNil(t, err)
				return
			}

			if !tt.wantErr && err != nil {
				assert.Nil(t, err)
			}
		})
	}
}

func prepareMongoDB(t *testing.T) (db *mongo.Database, teardown func() error) {
	dbName := fmt.Sprintf("db_%d", time.Now().UnixNano())
	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI("mongodb://localhost:27017"))
	if err != nil {
		log.Fatalf("open mongo connection: %s", err)
	}
	return client.Database(dbName), func() error {
		return client.Disconnect(context.Background())
	}
}
