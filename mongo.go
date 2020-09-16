package polluter

import (
	"context"
	"encoding/json"
	"github.com/pkg/errors"
	"github.com/romanyx/jwalk"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

type mongoEngine struct {
	db *mongo.Database
}

func (m mongoEngine) exec(cmds []command) error {
	for _, c := range cmds {
		coll := m.db.Collection(c.q)
		if _, err := coll.InsertMany(context.Background(), c.args); err != nil {
			return errors.Wrap(err, "failed to insert one")
		}
	}
	return nil
}

func (m mongoEngine) build(obj jwalk.ObjectWalker) (commands, error) {
	cmds := make(commands, 0)
	if err := obj.Walk(func(collection string, value interface{}) error {
		data, err := json.Marshal(value)
		if err != nil {
			return err
		}
		docs := make([]bson.D, 0)
		if err = bson.UnmarshalExtJSON(data, true, &docs); err != nil {
			return err
		}
		args := make([]interface{}, len(docs))
		for i, doc := range docs {
			args[i] = doc
		}
		cmds = append(cmds, command{collection, args})
		return nil
	}); err != nil {
		return nil, err
	}

	return cmds, nil
}
