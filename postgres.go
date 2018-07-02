package polluter

import (
	"database/sql"
	"fmt"

	"github.com/pkg/errors"
	"github.com/romanyx/jwalk"
)

type postgresEngine struct {
	db *sql.DB
}

func (e postgresEngine) exec(cmds []command) error {
	tx, err := e.db.Begin()
	if err != nil {
		return errors.Wrap(err, "tx begin")
	}

	for _, c := range cmds {
		if _, err := tx.Exec(c.q, c.args...); err != nil {
			if rErr := tx.Rollback(); rErr != nil {
				err = errors.Wrap(rErr, err.Error())
			}
			return errors.Wrap(err, "exec")
		}
	}

	return errors.Wrap(tx.Commit(), "commit")
}

func (e postgresEngine) build(obj jwalk.ObjectWalker) (commands, error) {
	cmds := make(commands, 0)

	if err := obj.Walk(func(table string, value interface{}) error {
		if v, ok := value.(jwalk.ObjectsWalker); ok {
			if err := v.Walk(func(obj jwalk.ObjectWalker) error {
				values := make([]interface{}, 0)
				insert := fmt.Sprintf("INSERT INTO %s (", table)
				valuesStr := "("

				first := true
				var i int
				if err := obj.Walk(func(field string, value interface{}) error {
					if v, ok := value.(jwalk.Value); ok {
						values = append(values, v.Interface())

						if !first {
							insert = insert + ", "
							valuesStr = valuesStr + ", "
						}

						insert = insert + field
						valuesStr = valuesStr + fmt.Sprintf("$%d", i+1)
					}

					if first {
						first = false
					}
					i++
					return nil
				}); err != nil {
					return err
				}

				insert = insert + ") VALUES " + valuesStr + ");"
				cmds = append(cmds, command{insert, values})
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}

	return cmds, nil
}
