package polluter

import (
	"database/sql"
	"fmt"

	"github.com/pkg/errors"
)

type postgresEngine struct {
	db *sql.DB
}

func (e postgresEngine) exec(cmds []command) error {
	tx, err := e.db.Begin()
	if err != nil {
		errors.Wrap(err, "tx begin")
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

func (e postgresEngine) build(colls collections) commands {
	cmds := make(commands, 0)
	for _, col := range colls {
		for _, record := range col.records {
			values := make([]interface{}, len(record))
			insert := fmt.Sprintf("INSERT INTO %s (", col.name)
			valuesStr := "("

			for i, field := range record {
				values[i] = field.val

				insert = insert + field.name
				valuesStr = valuesStr + fmt.Sprintf("$%d", i+1)

				if i+1 != len(record) {
					insert = insert + ", "
					valuesStr = valuesStr + ", "
				}
			}

			insert = insert + ") VALUES " + valuesStr + ");"
			cmds = append(cmds, command{insert, values})
		}
	}

	return cmds
}
