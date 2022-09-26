package polluter

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	txdb "github.com/DATA-DOG/go-txdb"
	_ "github.com/lib/pq"
	"github.com/ory/dockertest"
	"github.com/pkg/errors"
)

const (
	dockerStartWait = 600 * time.Second
)

func TestMain(m *testing.M) {
	flag.Parse()

	if testing.Short() {
		os.Exit(m.Run())
	}

	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("could not connect to docker: %s\n", err)
	}

	p, err := newPG(pool)
	if err != nil {
		log.Fatalf("prepare pg with docker: %v\n", err)
	}

	txdb.Register("pgsqltx", "postgres", fmt.Sprintf("password=test user=test dbname=test host=localhost port=%s sslmode=disable", p.Resource.GetPort("5432/tcp")))

	code := m.Run()

	if err := pool.Purge(p.Resource); err != nil {
		log.Fatalf("could not purge postgres docker: %v\n", err)
	}

	os.Exit(code)

}

type pgDocker struct {
	Resource *dockertest.Resource
}

const pgSchema = `
CREATE TABLE IF NOT EXISTS users (
	id integer NOT NULL, 
	name varchar(255) NOT NULL
);
CREATE TABLE IF NOT EXISTS "all" (
	"group" varchar(255) NOT NULL
);
`

func newPG(pool *dockertest.Pool) (*pgDocker, error) {
	res, err := pool.Run("postgres", "latest", []string{
		"POSTGRES_PASSWORD=test",
		"POSTGRES_USER=test",
		"POSTGRES_DB=test",
	})
	if err != nil {
		return nil, errors.Wrap(err, "start postgres")
	}

	purge := func() {
		pool.Purge(res)
	}

	errChan := make(chan error)
	done := make(chan struct{})

	var db *sql.DB

	go func() {
		if err := pool.Retry(func() error {
			db, err = sql.Open("postgres", fmt.Sprintf("user=test password=test dbname=test host=localhost port=%s sslmode=disable", res.GetPort("5432/tcp")))
			if err != nil {
				return err
			}
			return db.Ping()
		}); err != nil {
			errChan <- err
		}

		close(done)
	}()

	select {
	case err := <-errChan:
		purge()
		return nil, errors.Wrap(err, "check connection")
	case <-time.After(dockerStartWait):
		purge()
		return nil, errors.New("timeout on checking postgres connection")
	case <-done:
		close(errChan)
	}

	defer db.Close()
	if _, err := db.Exec(pgSchema); err != nil {
		return nil, errors.Wrap(err, "failed to create schema")
	}

	pg := pgDocker{
		Resource: res,
	}

	return &pg, nil
}
