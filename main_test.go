package polluter

import (
	"database/sql"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"testing"
	"time"

	txdb "github.com/DATA-DOG/go-txdb"
	"github.com/go-redis/redis"
	mysqlD "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v4/stdlib"
	"github.com/ory/dockertest"
	"github.com/pkg/errors"
)

const (
	dockerStartWait = 2 * 60 * time.Second
)

var (
	redisAddr = ""
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

	mysql, err := newMySQL(pool)
	if err != nil {
		log.Fatalf("prepare mysql with docker: %v\n", err)
	}

	txdb.Register("mysqltx", "mysql", fmt.Sprintf("test:test@tcp(localhost:%s)/test", mysql.Resource.GetPort("3306/tcp")))

	p, err := newPG(pool)
	if err != nil {
		log.Fatalf("prepare pg with docker: %v\n", err)
	}

	txdb.Register("pgsqltx", "pgx", fmt.Sprintf("postgres://test:test@localhost:%s/test?sslmode=disable", p.Resource.GetPort("5432/tcp")))

	r, err := newRedis(pool)
	if err != nil {
		log.Fatalf("prepare redis with docker: %v\n", err)
	}

	code := m.Run()

	if err := pool.Purge(mysql.Resource); err != nil {
		log.Fatalf("could not purge mysql docker: %v\n", err)
	}
	if err := pool.Purge(r.Resource); err != nil {
		log.Fatalf("could not purge redis docker: %v\n", err)
	}
	if err := pool.Purge(p.Resource); err != nil {
		log.Fatalf("could not purge postgres docker: %v\n", err)
	}

	os.Exit(code)

}

type mySQL struct {
	Resource *dockertest.Resource
}

const mysqlSchema = `
CREATE TABLE IF NOT EXISTS users (
	id integer NOT NULL,
	name varchar(255) NOT NULL
);
`

func newMySQL(pool *dockertest.Pool) (*mySQL, error) {
	res, err := pool.Run("mysql", "latest", []string{
		"MYSQL_ROOT_PASSWORD=qwerty",
		"MYSQL_USER=test",
		"MYSQL_PASSWORD=test",
		"MYSQL_DATABASE=test",
	})
	if err != nil {
		return nil, errors.Wrap(err, "start mysql")
	}

	purge := func() {
		pool.Purge(res)
	}

	errChan := make(chan error)
	done := make(chan struct{})

	mysqlD.SetLogger(log.New(ioutil.Discard, "", 0)) // mute mysql logger.

	var db *sql.DB

	go func() {
		if err := pool.Retry(func() error {
			db, err = sql.Open("mysql", fmt.Sprintf("test:test@(localhost:%s)/test", res.GetPort("3306/tcp")))
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
		return nil, errors.New("timeout on checking mysql connection")
	case <-done:
		close(errChan)
	}

	defer db.Close()
	if _, err := db.Exec(mysqlSchema); err != nil {
		return nil, errors.Wrap(err, "failed to create schema")
	}

	mysql := mySQL{
		Resource: res,
	}

	return &mysql, nil
}

type redisDocker struct {
	Resource *dockertest.Resource
}

func newRedis(pool *dockertest.Pool) (*redisDocker, error) {
	res, err := pool.Run("redis", "latest", nil)
	if err != nil {
		return nil, errors.Wrap(err, "start redis")
	}

	purge := func() {
		pool.Purge(res)
	}

	errChan := make(chan error)
	done := make(chan struct{})

	go func() {
		if err := pool.Retry(func() error {
			cli := redis.NewClient(&redis.Options{
				Addr: fmt.Sprintf("localhost:%s", res.GetPort("6379/tcp")),
				DB:   0,
			})
			defer cli.FlushDB()

			if _, err := cli.Ping().Result(); err != nil {
				return err
			}

			return nil
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
		return nil, errors.New("timeout on checking redis connection")
	case <-done:
		close(errChan)
	}

	redisAddr = fmt.Sprintf("localhost:%s", res.GetPort("6379/tcp"))

	r := redisDocker{
		Resource: res,
	}

	return &r, nil
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
			db, err = sql.Open("pgx", fmt.Sprintf("postgres://test:test@localhost:%s/test?sslmode=disable", res.GetPort("5432/tcp")))
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
