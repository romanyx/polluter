package polluter

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"io/ioutil"
	"log"
	"os"
	"testing"
	"time"

	txdb "github.com/DATA-DOG/go-txdb"
	"github.com/go-redis/redis"
	mysqlD "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	"github.com/ory/dockertest"
	"github.com/pkg/errors"
)

const (
	dockerStartWait = 60 * time.Second
)

var (
	redisAddr = ""
	mongoAddr = ""
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

	txdb.Register("pgsqltx", "postgres", fmt.Sprintf("password=test user=test dbname=test host=localhost port=%s sslmode=disable", p.Resource.GetPort("5432/tcp")))

	r, err := newRedis(pool)
	if err != nil {
		log.Fatalf("prepare redis with docker: %v\n", err)
	}

	mg, err := newMongo(pool)
	if err != nil {
		log.Fatalf("prepare mongo with docker: %v\n", err)
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
	if err := pool.Purge(mg.Resource); err != nil {
		log.Fatalf("could not purge mongo docker: %v\n", err)
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

type mongoDocker struct {
	Resource *dockertest.Resource
}

func newMongo(pool *dockertest.Pool) (*mongoDocker, error) {
	res, err := pool.Run("mongo", "latest", []string{
		"MONGO_INITDB_ROOT_USERNAME=test",
		"MONGO_INITDB_ROOT_PASSWORD=test",
	})
	if err != nil {
		return nil, errors.Wrap(err, "start mongo")
	}

	purge := func() {
		pool.Purge(res)
	}

	errChan := make(chan error)
	done := make(chan struct{})

	var client *mongo.Client

	go func() {
		if err := pool.Retry(func() error {
			mongoAddr = fmt.Sprintf("mongodb://test:test@localhost:%s/admin", res.GetPort("27017/tcp"))
			client, err = mongo.Connect(context.Background(), options.Client().ApplyURI(mongoAddr))
			if err != nil {
				return err
			}
			return client.Ping(context.Background(), readpref.Primary())
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
		return nil, errors.New("timeout on checking mongo connection")
	case <-done:
		close(errChan)
	}

	defer client.Disconnect(context.Background())

	mg := mongoDocker{
		Resource: res,
	}

	return &mg, nil
}
