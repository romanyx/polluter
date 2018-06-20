[![GoDoc](https://godoc.org/github.com/romanyx/polluter?status.svg)](https://godoc.org/github.com/romanyx/polluter)
[![Go Report Card](https://goreportcard.com/badge/github.com/romanyx/polluter)](https://goreportcard.com/report/github.com/romanyx/polluter)
[![Build Status](https://travis-ci.org/romanyx/polluter.svg?branch=master)](https://travis-ci.org/romanyx/polluter)

# polluter

Mainly this package was created for testing purposes, to give the ability to seed a database with records from simple .yaml files. Polluter respects the order in files, so you can handle foreign_keys just by placing them in the right order.

## Usage

```go
package main

import "github.com/romanyx/polluter"

const input = `
roles:
- name: User
users:
- name: Roman
  role_id: 1
`

func TestX(t *testing.T) {
	db := prepareMySQL(t)
	defer db.Close()
	p := polluter.New(polluter.MySQLEngine(db))

	if err := p.Pollute(strings.NewReader(input)); err != nil {
		t.Fatalf("failed to pollute: %s", err)
	}

	....
}
```

## Examples

[See](https://github.com/romanyx/polluter/blob/master/polluter_test.go#L109) examples of usage with parallel testing.

## Testing

Make shure to start docker before testing.

```bash
make start
go test
```

## Supported databases

* MySQL
* Postgres
* Redis

## Contributing

Please feel free to submit issues, fork the repository and send pull requests!
