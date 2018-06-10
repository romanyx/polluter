package polluter

import (
	"io"
	"io/ioutil"

	"github.com/ghodss/yaml"
	"github.com/pkg/errors"
)

type yamlParser struct{}

func (p yamlParser) parse(r io.Reader) (collections, error) {
	data, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, errors.Wrap(err, "read from input")
	}

	var colls collections
	if err := yaml.Unmarshal(data, &colls); err != nil {
		return nil, errors.Wrap(err, "decode input")
	}

	return colls, nil
}
