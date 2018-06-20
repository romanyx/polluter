package polluter

import (
	"io"
	"io/ioutil"

	"github.com/ghodss/yaml"
	"github.com/pkg/errors"
	"github.com/romanyx/jwalk"
)

type yamlParser struct{}

func (p yamlParser) parse(r io.Reader) (jwalk.ObjectWalker, error) {
	data, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, errors.Wrap(err, "read from input")
	}

	j, err := yaml.YAMLToJSON(data)
	if err != nil {
		return nil, errors.Wrap(err, "failed convert to json")
	}

	i, err := jwalk.Parse(j)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse")
	}

	obj, ok := i.(jwalk.ObjectWalker)
	if !ok {
		return nil, errors.New("unexpected format")
	}

	return obj, nil
}
