package polluter

import (
	"io"
	"io/ioutil"

	"github.com/pkg/errors"
	"github.com/romanyx/jwalk"
)

type jsonParser struct{}

func (p jsonParser) parse(r io.Reader) (jwalk.ObjectWalker, error) {
	data, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read")
	}

	i, err := jwalk.Parse(data)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse")
	}

	obj, ok := i.(jwalk.ObjectWalker)
	if !ok {
		return nil, errors.New("unexpected format")
	}

	return obj, nil
}
