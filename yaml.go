package polluter

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"strconv"

	"github.com/pkg/errors"
	"github.com/romanyx/jwalk"
	yaml "gopkg.in/yaml.v2"
)

type yamlParser struct{}

func (p yamlParser) parse(r io.Reader) (jwalk.ObjectWalker, error) {
	data, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, errors.Wrap(err, "read from input")
	}

	j, err := yamlToJSON(data)
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

func yamlToJSON(data []byte) ([]byte, error) {
	mapSlice := yaml.MapSlice{}

	err := yaml.Unmarshal(data, &mapSlice)
	if err != nil {
		return nil, errors.Wrap(err, "unmarshal failed")
	}

	buf := new(bytes.Buffer)
	handleMapSlice(mapSlice, buf)

	return buf.Bytes(), nil
}

func handleMapSlice(mapSlice yaml.MapSlice, buf *bytes.Buffer) {
	buf.WriteString("{")
	first := true
	indent := ""
	for _, item := range mapSlice {
		buf.WriteString(indent + "\"" + item.Key.(string) + "\"" + ":")
		switch v := item.Value.(type) {
		case yaml.MapSlice:
			handleMapSlice(v, buf)
		case []interface{}:
			buf.WriteString("[")
			first := true
			indent := ""
			for _, i := range v {
				switch v := i.(type) {
				case yaml.MapSlice:
					buf.WriteString(indent)
					handleMapSlice(v, buf)
				default:
					buf.WriteString(indent + formatValue(v))
				}
				if first {
					first = false
					indent = ","
				}
			}
			buf.WriteString("]")
		default:
			buf.WriteString(formatValue(v))
		}
		if first {
			first = false
			indent = ","
		}

	}
	buf.WriteString("}")
}

func formatValue(typedYAMLObj interface{}) string {
	switch typedVal := typedYAMLObj.(type) {
	case string:
		return fmt.Sprintf("\"%s\"", typedVal)
	case int:
		return strconv.FormatInt(int64(typedVal), 10)
	case int64:
		return strconv.FormatInt(typedVal, 10)
	case float64:
		return strconv.FormatFloat(typedVal, 'g', -1, 32)
	case uint64:
		return strconv.FormatUint(typedVal, 10)
	case bool:
		if typedVal {
			return "true"
		}
		return "false"
	default:
		return "null"
	}

	return ""
}
