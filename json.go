package polluter

import (
	"encoding/json"
	"io"
	"sort"

	"github.com/pkg/errors"
)

type jsonParser struct{}

func (p jsonParser) parse(r io.Reader) (collections, error) {
	var colls collections

	// TODO(romanyx): use faster approach for deconding.
	if err := json.NewDecoder(r).Decode(&colls); err != nil {
		return nil, errors.Wrap(err, "decode input")
	}

	return colls, nil
}

// UnmarshalJSON implements Unmarshaler interface.
// TODO(romanyx): improve algorithm, see benchmark.
func (c *collections) UnmarshalJSON(data []byte) error {
	var temp map[string][]map[string]interface{}
	if err := json.Unmarshal(data, &temp); err != nil {
		return err
	}
	names := make([]string, len(temp))
	var i int
	for name := range temp {
		names[i] = name
		i++
	}
	sort.Strings(names)
	res := make(collections, len(temp))
	for i, name := range names {
		records := temp[name]
		col := collection{
			name:    name,
			records: make([]record, len(records)),
		}
		for i, recRaw := range records {
			rec := make(record, len(recRaw))
			names := make([]string, len(recRaw))
			var y int
			for name := range recRaw {
				names[y] = name
				y++
			}
			sort.Strings(names)
			y = 0
			for _, name := range names {
				rec[y] = field{name, recRaw[name]}
				y++
			}
			col.records[i] = rec
		}
		res[i] = col
	}
	*c = res
	return nil
}
