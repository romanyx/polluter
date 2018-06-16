package polluter

import (
	"encoding/json"
	"io"

	"github.com/mailru/easyjson/jlexer"
	"github.com/pkg/errors"
)

type jsonParser struct{}

func (p jsonParser) parse(r io.Reader) (collections, error) {
	var colls collections

	if err := json.NewDecoder(r).Decode(&colls); err != nil {
		return nil, errors.Wrap(err, "decode input")
	}

	return colls, nil
}

func (cs *collections) UnmarshalJSON(data []byte) error {
	l := jlexer.Lexer{Data: data}
	cs.unmarshalEasyJSON(&l)
	return l.Error()
}

func (cs *collections) unmarshalEasyJSON(in *jlexer.Lexer) {
	isTopLevel := in.IsStart()
	if in.IsNull() {
		if isTopLevel {
			in.Consumed()
		}
		in.Skip()
		return
	}
	in.Delim('{')
	for !in.IsDelim('}') {
		var c collection
		key := in.UnsafeString()
		in.WantColon()
		if in.IsNull() {
			in.Skip()
			in.WantComma()
			continue
		}
		c.name = key
		if in.IsNull() {
			in.Skip()
			c.records = nil
		} else {
			in.Delim('[')
			if c.records == nil {
				if !in.IsDelim(']') {
					c.records = make([]record, 0, 2)
				} else {
					c.records = []record{}
				}
			} else {
				c.records = (c.records)[:0]
			}
			for !in.IsDelim(']') {
				var v1 record
				(v1).unmarshalEasyJSON(in)
				c.records = append(c.records, v1)
				in.WantComma()
			}
			in.Delim(']')
		}
		*cs = append(*cs, c)
		in.WantComma()
	}
	in.Delim('}')
	if isTopLevel {
		in.Consumed()
	}
}

func (r *record) unmarshalEasyJSON(in *jlexer.Lexer) {
	isTopLevel := in.IsStart()
	if in.IsNull() {
		if isTopLevel {
			in.Consumed()
		}
		in.Skip()
		return
	}
	in.Delim('{')
	for !in.IsDelim('}') {
		key := in.UnsafeString()
		in.WantColon()
		if in.IsNull() {
			in.Skip()
			in.WantComma()
			continue
		}

		*r = append(*r, field{key, in.Interface()})
		in.WantComma()
	}
	in.Delim('}')
	if isTopLevel {
		in.Consumed()
	}
}
