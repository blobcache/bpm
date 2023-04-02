// Package bpmmd contains types for dealing with metadata
package bpmmd

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/brendoncarroll/go-tai64"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
)

type Label struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func (l Label) MarshalJSON() ([]byte, error) {
	return json.Marshal([2]string{l.Key, l.Value})
}

func (l *Label) UnmarshalJSON(data []byte) error {
	var x [2]string
	if err := json.Unmarshal(data, &x); err != nil {
		return err
	}
	l.Key = x[0]
	l.Value = x[1]
	return nil
}

func (l Label) TAI64() (tai64.TAI64, error) {
	n, err := strconv.ParseUint(l.Value, 10, 63)
	if err != nil {
		return 0, err
	}
	return tai64.TAI64(n), nil
}

func (l Label) Uint64() (uint64, error) {
	return strconv.ParseUint(l.Value, 10, 64)
}

func (l Label) SemVer() (*SemVer, error) {
	return ParseSemVer(l.Value)
}

func (l Label) String() string {
	return fmt.Sprintf("(%s: %s)", l.Key, l.Value)
}

type LabelSet map[string]string

func (s LabelSet) Get(k string) Label {
	return Label{Key: k, Value: s[k]}
}

func (s LabelSet) Put(x Label) {
	s[x.Key] = s[x.Value]
}

func (s LabelSet) String() string {
	sb := strings.Builder{}
	sb.WriteString("{")
	ks := maps.Keys(s)
	slices.Sort(ks)
	for i, k := range ks {
		if i > 0 {
			sb.WriteString(", ")
		}
		sb.WriteString(s.Get(k).String())
	}
	sb.WriteString("}")
	return sb.String()
}
