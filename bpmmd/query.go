package bpmmd

type Query struct {
	Where   Predicate `json:"where"`
	OrderBy []string  `json:"order_by"`
	Limit   uint      `json:"limit"`
}

type And []Predicate

func (p *And) Matches(s LabelSet) bool {
	for _, p2 := range *p {
		if !p2.Matches(s) {
			return false
		}
	}
	return true
}

type Or []Predicate

func (p *Or) Matches(s LabelSet) bool {
	for _, p2 := range *p {
		if p2.Matches(s) {
			return true
		}
	}
	return false
}

type Range struct {
	Key  string
	Gteq *string
	Lteq *string
}

func (p *Range) Matches(s LabelSet) bool {
	v, ok := s[p.Key]
	if !ok {
		return false
	}
	if p.Gteq != nil {
		if !(v >= *p.Gteq) {
			return false
		}
	}
	if p.Lteq != nil {
		if !(v <= *p.Lteq) {
			return false
		}
	}
	return true
}

type Predicate struct {
	And   *And   `json:"and,omitempty"`
	Or    *Or    `json:"or,omitempty"`
	Range *Range `json:"range,omitempty"`
}

func (p Predicate) Matches(s LabelSet) bool {
	switch {
	case p.And != nil:
		return p.And.Matches(s)
	case p.Or != nil:
		return p.Or.Matches(s)
	case p.Range != nil:
		return p.Range.Matches(s)
	default:
		return true // zero value is inclusive
	}
}
