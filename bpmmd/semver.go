package bpmmd

import (
	"errors"
	"strconv"
	"strings"

	"golang.org/x/mod/semver"
)

type SemVer struct {
	Major uint
	Minor uint
	Patch string
}

func (sv *SemVer) String() string {
	return strings.Join([]string{
		strconv.Itoa(int(sv.Major)),
		strconv.Itoa(int(sv.Minor)),
		sv.Patch,
	}, ".")
}

func ParseSemVer(x string) (*SemVer, error) {
	if !semver.IsValid(x) {
		return nil, errors.New("invalid semver")
	}
	x = semver.Canonical(x)
	parts := strings.SplitN(x, ".", 3)
	maj, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		return nil, err
	}
	min, err := strconv.ParseUint(parts[1], 10, 64)
	if err != nil {
		return nil, err
	}
	return &SemVer{
		Major: uint(maj),
		Minor: uint(min),
		Patch: parts[2],
	}, nil
}
