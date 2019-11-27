package main_test

import (
	"reflect"
	"strconv"
	"testing"

	"golang.org/x/xerrors"

	gp "github.com/greenplum-db/gpdb/gpMgmt/bin/cmd/gpinitsystem"
)

func TestParseLine(t *testing.T) {
	cases := []struct {
		line     string
		expected *gp.Segment
	}{{
		"host1~6001~/tmp/seg-1~1~-1",
		&gp.Segment{
			Host:      "host1",
			Port:      6001,
			DataDir:   "/tmp/seg-1",
			DBID:      1,
			ContentID: -1,
		},
	}, {
		"host2~10002~/tmp/seg0~2~0~ignored~ignored",
		&gp.Segment{
			Host:      "host2",
			Port:      10002,
			DataDir:   "/tmp/seg0",
			DBID:      2,
			ContentID: 0,
		},
	}}

	for _, c := range cases {
		seg, err := gp.ParseLine(c.line)

		if err != nil {
			t.Errorf("ParseLine(%q) returned error %#v", c.line, err)
		}
		if !reflect.DeepEqual(seg, c.expected) {
			t.Errorf("ParseLine(%q)=%+v; want %+v", c.line, seg, c.expected)
		}
	}

	syntaxCases := []string{
		"host~10x2~/tmp~0~0~0",
		"host~1002~/tmp~x~0~0",
		"host~1002~/tmp~0~x~0",
	}

	for _, c := range syntaxCases {
		_, err := gp.ParseLine(c)

		var numErr *strconv.NumError
		if !xerrors.As(err, &numErr) {
			t.Errorf("ParseLine(%q) returned error %#v, want NumError", c, err)
		}

		if !xerrors.Is(numErr.Err, strconv.ErrSyntax) {
			t.Errorf("ParseLine(%q) returned error %#v, want ErrSyntax", c, err)
		}
	}

	lengthCases := []string{
		"host",
		"host~10~/tmp~0",
	}

	for _, c := range lengthCases {
		_, err := gp.ParseLine(c)

		if !xerrors.Is(err, gp.ErrSyntax) {
			t.Errorf("ParseLine(%q) returned error %#v, want ErrSyntax", c, err)
		}
	}
}
