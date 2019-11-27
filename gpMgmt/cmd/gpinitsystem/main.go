package main

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"golang.org/x/xerrors"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "usage: %s config_file\n", os.Args[0])
		os.Exit(1)
	}

	conf := os.Args[1]

	encoding, err := readStringParam(conf, "ENCODING")
	if err != nil {
		log.Fatalf("reading %q: %+v", conf, err)
	}

	masterDesc, err := readStringParam(conf, "QD_PRIMARY_ARRAY")
	if err != nil {
		log.Fatalf("reading %q: %+v", conf, err)
	}

	segDescs, err := readArrayParam(conf, "PRIMARY_ARRAY")
	if err != nil {
		log.Fatalf("reading %q: %+v", conf, err)
	}

	master, err := ParseLine(masterDesc)
	if err != nil {
		log.Fatalf("parsing %q: %+v", masterDesc, err)
	}

	var segments []*Segment
	segments = append(segments, master)
	for _, d := range segDescs {
		s, err := ParseLine(d)
		if err != nil {
			log.Fatalf("parsing %q: %+v", d, err)
		}

		segments = append(segments, s)
	}

	var wg sync.WaitGroup
	errs := make(chan error, len(segments))

	for i := range segments {
		segment := segments[i]
		args := []string{
			"-D", segment.DataDir,
			"--encoding", encoding,
			"--nosync",
			//"--max_connections", "200",
			//"--shared_buffers", "128MB",
			//fmt.Sprintf("--backend_output=%s.initdb", segment.DataDir),
		}

		wg.Add(1)
		go func() {
			defer wg.Done()

			var err error
			defer func() {
				if err != nil {
					errs <- xerrors.Errorf("initdb %s: %w", segment.DataDir, err)
				}
			}()

			err = initdb(args...)
			if err != nil {
				return
			}

			// Write internal configuration.
			conf := filepath.Join(segment.DataDir, "internal.auto.conf")
			contents := new(bytes.Buffer)

			fmt.Fprintf(contents, "gp_dbid = %d\n", segment.DBID)

			err = ioutil.WriteFile(conf, contents.Bytes(), 0644)

			// Write to postgresql.conf.
			conf = filepath.Join(segment.DataDir, "postgresql.conf")
			writer, err := os.OpenFile(conf, os.O_APPEND|os.O_WRONLY, 0644)
			if err != nil {
				return
			}
			defer writer.Close()

			fmt.Fprintln(writer)
			fmt.Fprintf(writer, "port = %d\n", segment.Port)
			fmt.Fprintf(writer, "gp_contentid = %d\n", segment.ContentID)
			fmt.Fprintln(writer, "fsync = off")
			fmt.Fprintln(writer, "listen_addresses = '*'")
		}()
	}

	wg.Wait()
	close(errs)

	var exit bool
	for err := range errs {
		log.Printf("Error: %+v", err)
		exit = true
	}
	if exit {
		os.Exit(1)
	}

	// Now write gp_segment_configuration.
	input := new(bytes.Buffer)

	fmt.Fprintln(input, "INSERT INTO gp_segment_configuration VALUES")
	for i, seg := range segments {
		if i > 0 {
			fmt.Fprintln(input, ",")
		}

		// backend> select * from gp_segment_configuration
		//	 1: dbid	(typeid = 21, len = 2, typmod = -1, byval = t)
		//	 2: content	(typeid = 21, len = 2, typmod = -1, byval = t)
		//	 3: role	(typeid = 18, len = 1, typmod = -1, byval = t)
		//	 4: preferred_role	(typeid = 18, len = 1, typmod = -1, byval = t)
		//	 5: mode	(typeid = 18, len = 1, typmod = -1, byval = t)
		//	 6: status	(typeid = 18, len = 1, typmod = -1, byval = t)
		//	 7: port	(typeid = 23, len = 4, typmod = -1, byval = t)
		//	 8: hostname	(typeid = 25, len = -1, typmod = -1, byval = f)
		//	 9: address	(typeid = 25, len = -1, typmod = -1, byval = f)
		//	10: datadir	(typeid = 25, len = -1, typmod = -1, byval = f)
		fmt.Fprintf(input, "(%d, %d, 'p', 'p', 'n', 'u', %d, '%s', '%s', '%s')",
			seg.DBID,
			seg.ContentID,
			seg.Port,
			seg.Host,
			seg.Host,
			seg.DataDir,
		)
	}
	fmt.Fprintln(input)

	err = singleMode(master.DataDir, input)
	if err != nil {
		log.Fatalf("writing master config: %+v", err)
	}
}

type Segment struct {
	Host      string
	Port      int
	DataDir   string
	DBID      int
	ContentID int
}

var ErrSyntax = errors.New("bad syntax")

// TODO: rename this something like ParseSegmentDescriptor? Or make it a
// constructor for Segment
func ParseLine(line string) (*Segment, error) {
	parts := strings.Split(line, "~")
	if len(parts) < 5 {
		return nil, xerrors.Errorf("line %q: %w", ErrSyntax)
	}

	var err error
	seg := &Segment{
		Host:    parts[0],
		DataDir: parts[2],
	}

	seg.Port, err = strconv.Atoi(parts[1])
	if err != nil {
		return nil, xerrors.Errorf("parsing port: %w", err)
	}

	seg.DBID, err = strconv.Atoi(parts[3])
	if err != nil {
		return nil, xerrors.Errorf("parsing dbid: %w", err)
	}

	seg.ContentID, err = strconv.Atoi(parts[4])
	if err != nil {
		return nil, xerrors.Errorf("parsing content: %w", err)
	}

	return seg, nil
}

func readStringParam(path string, key string) (string, error) {
	cmd := exec.Command("bash", "-c",
		fmt.Sprintf("source %q && echo ${%s}", path, key))

	output, err := cmd.Output()
	if err != nil {
		return "", err
	}

	return strings.Trim(string(output), "\n"), nil
}

func readArrayParam(path string, key string) ([]string, error) {
	cmd := exec.Command("bash", "-c",
		fmt.Sprintf(`source %q && for x in "${%s[@]}"; do echo $x; done`, path, key))

	output, err := cmd.Output()
	if err != nil {
		return nil, err
	}

	value := string(output)
	lines := strings.FieldsFunc(value, func(c rune) bool { return c == '\n' })
	return lines, nil
}

// TODO: run via SSH
func initdb(args ...string) error {
	cmd := exec.Command("initdb", args...)

	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	return cmd.Run()
}

func singleMode(datadir string, stdin io.Reader) error {
	cmd := exec.Command("postgres",
		"--single",
		"-D", datadir,
		"-E", // echo stdin
		"-j", // EOF terminates command
		"postgres",
	)

	cmd.Stdin = stdin
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	return cmd.Run()
}
