package file

import (
	_ "embed"
	"errors"
	"fmt"
	"io"
	"reflect"
)

type Processor struct {
	Parser interface {
		// Parses and stores the record in memory buffer.
		ParseToBuffer(lineBytes []byte) error
		// Applies the records in the memory buffer to memory if buffer is full. Always flushes if force = true.
		FlushBuffer(force bool) (wasFlushed bool, err error)
	}
	Log        io.Writer
	SkipPrefix string
}

type fileReadLiner interface {
	ReadLine() (line []byte, isPrefix bool, err error)
}

func (p *Processor) Process(input fileReadLiner) error {
	// Check input
	if input == nil || reflect.ValueOf(input).IsNil() {
		return errors.New("input cannot be nil")
	}

	i := 0
	batchNum := 0
	fmt.Printf("Processing batch #%v\n", batchNum)
	for {
		i++
		lineBytes, _, err := input.ReadLine()
		if err == io.EOF {
			break
		}
		if err != nil {
			p.log(fmt.Sprintf("error reading line #%v: %v\n", i, err.Error()))
			continue
		}
		if p.shouldSkipLine(lineBytes) {
			continue
		}
		err = p.Parser.ParseToBuffer(lineBytes)
		if err != nil {
			p.log(fmt.Sprintf("error parsing line #%v: %v\n", i, err.Error()))
			continue
		}
		wasFlushed, err := p.Parser.FlushBuffer(false)
		if err != nil {
			p.log(fmt.Sprintf("error in batch insert near line %v, %v\n", i, err.Error()))
			return err
		}
		if wasFlushed {
			batchNum++
			fmt.Printf("Processing batch #%v\n", batchNum)
		}
	}
	// Flush leftover records
	_, err := p.Parser.FlushBuffer(true)
	if err != nil {
		p.log(fmt.Sprintf("error in batch insert near line %v, %v\n", i, err.Error()))
	}

	return nil
}

// Writes the message to the parser's log if it's not nil. Otherwise, does nothing.
func (p *Processor) log(msg string) {
	if p.Log != nil && !reflect.ValueOf(p.Log).IsNil() {
		p.Log.Write([]byte(msg))
	}
}

// Determines if the line should be skipped for processing.
func (p *Processor) shouldSkipLine(lineBytes []byte) bool {
	// Skip empty lines.
	if len(lineBytes) <= 0 {
		return true
	}
	// Skip lines starting with SkipPrefix.
	if p.startsWithSkipPrefix(lineBytes) {
		return true
	}

	return false
}

// Determines if the line begins with the processor's SkipPrefix. If SkipPrefix is empty, this always returns false.
func (p *Processor) startsWithSkipPrefix(lineBytes []byte) bool {
	if p.SkipPrefix == "" {
		return false
	}
	lineRunes := []rune(string(lineBytes))
	prefixRunes := []rune(p.SkipPrefix)
	if len(prefixRunes) > len(lineRunes) {
		return false
	}
	for i, aRune := range prefixRunes {
		if aRune != lineRunes[i] {
			return false
		}
	}
	return true
}
