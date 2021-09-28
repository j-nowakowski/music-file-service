package genre

import (
	"database/sql"
	_ "embed"
	"fmt"
	"music-file-service/src/runeHelper"
	"strconv"
	"strings"
)

const (
	tokenDelimiter      rune = 1
	rowDelimiter        rune = 2
	expectedNumOfTokens int  = 4
)

type Parser struct {
	DB interface {
		Exec(query string, args ...interface{}) (sql.Result, error)
	}
	BatchSize int
	buffer    []genreRecord
}

func (p *Parser) ParseToBuffer(lineBytes []byte) error {
	// Parse genre out of input.
	genreRecord, err := p.parseGenre(lineBytes)
	if err != nil {
		return fmt.Errorf("in Parser.parseGenre: %v", err)
	}

	// Initialize buffer and store the record.
	err = p.initializeBuffer()
	if err != nil {
		return fmt.Errorf("in Parser.initializeBuffer: %v", err)
	}
	p.buffer = append(p.buffer, genreRecord)

	return nil
}

func (p *Parser) parseGenre(lineBytes []byte) (genreRecord, error) {
	// Parse out the tokens from the line.
	line := string(lineBytes)
	tokens := runeHelper.Split(line, tokenDelimiter, expectedNumOfTokens)
	if len(tokens) != expectedNumOfTokens {
		return genreRecord{}, fmt.Errorf("while parsing `%v`, expected %v tokens, instead counted %v",
			line, expectedNumOfTokens, len(tokens))
	}

	// Parse exportDate (token #0)
	exportDate, err := strconv.ParseInt(tokens[0], 10, 64)
	if err != nil {
		return genreRecord{}, fmt.Errorf("while parsing export_date (%v) out of `%v`: %w", tokens[0], line, err)
	}

	// Parse id (token #1)
	id, err := strconv.ParseInt(tokens[1], 10, 64)
	if err != nil {
		return genreRecord{}, fmt.Errorf("while parsing id (%v) out of `%v`: %w", tokens[1], line, err)
	}

	// Parse parent ID (token #2). Interpret empty string as null.
	parentIDRaw := tokens[2]
	var parentID sql.NullInt64
	if parentIDRaw == "" {
		parentID = sql.NullInt64{
			Valid: false,
		}
	} else {
		parentIDInt, err := strconv.ParseInt(parentIDRaw, 10, 64)
		if err != nil {
			return genreRecord{}, fmt.Errorf("while parsing parent_id (%v) out of `%v`: %w", parentIDRaw, line, err)
		}
		parentID = sql.NullInt64{
			Int64: parentIDInt,
			Valid: true,
		}
	}

	// Parse name (token #3). Strip the UTF-8 byte 02 from the end.
	name := runeHelper.RemoveSuffix(tokens[3], rowDelimiter)

	return genreRecord{
		ID:         int(id),
		ParentID:   parentID,
		Name:       name,
		ExportDate: int(exportDate),
	}, nil
}

//go:embed sql/insert_genres.sql
var insertGenresSQL string

const (
	insertValuesTemplate  string = "(?,?,?,?)"
	insertValuesDelimiter string = ","
)

// Initializes the buffer if it is currently nil. Otherwise, does nothing.
func (p *Parser) initializeBuffer() error {
	if p.buffer != nil {
		return nil
	}
	if p.BatchSize <= 0 {
		return fmt.Errorf("parser batch size (%v) must be positive", p.BatchSize)
	}
	p.buffer = make([]genreRecord, 0, p.BatchSize)
	return nil
}

func (p *Parser) FlushBuffer(force bool) (wasFlushed bool, err error) {
	// Exit early?
	if len(p.buffer) < 1 {
		return false, nil
	}

	// Buffer not full?
	if len(p.buffer) < cap(p.buffer) && !force {
		return false, nil
	}

	// At this point, queue the buffer to empty no matter what.
	defer func() {
		p.buffer = p.buffer[:0]
	}()

	// Construct the SQL
	fullValuesTemplate := strings.Repeat(insertValuesTemplate+insertValuesDelimiter, len(p.buffer)-1) + insertValuesTemplate
	sql := strings.Replace(insertGenresSQL, ":values", fullValuesTemplate, 1)

	// Flatten the values
	values := make([]interface{}, 0, len(p.buffer)*6)
	for _, genre := range p.buffer {
		values = append(values, genre.ID, genre.ParentID, genre.Name, genre.ExportDate)
	}

	// Execute
	_, err = p.DB.Exec(sql, values...)
	if err != nil {
		fmt.Println(p.buffer)
		return true, err
	}
	return true, nil
}
