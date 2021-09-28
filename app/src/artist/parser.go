package artist

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
	expectedNumOfTokens int  = 6
)

type Parser struct {
	DB interface {
		Exec(query string, args ...interface{}) (sql.Result, error)
	}
	BatchSize int
	buffer    []artistRecord
}

func (p *Parser) ParseToBuffer(lineBytes []byte) error {
	// Parse artist out of input.
	artistRecord, err := p.parseArtist(lineBytes)
	if err != nil {
		return fmt.Errorf("in Parser.parseArtist: %v", err)
	}

	// Initialize buffer and store the record.
	err = p.initializeBuffer()
	if err != nil {
		return fmt.Errorf("in Parser.initializeBuffer: %v", err)
	}
	p.buffer = append(p.buffer, artistRecord)

	return nil
}

func (p *Parser) parseArtist(lineBytes []byte) (artistRecord, error) {
	// Parse out the tokens from the line.
	line := string(lineBytes)
	tokens := runeHelper.Split(line, tokenDelimiter, expectedNumOfTokens)
	if len(tokens) != expectedNumOfTokens {
		return artistRecord{}, fmt.Errorf("while parsing `%v`, expected %v tokens, instead counted %v",
			line, expectedNumOfTokens, len(tokens))
	}

	// Parse exportDate (token #0)
	exportDate, err := strconv.ParseInt(tokens[0], 10, 64)
	if err != nil {
		return artistRecord{}, fmt.Errorf("while parsing export_date (%v) out of `%v`: %w", tokens[0], line, err)
	}

	// Parse id (token #1)
	id, err := strconv.ParseInt(tokens[1], 10, 64)
	if err != nil {
		return artistRecord{}, fmt.Errorf("while parsing id (%v) out of `%v`: %w", tokens[1], line, err)
	}

	// Parse name (token #2)
	name := tokens[2]

	// Parse id (token #3)
	isActualArtist, err := strconv.ParseBool(tokens[3])
	if err != nil {
		return artistRecord{}, fmt.Errorf("while parsing is_actual_artist (%v) out of `%v`: %w", tokens[3], line, err)
	}

	// Parse viewURL (token #4)
	viewURL := tokens[4]

	// Parse artistTypeID (token #5). Strip the UTF-8 byte 02 from the end.
	artistTypeIDRaw := tokens[5]
	artistTypeIDRaw = runeHelper.RemoveSuffix(artistTypeIDRaw, rowDelimiter)
	artistTypeID, err := strconv.ParseInt(artistTypeIDRaw, 10, 64)
	if err != nil {
		return artistRecord{}, fmt.Errorf("while parsing artist_type_id (%v) out of `%v`: %w", artistTypeIDRaw, line, err)
	}

	return artistRecord{
		ID:             int(id),
		Name:           name,
		ArtistTypeID:   int(artistTypeID),
		IsActualArtist: isActualArtist,
		ViewURL:        viewURL,
		ExportDate:     int(exportDate),
	}, nil
}

//go:embed sql/insert_artists.sql
var insertArtistsSQL string

const (
	insertValuesTemplate  string = "(?,?,?,?,?,?)"
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
	p.buffer = make([]artistRecord, 0, p.BatchSize)
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
	sql := strings.Replace(insertArtistsSQL, ":values", fullValuesTemplate, 1)

	// Flatten the values
	values := make([]interface{}, 0, len(p.buffer)*6)
	for _, artist := range p.buffer {
		values = append(values, artist.ID, artist.Name, artist.ArtistTypeID,
			artist.IsActualArtist, artist.ViewURL, artist.ExportDate)
	}

	// Execute
	_, err = p.DB.Exec(sql, values...)
	if err != nil {
		fmt.Println(p.buffer)
		return true, err
	}
	return true, nil
}
