package artistToGenre

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
	buffer    []artistToGenreRecord
}

func (p *Parser) ParseToBuffer(lineBytes []byte) error {
	// Parse artistToGenre out of input.
	artistToGenreRecord, err := p.parseArtistToGenre(lineBytes)
	if err != nil {
		return fmt.Errorf("in Parser.parseArtistToGenre: %v", err)
	}

	// Initialize buffer and store the record.
	err = p.initializeBuffer()
	if err != nil {
		return fmt.Errorf("in Parser.initializeBuffer: %v", err)
	}
	p.buffer = append(p.buffer, artistToGenreRecord)

	return nil
}

func (p *Parser) parseArtistToGenre(lineBytes []byte) (artistToGenreRecord, error) {
	// Parse out the tokens from the line.
	line := string(lineBytes)
	tokens := runeHelper.Split(line, tokenDelimiter, expectedNumOfTokens)
	if len(tokens) != expectedNumOfTokens {
		return artistToGenreRecord{}, fmt.Errorf("while parsing `%v`, expected %v tokens, instead counted %v",
			line, expectedNumOfTokens, len(tokens))
	}

	// Parse exportDate (token #0)
	exportDate, err := strconv.ParseInt(tokens[0], 10, 64)
	if err != nil {
		return artistToGenreRecord{}, fmt.Errorf("while parsing export_date (%v) out of `%v`: %w", tokens[0], line, err)
	}

	// Parse genre_id (token #1)
	genreID, err := strconv.ParseInt(tokens[1], 10, 64)
	if err != nil {
		return artistToGenreRecord{}, fmt.Errorf("while parsing genre_id (%v) out of `%v`: %w", tokens[1], line, err)
	}

	// Parse artist_id (token #2)
	artistID, err := strconv.ParseInt(tokens[2], 10, 64)
	if err != nil {
		return artistToGenreRecord{}, fmt.Errorf("while parsing artist_id (%v) out of `%v`: %w", tokens[2], line, err)
	}

	// Parse is_primary (token #3). Strip the UTF-8 byte 02 from the end.
	isPrimaryRaw := runeHelper.RemoveSuffix(tokens[3], rowDelimiter)
	isPrimary, err := strconv.ParseBool(isPrimaryRaw)
	if err != nil {
		return artistToGenreRecord{}, fmt.Errorf("while parsing is_primary (%v) out of `%v`: %w", tokens[3], line, err)
	}
	return artistToGenreRecord{
		ArtistID:   int(artistID),
		GenreID:    int(genreID),
		IsPrimary:  isPrimary,
		ExportDate: int(exportDate),
	}, nil
}

//go:embed sql/insert_artists_to_genres.sql
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
	p.buffer = make([]artistToGenreRecord, 0, p.BatchSize)
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
	for _, artistToGenre := range p.buffer {
		values = append(values, artistToGenre.ArtistID, artistToGenre.GenreID,
			artistToGenre.IsPrimary, artistToGenre.ExportDate)
	}

	// Execute
	_, err = p.DB.Exec(sql, values...)
	if err != nil {
		fmt.Println(p.buffer)
		return true, err
	}
	return true, nil
}
