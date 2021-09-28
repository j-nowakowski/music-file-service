package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"music-file-service/src/artist"
	"music-file-service/src/artistToGenre"
	"music-file-service/src/file"
	"music-file-service/src/genre"
	"os"
	"time"

	_ "embed"

	_ "github.com/go-sql-driver/mysql"
)

const (
	artistFileLoc      string = "/tmp/input/artist"
	genreFileLoc       string = "/tmp/input/genre"
	genreArtistFileLoc string = "/tmp/input/genre_artist"
	dbUsernameEnvVar   string = "MYSQL_USER"
	dbPasswordEnvVar   string = "MYSQL_PASSWORD"
	dbHostEnvVar       string = "MYSQL_HOST"
)

//go:embed sql/create_artists.sql
var createArtistsSQL string

//go:embed sql/create_genres.sql
var createGenresSQL string

//go:embed sql/create_artists_to_genres.sql
var createArtistsToGenresSQL string

func main() {
	// Bootstrap the DB.
	db, err := initializeDbConn()
	if err != nil {
		panic(fmt.Errorf("in initializeDbConn: %w", err))
	}
	defer db.Close()
	err = initializeDbSchema(db)
	if err != nil {
		panic(fmt.Errorf("in initializeDbSchema: %w", err))
	}

	// Parse the input files.
	err = parseArtistFile(db)
	if err != nil {
		panic(fmt.Errorf("in parseArtistFile: %w", err))
	}
	err = parseGenreFile(db)
	if err != nil {
		panic(fmt.Errorf("in parseGenreFile: %w", err))
	}
	err = parseGenreArtistFile(db)
	if err != nil {
		panic(fmt.Errorf("in parseGenreArtistFile: %w", err))
	}
}

// Establishes a connection to the database.
func initializeDbConn() (*sql.DB, error) {
	username := os.Getenv(dbUsernameEnvVar)
	password := os.Getenv(dbPasswordEnvVar)
	host := os.Getenv(dbHostEnvVar)
	db, err := sql.Open("mysql", fmt.Sprintf("%v:%v@tcp(%v)/?parseTime=true", username, password, host))
	time.Sleep(15 * time.Second) // This is to wait for MySQL to initialize.
	return db, err
}

type Printer struct{}

func (p *Printer) Write(msg []byte) (int, error) {
	fmt.Println(string(msg))
	return 0, nil
}

// Creates the schema for database.
//
// For the sake of this example project, no volume is being used to store the DB outside of the container, so the DB
// will be destroyed every time the container resets, hence the need for the schema to be initialized here.
// If this function existed in a real application, it would need to be written idempotently.
func initializeDbSchema(db *sql.DB) error {
	_, err := db.Exec("CREATE DATABASE `music`;")
	if err != nil {
		return fmt.Errorf("while creating database: %v", err)
	}

	_, err = db.Exec("USE `music`;")
	if err != nil {
		return fmt.Errorf("while using database: %v", err)
	}

	_, err = db.Exec(createArtistsSQL)
	if err != nil {
		return fmt.Errorf("while creating artists table: %v", err)
	}

	_, err = db.Exec(createGenresSQL)
	if err != nil {
		return fmt.Errorf("while creating genres table: %v", err)
	}

	_, err = db.Exec(createArtistsToGenresSQL)
	if err != nil {
		return fmt.Errorf("while creating artists_to_genres table: %v", err)
	}

	return nil
}

func parseArtistFile(db *sql.DB) error {
	fmt.Println("Parsing artist file.")
	artistsFile, err := os.Open(artistFileLoc)
	if err != nil {
		return fmt.Errorf("while opening artist file: %w", err)
	}
	defer artistsFile.Close()
	artistsReader := bufio.NewReader(artistsFile)
	artistProcessor := &file.Processor{
		Parser: &artist.Parser{
			DB:        db,
			BatchSize: 5000,
		},
		Log:        &Printer{},
		SkipPrefix: "#",
	}
	err = artistProcessor.Process(artistsReader)
	if err != nil {
		return fmt.Errorf("in *file.Processor.Parse: %w", err)
	}
	return nil
}

func parseGenreFile(db *sql.DB) error {
	fmt.Println("Parsing genre file.")
	genresFile, err := os.Open(genreFileLoc)
	if err != nil {
		return fmt.Errorf("while opening genre file: %w", err)
	}
	defer genresFile.Close()
	genresReader := bufio.NewReader(genresFile)
	genreProcessor := &file.Processor{
		Parser: &genre.Parser{
			DB:        db,
			BatchSize: 5000,
		},
		Log:        &Printer{},
		SkipPrefix: "#",
	}
	err = genreProcessor.Process(genresReader)
	if err != nil {
		return fmt.Errorf("in genreProcessor.Parse: %w", err)
	}
	return nil
}

func parseGenreArtistFile(db *sql.DB) error {
	fmt.Println("Parsing genre_artist file.")
	artistsToGenresFile, err := os.Open(genreArtistFileLoc)
	if err != nil {
		return fmt.Errorf("while opening genre_artist file: %w", err)
	}
	defer artistsToGenresFile.Close()
	artistsToGenresReader := bufio.NewReader(artistsToGenresFile)
	artistToGenreProcessor := &file.Processor{
		Parser: &artistToGenre.Parser{
			DB:        db,
			BatchSize: 5000,
		},
		Log:        &Printer{},
		SkipPrefix: "#",
	}
	err = artistToGenreProcessor.Process(artistsToGenresReader)
	if err != nil {
		return fmt.Errorf("in artistParser.Parse: %w", err)
	}
	return nil
}
