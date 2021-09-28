package genre

import (
	"database/sql"
)

type genreRecord struct {
	ID         int
	ParentID   sql.NullInt64
	Name       string
	ExportDate int
}
