CREATE TABLE `artists_to_genres` (
    `artist_id` BIGINT NOT NULL,
    `genre_id` INTEGER NOT NULL,
    `is_primary` BOOLEAN NOT NULL,
    `export_date` BIGINT NOT NULL,
    PRIMARY KEY (`artist_id`, `genre_id`),
    UNIQUE INDEX (`genre_id`, `artist_id`)
) ENGINE=InnoDB
