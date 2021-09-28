CREATE TABLE `artists` (
    `id` BIGINT NOT NULL,
    `name` NVARCHAR(1000) NOT NULL,
    `artist_type_id` INTEGER NOT NULL,
    `is_actual_artist` BOOLEAN NOT NULL,
    `view_url` NVARCHAR(1000) NOT NULL,
    `export_date` BIGINT NOT NULL,
    PRIMARY KEY (`id`)
) ENGINE=InnoDB
