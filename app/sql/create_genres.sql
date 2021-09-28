CREATE TABLE `genres` (
    `id` INTEGER NOT NULL,
    `parent_id` INTEGER,
    `name` NVARCHAR(200) NOT NULL,
    `export_date` BIGINT NOT NULL,
    PRIMARY KEY (`id`)
) ENGINE=InnoDB
