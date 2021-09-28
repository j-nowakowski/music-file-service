INSERT INTO `artists_to_genres`(`artist_id`, `genre_id`, `is_primary`, `export_date`)
VALUES :values
ON DUPLICATE KEY UPDATE 
    `is_primary` = VALUES(`is_primary`), 
    `export_date` = VALUES(`export_date`);

