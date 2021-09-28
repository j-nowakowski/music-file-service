INSERT INTO `artists`(`id`, `name`, `artist_type_id`, `is_actual_artist`, `view_url`, `export_date`)
VALUES :values
ON DUPLICATE KEY UPDATE 
    `name` = VALUES(`name`), 
    `artist_type_id` = VALUES(`artist_type_id`), 
    `is_actual_artist` = VALUES(`is_actual_artist`), 
    `view_url` = VALUES(`view_url`), 
    `export_date` = VALUES(`export_date`);

