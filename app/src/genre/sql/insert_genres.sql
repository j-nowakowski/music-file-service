INSERT INTO `genres`(`id`, `parent_id`, `name`, `export_date`)
VALUES :values
ON DUPLICATE KEY UPDATE 
    `parent_id` = VALUES(`parent_id`), 
    `name` = VALUES(`name`), 
    `export_date` = VALUES(`export_date`);

