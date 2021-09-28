SELECT `ar`.`id`, `ar`.`name`, `ge`.`name`, `axg`.`is_primary`
FROM `artists` `ar`
INNER JOIN `artists_to_genres` `axg` 
    ON `axg`.`artist_id` = `ar`.`id`
INNER JOIN `genres` `ge` 
    ON `ge`.`id` = `axg`.`genre_id`
LIMIT 10;
