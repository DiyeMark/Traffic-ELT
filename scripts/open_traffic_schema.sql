CREATE TABLE IF NOT EXISTS `open_traffic`
(
    `track_id` INT NOT NULL,
    `type` TEXT DEFAULT NULL,
    `traveled_d` FLOAT DEFAULT NULL,
    `avg_speed` FLOAT DEFAULT NULL,
    `lat` FLOAT DEFAULT NULL,
    `lon` FLOAT DEFAULT NULL,
    `speed` FLOAT DEFAULT NULL,
    `lon_acc` FLOAT DEFAULT NULL,
    `lat_acc` FLOAT DEFAULT NULL,
    `time` FLOAT NULL DEFAULT NULL,
    PRIMARY KEY (`track_id`)

)
ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE utf8mb4_unicode_ci;
