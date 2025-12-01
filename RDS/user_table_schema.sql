CREATE TABLE `user` (
  `no` int NOT NULL AUTO_INCREMENT,
  `nickname` varchar(45) NOT NULL,
  `uuid` char(36) NOT NULL,
  `gender` varchar(1) NOT NULL,
  `created_at` timestamp NULL DEFAULT NULL,
  `fcm_token` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`no`),
  UNIQUE KEY `uuid_UNIQUE` (`uuid`)
) ENGINE=InnoDB AUTO_INCREMENT=505 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;