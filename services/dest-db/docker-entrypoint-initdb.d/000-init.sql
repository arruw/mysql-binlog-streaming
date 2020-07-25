USE `destdb`;
CREATE TABLE `destdb`.`user` (
  `id` int NOT NULL,
  `name` varchar(20) NOT NULL,
  `phone` varchar(20) NULL,
  `age` int NULL,
  `born` TIMESTAMP NULL
);