CREATE USER 'repl'@'%' IDENTIFIED BY 'Password123!';
GRANT REPLICATION SLAVE, REPLICATION CLIENT, SELECT ON *.* TO 'repl'@'%';

USE `srcdb`;
CREATE TABLE `srcdb`.`user` (
  `id` int PRIMARY KEY AUTO_INCREMENT,
  `name` varchar(20) NOT NULL,
  `phone` varchar(20) NULL,
  `age` int NULL,
  `born` TIMESTAMP NULL
);