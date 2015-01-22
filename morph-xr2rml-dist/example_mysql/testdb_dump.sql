SET SQL_MODE="NO_AUTO_VALUE_ON_ZERO";
SET time_zone = "+00:00";

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8 */;

--
-- Database: `test`
--

-- --------------------------------------------------------

--
-- Structure of table `sport`
--

DROP TABLE IF EXISTS `sport`;
CREATE TABLE IF NOT EXISTS `sport` (
  `id` int(11) NOT NULL DEFAULT '0',
  `name` varchar(50) DEFAULT NULL,
  `code` varchar(50) NOT NULL,
  `shirt_colors` char(255) DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

--
-- Content of table `sport`
--

INSERT INTO `sport` (`id`, `name`, `code`, `shirt_colors`) VALUES
(0, 'BasketBall', 'B, BSK, BK', '{"color1": "black","color2": "grey"}'),
(100, 'Tennis', 'T, TN,TNS', ' ["blue", "red", "brown"]'),
(200, 'Chess', 'CHS', ' {"color": "yellow"}'),
(300, 'Scuba diving', 'SCB,DIV', NULL),
(400, 'Free diving', 'FD,APNEA', NULL),
(500, 'Pool', 'P,PL,POO', '{"color": "grey", "color": "orange"}'),
(600, 'Soccer', 'SOC,SC', '{"color": "dark blue"}');

-- --------------------------------------------------------

--
-- Structure of table `student`
--

DROP TABLE IF EXISTS `student`;
CREATE TABLE IF NOT EXISTS `student` (
  `id` char(8) NOT NULL DEFAULT '0',
  `comments` text NOT NULL,
  `name` varchar(255) CHARACTER SET utf8 COLLATE utf8_roman_ci DEFAULT NULL,
  `sport` int(11) DEFAULT NULL,
  `webpage` varchar(100) DEFAULT NULL,
  `email` varchar(100) DEFAULT NULL,
  `birthdate` datetime DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `Sport` (`sport`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

--
-- Content of table `student`
--

INSERT INTO `student` (`id`, `comments`, `name`, `sport`, `webpage`, `email`, `birthdate`) VALUES
('B1', '[{"hobby": "Swimming","place": "sea"},{"hobby": "Soccer","place": "street"}]', '<?xml version="1.0"?>\n<FullName>\n<FirstNames><FirstName>Paul</FirstName><FirstName>Jack</FirstName></FirstNames>\n<LastName>Smith</LastName>\n</FullName>', 0, NULL, NULL, '2000-12-31 00:00:00'),
('B2', '[{"hobby": "Hiking","place": "Mountain"},{"hobby": "Basketball","place": "street"},{"sportInClub": [100, 200, 300]}]', '<FullName>\n<FirstNames><FirstName>John</FirstName><FirstName>Fitzgerald</FirstName></FirstNames>\n<LastName>Kennedy</LastName>\n</FullName>\n', 100, NULL, 'john@acd.edu', NULL),
('B3', '', '<FullName>\n<FirstNames><FirstName>Mike</FirstName></FirstNames>\n<LastName>Peterson</LastName>\n</FullName>\n', NULL, 'www.george.edu', NULL, '1990-06-18 00:00:00'),
('B4', '[{"sportInClub": [400, 500, 600]}]', NULL, 600, 'www.starr.edu', 'ringo@acd.edu', NULL);

--
-- Contraints on table `student`
--
ALTER TABLE `student`
  ADD CONSTRAINT `Student_ibfk_1` FOREIGN KEY (`sport`) REFERENCES `sport` (`id`);

/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
