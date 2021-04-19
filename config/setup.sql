CREATE TABLE `PostRelatedness` (
  `post1_id` bigint(20) NOT NULL,
  `post2_id` bigint(20) NOT NULL,
  `rel_txt_wt` double DEFAULT NULL,
  `rel_txt_meta` varchar(255) DEFAULT NULL,
  `sub_img_wt` double DEFAULT NULL,
  `sub_img_meta` varchar(255) DEFAULT NULL,
  `ocr_wt` double DEFAULT NULL,
  `ocr_meta` varchar(255) DEFAULT NULL,
  PRIMARY KEY (`post1_id`,`post2_id`)
);

CREATE TABLE `PostCentrality` (
  `post_id` bigint(20) NOT NULL,
  `score` double NOT NULL,
  `evaluated` datetime NOT NULL,
  PRIMARY KEY (`post_id`)
);
