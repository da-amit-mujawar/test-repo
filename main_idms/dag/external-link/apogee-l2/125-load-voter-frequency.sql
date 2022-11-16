--ADD VC Job: L2 : DOWNLOAD L2 VOTER FREQUENCY here
DROP TABLE IF EXISTS l2_votefrequency_new;

CREATE TABLE l2_votefrequency_new(
    lalvoterid varchar(25),
    vote_frequency varchar(1) );


COPY l2_votefrequency_new
FROM 's3://{s3-key2}/{yyyy}{mm}22_NW_VoteFrequency.csv'
IAM_ROLE '{iam}'
DELIMITER ','
IGNOREHEADER 1;


UPDATE {tablename1}
   SET  l2_vote_frequency = vote_frequency
  FROM {tablename1} e
       INNER JOIN l2_votefrequency_new f on e.l2_lalvoterid = f.lalvoterid;


