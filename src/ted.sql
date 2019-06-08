use ted;

create table ted_main (
  comments int not null,
  description text not null,
  duration long not null,
  event text not null,
  film_date int not null ,
  languages int not null ,
  main_speaker text not null ,
  name text not null ,
  num_speaker int not null ,
  published_date int not null,
  ratings text not null ,
  related_talks text not null,
  speaker_occupation text not null,
  tags text not null ,
  title text not null,
  url text not null,
  views bigint not null
) engine=InnoDB DEFAULT CHARSET=utf8mb4;

create table transcripts (
  url text not null,
  transcript text not null
) engine=InnoDB DEFAULT CHARSET=utf8mb4;

load data local infile "/your-local-path/ted_main.csv"
into table ted_main
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
IGNORE 1 LINES;

load data local infile "/your-local-path/transcripts.csv"
into table transcripts
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '"'
IGNORE 1 LINES;
