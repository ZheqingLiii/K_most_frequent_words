--create database
create database load16;
use load16;

create table text32gb(line string);
create table text64gb(line string);

--store as rc files
SET hive.default.fileformat=Orc

-- load by lines
load data inpath 'data_32GB.txt' into table text32gb;
load data inpath 'data_64GB.txt' into table text64gb;

-- load by words into tables and store counts
create table word_count32 as select word, count(1) as count from (select explode(split(line, '\\s'))as word from text32gb) w group by word order by word;
create table word_count64 as select word, count(1) as count from (select explode(split(line, '\\s'))as word from text64gb) w group by word order by word;

-- top 100 frequent words
select * from word_count32 order by count desc limit 100;
select * from word_count64 order by count desc limit 100;
-- output into files
hive -e 'select * from word_count32 order by count desc limit 100' > /output32.tsv

-- top 100 frequents with length more than 3
select * from word_count32 where length(word)>3 order by count desc limit 100;
select * from word_count64 where length(word)>3 order by count desc limit 100;

-- drop tables and database
drop table word_count32;
drop table word_count64;
drop table text32gb;
drop table text64gb;
drop database load16;