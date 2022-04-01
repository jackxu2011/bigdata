create database jack;
use jack;

create external table t_user (
    user_id int,
    sex string,
    age tinyint,
    occupation string,
    zipcode string
)
row format serde 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe'
with serdeproperties('field.delim'='::')
LOCATION '/data/hive/users';

create external table t_rating (
    user_id int,
    movie_id int,
    rate tinyint,
    times bigint
)
row format serde 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe'
with serdeproperties('field.delim'='::')
LOCATION '/data/hive/ratings';

create external table t_movie (
    movie_id int,
    movie_name string,
    movie_type string
)
row format serde 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe'
with serdeproperties('field.delim'='::')
LOCATION '/data/hive/movies';

select age, avg(rate) avgrate from t_user u join t_rating r on u.user_id = r.user_id
group by age;

select movie_name, avg(rate) avgrate, count(*) total
from t_user u join t_rating r on u.user_id = r.user_id and u.sex = 'M'
join t_movie m on r.movie_id = m.movie_id
group by movie_name having count(*) > 50
order by avg(rate) desc
limit 10;

select movie_name, avg(rate) avgrate from
t_movie m left semi join (
    select user_id, movie_id, rate from t_rating a left semi join
    (
    select u.user_id, count(*) rates from t_rating r join t_user u on r.user_id = u.user_id and u.sex = 'F'
    group by u.user_id
    order by rates desc
    limit 1
    ) b on a.user_id = b.user_id
    order by rate desc
    limit 10
) tmp on m.movie_id = tmp.movie_id
    join t_rating r on m.movie_id = r.movie_id
group by m.movie_name;

