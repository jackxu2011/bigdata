# 创建表

```sql
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
```
## 作业1

```sql
select age, avg(rate) avgrate from t_user u join t_rating r on u.user_id = r.user_id
group by age;
```
* 结果
```
+------+---------------------+
| age  |       avgrate       |
+------+---------------------+
| 45   | 3.638061530735475   |
| 1    | 3.549520414538238   |
| 18   | 3.5075734460814227  |
| 25   | 3.5452350615336385  |
| 56   | 3.766632284682826   |
| 35   | 3.6181615352532375  |
| 50   | 3.714512346530556   |
+------+---------------------+
```

## 作业2

```sql
select movie_name, avg(rate) avgrate, count(*) total
from t_user u join t_rating r on u.user_id = r.user_id and u.sex = 'M'
join t_movie m on r.movie_id = m.movie_id
group by movie_name having count(*) > 50
order by avg(rate) desc
limit 10;
```
* 结果
```sql
+----------------------------------------------------+--------------------+--------+
|                     movie_name                     |      avgrate       | total  |
+----------------------------------------------------+--------------------+--------+
| Sanjuro (1962)                                     | 4.639344262295082  | 61     |
| Godfather, The (1972)                              | 4.583333333333333  | 1740   |
| Seven Samurai (The Magnificent Seven) (Shichinin no samurai) (1954) | 4.576628352490421  | 522    |
| Shawshank Redemption, The (1994)                   | 4.560625           | 1600   |
| Raiders of the Lost Ark (1981)                     | 4.520597322348094  | 1942   |
| Usual Suspects, The (1995)                         | 4.518248175182482  | 1370   |
| Star Wars: Episode IV - A New Hope (1977)          | 4.495307167235495  | 2344   |
| Schindler's List (1993)                            | 4.49141503848431   | 1689   |
| Paths of Glory (1957)                              | 4.485148514851486  | 202    |
| Wrong Trousers, The (1993)                         | 4.478260869565218  | 644    |
+----------------------------------------------------+--------------------+--------+
```

## 作业3
```sql
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
```
* 结果

```
+--------------------------------------------+---------------------+
|                 movie_name                 |       avgrate       |
+--------------------------------------------+---------------------+
| Big Lebowski, The (1998)                   | 3.7383773928896993  |
| Rear Window (1954)                         | 4.476190476190476   |
| Star Wars: Episode IV - A New Hope (1977)  | 4.453694416583082   |
| Sound of Music, The (1965)                 | 3.931972789115646   |
| Waiting for Guffman (1996)                 | 4.147186147186147   |
| Badlands (1973)                            | 4.078838174273859   |
| House of Yes, The (1997)                   | 3.4742268041237114  |
| Fast, Cheap & Out of Control (1997)        | 3.8518518518518516  |
| Roger & Me (1989)                          | 4.0739348370927315  |
| City of Lost Children, The (1995)          | 4.062034739454094   |
+--------------------------------------------+---------------------+
```