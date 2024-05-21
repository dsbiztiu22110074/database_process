if ( !require(duckdb) ) install.packages("duckdb")

library(duckdb)

con <- dbConnect(duckdb(), dbdir = 'a.duckdb', read_only = FALSE)

dbListTables(con)

d <- data.frame(name   = c('Taro', 'Jiro'),
                salary = c(600, 550))
dbWriteTable(con, 'items', d, append = T)

res <- dbGetQuery(con, "SELECT * FROM items")

print(res)

dbDisconnect(con, shutdown = TRUE)

library(nycflights13)
data("flights", package = "nycflights13") 

## SQLクエリによる取得
con <- dbConnect(duckdb())

duckdb_register(con, "flights", flights)

res <- dbGetQuery(con,
                  'SELECT origin, dest, n
  FROM (
    SELECT q01.*, RANK() OVER (PARTITION BY origin ORDER BY n DESC) AS col01
    FROM (
      SELECT origin, dest, COUNT(*) AS n
      FROM flights
      GROUP BY origin, dest
    ) q01
  ) q01
  WHERE (col01 <= 3) ORDER BY origin')

print(res)

duckdb_unregister(con, "flights")

dbDisconnect(con, shutdown = TRUE)

## dplyrによる取得
library(tidyverse)

con <- dbConnect(duckdb())

duckdb_register(con, "flights", flights) 

tbl(con, 'flights') |> group_by(origin) |> count(dest) |> slice_max(n, n = 3) |> arrange(origin) -> res

print(res)

res |> collect() |> as.data.frame() -> d.out

duckdb_unregister(con, "flights")

dbDisconnect(con, shutdown = TRUE)

## 演習課題
library(tidyverse)

d <- data.frame(
  name = c("太郎", "花子", "三郎", "良子", "次郎", "桜子", "四郎", "松子", "愛子"),
  school = c("南", "南", "南", "南", "南", "東", "東", "東", "東"),
  teacher = c("竹田", "竹田", "竹田", "竹田",  "佐藤", "佐藤", "佐藤", "鈴木", "鈴木"),
  gender = c("男", "女", "男", "女", "男", "女", "男", "女", "女"),
  math = c(4, 3, 2, 4, 3, 4, 5, 4, 5),
  reading = c(1, 5, 2, 4, 5, 4, 1, 5, 4) )

library(DT)
datatable(d)

## 1
d |> select(name,math)

## 2
d |> select(-gender)

## 3
d |> slice(3:6)

## 4
d |> arrange(name)

## 5
d |> arrange(desc(math))

## 6
d |> arrange(desc(math),desc(reading))

## 7
d |> select(name,reading)

## 8
d |> summarise(math_mean=mean(math))

## 9
d |> group_by(teacher) %>%
  summarise(mean(math))

## 10
d |> filter(gender=='女') %>%
  select(name,math,reading)

## 11
d |> filter(school == '南',gender == '男') %>%
  select(name,reading)

## 12
d |> group_by(teacher) %>%
  filter(n()>=3) 

## 13
d |> mutate(total = math + reading)

## 14
d |> mutate(math100 = math*20)
