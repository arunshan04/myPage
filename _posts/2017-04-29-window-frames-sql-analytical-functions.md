---
title:  "SQL Analytical Functions - II - Window Frames, ROWS and RANGE"
date:   2017-04-29 21:08:49 +0530
categories: sql
tags: sql analytics data-exploration analytical-functions
comments: true
---

A follow up post about specifying window frames to SQL analytical functions. This assumes you have already read my previous post where I described the use of OVER(), PARTITION BY and ORDER BY clause.

### ROWS and RANGE clause
The Over clause also allows us to specify window frames on which the analytical functions get applied. This is useful in calculating things like moving average and cumulative sum using SQL.
The partition by clause allows us to group the data set before the analytical function is applied. However it doesn't allow us to choose different sets records for each input record to apply a function.
Consider we have a rainfall data set for a city for 1 month. It has a schema like "date, precipitation". We want to find the out the total rainfall for the past 5 dates for each day of the month. For arguments sake, lets visually ignore the first 5 values, since they wont be having data for the last 5 days.
Here we see that to find sum, we can use the SUM function but we need only the last 5 records and not all the records, for each row.
Lets start with a simpler problem, and find the sum of all records and have that as a column. 
```sql
SELECT date, precipitation,
       SUM(precipitation) OVER () as total_precipitation
FROM rainfall
```

Next we would apply the window on the input data to the SUM function, so that only the last 5 days are taken. For this we would first sort the data and then add the window as follows
```sql
SELECT date, precipitation,
       SUM(precipitation) OVER (ORDER BY date ROWS BETWEEN 1 PRECEDING AND 5 PRECEDING) as total_precipitation
FROM rainfall
```
This query first orders the data based on date, and then selects the window frame for each row, which consists of the previous 5 rows based on the ordering specified. This set of records changes for every record. The analytical function is then applied on the selected window. This is particularly useful for moving averages.

ROWS or RANGE requires that the ORDER BY clause be specified. 

Practice problem [Calculate Moving Average on HackerEarth](https://www.hackerearth.com/problem/sql/calculate-moving-average/)

### Using PARTITION BY, ORDER BY and ROWS/RANGE together
Consider the above example but lets say now the data set contains data for multiple cities. Now the precipitation for each city would be different and the sum would also have to be calculated differently. Here we have the data set as "city, date, precipitation". We would patition the data by city in the Over clause.
```sql
SELECT city, date, precipitation,
       SUM(precipitation) OVER (PARTITION BY city ORDER BY date ROWS BETWEEN 1 PRECEDING AND 5 PRECEDING) as total_precipitation
FROM rainfall
```
When applied with partition by clause, the window frame is applied separately for each partition.

##### Other keywords
The window frame can be specified using (but not limited to) the following:
 - ROWS BETWEEN 1 PRECEDING AND CURRENT ROW -> the current value and one previous
 - ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW -> all records from start to current row in the given ordering. This is useful for cumulative sum calculation
 - ROWS BETWEEN 2 FOLLOWING AND 10 FOLLOWING -> records after the current row as per the number specified.
 - ROWS 5 PRECEDING -> This is equivalent to ROWS BETWEEN 5 PRECEDING AND CURRENT ROW

#### Summary
This is very useful for statistical analysis which involves calculation where the input set record for a particular analytical function changes for every record.
Applications of this technique include calculation of Year to Date sales, seasonal precipitation analysis, etc.
