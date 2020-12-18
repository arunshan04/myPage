---
title:  "SQL Analytical Functions - I - Overview, PARTITION BY and ORDER BY"
date:   2017-04-27 21:08:49 +0530
categories: sql
tags: sql analytics data-exploration analytical-functions
comments: true
---

For a long time I had faced a lot of problems while working with data bases and SQL where in order to get a better understanding of the available data, simple aggregations using group by and joins were not enough. For deep diving into data, advanced SQL features were necessarry.

#### Introduction 
Aggregate functions are good with the Group By clause for most simple data related calculations. However things like having the average of a group as a column or calculating the deviation from average for each record becomes increasingly complex to express using group by and self join in a sql query.
Often while learning SQL, the group by and join are probably enough to gain a basic understanding but very difficult for most analytical operations where the aim is to understand the data.

A few features of the SQL dialect available in most RDBMS or SQL interface which help in better understanding are 
 - the OVER() clause
 - the analytical functions, for example AVG, LAG, LEAD, PERCENTILE_RANK
 - the PARTITION BY clause, for example PARTITION BY dept, job
 - the ORDER BY clause, for example order by job nulls last
 - the ROW or RANGE clause for the window frame extent, for example RANGE UNBOUNDED PRECEDING or ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
 
Over the course of a series of posts, I will try to explain the use of these in SQL along with solutions to few problems from HackerEarth for practice and better understanding.

#### Recap
*The aggregate function and GROUP BY:* This post assumes that the reader has good knowledge of group by and finding aggregates for a given data set.
As a quick example, suppose we want to find the average marks scored by students from a table having "id, name, subject, marks" we can run
```sql
SELECT AVG(marks) 
FROM marks
```
The above query gives one record as output which is the average of the whole data.
Now to find AVG of marks for each subject, we can use a group by clause:
```sql
SELECT subject, AVG(marks) 
FROM marks 
GROUP BY subject
```
Now I have applied a group by clause and specified the column to group the data. This has produced as many groups as there are subjects, and calculated avg marks for each subject. This has returned me records exactly equal to the number of distinct subject values in the table.


### OVER() clause
The Over clause is used to define the partition, ordering and window extent about how a particular analytical function would act on the data set.
Analytical functions that are used can be AVG, SUM (which are aggregate functions) or RANK, ROW_NUMBER which are purely analytical functions. While using the analytical version of these functions, we don't need a group by clause and the number of records produced are same as the input records.
Lets us take a simple example of AVG, and apply it to a dataset of "id, name, subject, marks"
```sql
SELECT id, name, subject, marks,
       AVG(marks) OVER() AS avg_marks
FROM marks
```
Here the OVER clause is blank and it doesn't partition / order / window the data before applying the AVG function. Thus the output has an extra column with the avg marks of all the records as a new column.
The real application of the over clause is when we use it to specify how to handle the data before applying the AVG function and what should act as inputs to each function calls.


### PARTITION BY clause
Lets say our aim was to find out how much each student's marks deviates from the mean for that subject. For this we know that we don't want to aggregate the data and want to keep the same number of rows as the input.
An easy way to approach this problem is to find the average of marks for each subject and then have that as a column and find the difference for each student and subject mark.
Now suppose we wanted to have avg marks for each subject as a column, without the over clause, would have meant grouping the data by subject, and then applying the AVG function for each group.
This would produce records equal to the number of subjects. To get the avg as another column we would have to do a self join and then find the deviation for each student making things increasingly complex.

The OVER clause allows us to apply a function over a specified subset of input and have that aggregate or output as a column.
For our example, we need to find average for each subject, which means we can partition the data based on each subject and then apply our function on each partition, have the output as a column.
Lets try the query
```sql
SELECT id, name, subject, marks,
       AVG(marks) OVER (PARTITION BY subject) AS avg_subject_marks
FROM marks
```
The above query applies the AVG of marks after partitioning the data by subject. So average is calculated for each subject, and is returned as a column.
Our initial problem to find the difference of each student from subject average is now simple and can be expressed as follows:
```sql
SELECT id, name, subject, marks,
       ABS(marks - AVG(marks) OVER (PARTITION BY subject)) AS mean_subject_diff
FROM marks
```


### ORDER BY clause
We all know using the Order by clause to sort the query output. But inside the Over Clause the order by is used to specify the order in which the data would be provided to the analytical function as input and processed.
This is particularly useful with ranking and percentile functions which work on ordered datasets.
Consider we have to find the rank of each student. For this we create a temporary table which has data for each student.
```sql
CREATE TABLE total_marks AS
SELECT id, name, SUM(marks) AS total_marks
FROM marks
GROUP BY id, name
```
Thus our input data set has 3 columns, and total records equal to the total number of students.
To find the rank, we simply order the data descending by total_marks, and put a rank against it.
```sql
SELECT id, name, RANK() OVER (ORDER BY total_marks DESC) as overall_rank
FROM total_marks
```
Here the OVER clause specified how to order the data before passing it over to the rank function which assigns a new value for each input record.
Yes, these to queries can be combined using a sub query or a CTE, but to keep things simple I made it as a table.


### Using PARTITION BY along with ORDER BY
Consider we have to find the rank of each student for each subject. For this we partition the data for each subject and then order the students based on their ranks. Now we can easily put a number and have a rank for each student for each subject.
The query looks like

```sql
SELECT id, name, subject, marks,
       RANK() OVER (PARTITION BY subject ORDER BY marks DESC) AS subject_rank
FROM marks
```
This query is pretty readable and self explanatory. It says to partition the data first and then order it descending based on marks, and finally put a rank for each record.

#### Summary
The above techniques cover most requirements for simple understandings. Coupled with functions like RANK(), DENSE_RANK(), ROW_NUMBER(), NTILE() which we would soon visit and try to understand, these techniques give a lot of simple ways to explore data.
In the future blog posts I would write about using ROWS and RANGE to specify window frames to apply analytical functions.
 