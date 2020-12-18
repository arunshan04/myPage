---
title:  "Implementing Statistical Mode in Apache Spark"
excerpt: "Finding the most common value in parallel across nodes, and having that as an aggregate function."
date:   2017-07-14 00:08:49 +0530
categories: spark
tags: spark scala udaf statistics functions
comments: true
---

In this post we would discuss how we can practically optimize the statistical function of Mode or the most common value(s) in Apache Spark by using UDAFs and the concept of monoid.
 
### What is mode?
In statistics, mode is defined as the value that appears most often in a set of data. Essentially it is the most common value in a given set of data.

### Finding the mode in SQL
This might sound simple and we might be hoping that an aggregate function is already available. However in most cases it is not available because it is pretty complex to implement the theoretical definition of mode using SQL. 

For example, lets say we have a data set, where all the values in a particular field is same, or there are multiple values having the same frequency. This boils down to the fact that the function should not give us one value as an aggregate, but a set of values as the output. This implies that the output should be an array. However in most real world use cases, where one would need to find mode, the attributes are usually nominal having a varied distribution and it can be assumed that there is only one value with the highest frequency. Thus for practical purposes we may assume that the output is a single value and not a set.

Now, let us consider a data set such as ```trip_id, user_id, timestamp, city, airline, hotel_name```.

We want to find out the most visited city for each visitor and tag that as his home city, find the airline he uses the most, and the hotel he stays the most in, and then use this data for further computations. Now lets try and express this use case.

Intuitively we would want something like this:
```sql
SELECT user_id, mcv(city) as home_city, mcv(airline) as fav_airline, mcv(hotel_name) as fav_hotel
FROM <trips>
GROUP BY user_id
```
This is simple to express, easy to read, but the catch is that we don't have a function to calculate the most common value (mcv) for a particular group of data.

Without having this function, here is how it looks like:
```sql
home_city =
SELECT user_id, city as home_city, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY cityFreq) as rn
FROM (
    SELECT user_id, city, count(city) as cityFreq
    FROM <trips>
    GROUP BY user_id, city
) WHERE rn = 1 
```
Using Rank would give the true definition of mode, as there can be multiple values holding the same max frequency. But then would have to introduce another GROUP BY to convert that into a SET, and keep it in one row. 

So now we have home_city, we write the same thing again for airline, and again for hotel, and again for 100 other features that we may have. And we have ended up with multiple tables, so join these based on user_id.

Lets see how this would work:
First it would do a grouping operation then a ROW_NUMBER or a RANK on an ordered set. Ordering the data is not the best thing we want to do especially because we would be ignoring every but the one with row number 1. Can we do a max? Yes, but that would need another join, and all this to calculate the home city.
That's not the end, we will need one join for each feature, before we can have the final data set. Thus for 3 features that is 2 joins.

So we have ended up doing 3 grouping operations, 3 partitioning and 3 ordering operations, and 2 joins to find the most common value for 3 attributes.
Pretty costly.

An UDAF is what helps us in these cases. Lets understand that now.

### Defining custom aggregate functions
Analysing our problem statement, we can say that our UDAF would be run for each user, and the input for one run would be all city names for that user. The output should be the most common value from that set.

This sounds simple, and the steps for this function may be defined as follows:
 - Find the frequency of each element in the given input data.
 - Find the element with the max frequency.
 - Send that as the output.
 
For practical use cases, we can assume that there is only one value which is the most frequent, and thus we would return a single value. This also means that the complexity of our function would now be ```O(n)```. (If we wanted to find all the most common values - theoretical mode - then we would have to sort based on frequencies, and it would be ```O(n log n)``` ) 

### Monoids
This might sound completely random to come in as a section here, but lets recollect the definition of Monoids. It would help us in writing much more optimized codes and understand how we can create UDAFs in Spark (or other SQL)

By definition, we can say:
```
Given a type T, a binary operation Op:(T,T) => T, and an instance Zero: T, with the properties that will be specified below, the triple (T, Op, Zero) is called a monoid. 
Here are the properties:

Neutral element: Zero Op a == a Op Zero == a
Associativity: (a Op b) Op c == a Op (b Op c)
```

Satisfying the associative property helps us in randomly grouping the given inputs and run these groups them in parallel and merge the outputs into one. 

If we want to calculate avg, the neutral element is the average itself, but average is not associative. However average can be expressed in terms of sum and count, which are associative. We can count elements in parallel and then add them up to find the total count. Similarly we can also add up elements and then add the results to find total sum. 
Using this information we can find the average at the end. Thus expressing average as a monoid helps us in calculating the average in parallel. The neutral element for sum is 0 and for count is null.

It is easy to understand how we can find sum and average in parallel at the same time.

Now we can see that computation of functions which can be expressed as monoids can run in parallel very easily. This helps us in writing very efficient map reduce code.  

### Spark UDAF
Going by the [user guide](https://docs.databricks.com/spark/latest/spark-sql/udaf-scala.html) on Spark UDAF its clear that the above algorithm can't be used as is, since we don't even have a function that we can override where the input parameter contains the whole list of elements.
To run our most common value function in parallel we can see that if we divide a given set of data into multiple sets and calculate the most common value in each set, and subsequently find the most common value, we wont end up with the actual most common value for the whole set.

This is where we try to define our function in terms of monoid, and see how it fits in the functions defined in the user guide.

Considering that for practical use cases the cardinality of the attribute wont be very high, (in most cases where you would logically want to find the mode) we can say that if we know the frequency, find the element with max frequency can happen in one node, and doesn't need to be distributed.
This leaves us with the first part: finding the frequencies. Can this be distributed? Let us try by writing a monoid which returns the frequency of each distinct element in a given set of values.

 - The computation would run in parallel, lets say we have `n` partitions.
 - The frequencies would essentially be of type ```Map[Object, Long]``` with the item and its frequency. 
 - Initially the Map would be empty, in each of the partitions. (This is the neutral element)
 - Add each element to the already maintained Map. (Update the map with one element, by increasing its frequency)
 - Every partition is now reduced to a ```Map[Object, Long]```
 - Merge the output of all the partitions till we get one output. (The actual function which is the monoid, this is where associativity is used) 
 - Find out the max frequency, from the merged Map. (Some other operation at the end)

Thus finding the frequencies in each partition and then merging them again to find the total set of frequencies, can happen in parallel at the same time.
 
Now let us write these logic as part of the Spark UDAF.

We are given a buffer for our aggregate function to store the intermediate results
Its Schema can be defined as follows. (Considering that we are calculating the mode on a field with elements of StringType)
```scala
override def bufferSchema: StructType = StructType(
  StructField("frequencyMap", DataTypes.createMapType(StringType, LongType)) :: Nil
)
```

Now for calculating we need to initialize the MapType for each partition.
```scala
override def initialize(buffer: MutableAggregationBuffer): Unit = {
  buffer(0) = Map[String, Long]()
}
```
The buffer is where we store intermediate results.

Now when a new value comes in, we need to add it to our frequency map.
```scala
override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
  buffer(0) = buffer.getAs[Map[String, Long]](0) |+| Map(input.getAs[String](0) -> 1L)
}
```
Note, the function ```|+|``` is from Scalaz and adds the frequencies for the same string. We also need to ```import scalaz.Scalaz._```
This is a SemiGroup operator from the package. This function is associative and hence we use to merge the frequency maps.

After we get all the frequency maps from the parallel tasks, we need to merge them. This merges two aggregation buffers.
```scala
override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
  buffer1(0) = buffer1.getAs[Map[String, Long]](0) |+| buffer2.getAs[Map[String, Long]](0)
}
```

Finally find the most common element from the merged Map.
```scala
override def evaluate(buffer: Row): String = {
  buffer.getAs[Map[String, Long]](0).maxBy(_._2)._1
}
```

With the above our Spark Code now becomes:
```scala
val mostCommonValue = new MostCommonValue
df.groupBy("user_id")
  .agg(mostCommonValue(col("airline")).as("fav_airline"), mostCommonValue(col("city")).as("home_city"))
```

Simple, clean and fast.

The full class for the UDAF is available [here as a gist](https://gist.github.com/anish749/6a815ed281f538068a0d3a20ca9044fa)

Cheers
