---
title:  "Exception Handling in Spark Data Frames"
date:   2017-05-04 21:08:49 +0530
categories: spark
tags: data-engineering spark scala data-frames data-errors
comments: true
---

### General Exception Handling

Handling exceptions in imperative programming in easy with a try-catch block. Though these exist in Scala, using this in Spark to find out the exact invalid record is a little different where computations are distributed and run across clusters. A simple try catch block at a place where an exception can occur would not point us to the actual invalid data, because the execution happens in executors which runs in different nodes and all transformations in Spark are lazily evaluated and optimized by the Catalyst framework before actual computation.

For most processing and transformations, with Spark Data Frames, we usually end up writing business logic as custom udfs which are serialized and then executed in the executors. Without exception handling we end up with Runtime Exceptions. Sometimes it is difficult to anticipate these exceptions because our data sets are large and it takes long to understand the data completely. Also in real time applications data might come in corrupted and without proper checks it would result in failing the whole Spark job.

### The Runtime Exception

Lets take an example where we are converting a column from String to Integer (which can throw NumberFormatException). Our idea is to tackle this so that the Spark job completes successfully.

Now this can be different in case of RDD[String] or Dataset[String] as compared to Dataframes. In most use cases while working with structured data, we encounter DataFrames. So our type here is a Row.

We define our function to work on Row object as follows without exception handling. This can however be any custom function throwing any Exception.: 
```scala
val df: DataFrame = ...

// Convert using a map function on the internal RDD and keep it as a new column
val simpleCastRDD: RDD[Row] = df.rdd.map { row: Row =>
  val memberIdStr = row.getAs[String]("member_id")
  val memberIdInt = memberIdStr.toInt
  Row.fromSeq(row.toSeq.toList :+ memberIdInt)
}

// Create schema for the new Column
val simpleCastSchema = StructType(df.schema.fields ++ Array(StructField("member_id_int", IntegerType, true)))
println("Simple Casting")
val simpleCastDF: Dataset[Row] = sparkSession.sqlContext.createDataFrame(simpleCastRDD, simpleCastSchema)

simpleCastDF.printSchema()
simpleCastDF.show()
```
The above can also be achieved with UDF, but when we implement exception handling, Spark wont support Either / Try / Exception classes as return types and would make our code more complex.
Since the map was called on the RDD and it created a new rdd, we have to create a Data Frame on top of the RDD with a new schema derived from the old schema.

The above code works fine with good data where the column member_id is having numbers in the data frame and is of type String.
When an invalid value arrives, say "**" or "," or a character "aa" the code would throw a java.lang.NumberFormatException in the executor and terminate the application.
Worse, it throws the exception after an hour of computation till it encounters the corrupt record.

### Handling Exceptions
Here I will discuss two ways to handle exceptions. One using an accumulator to gather all the exceptions and report it after the computations are over. The second option is to have the exceptions as a separate column in the data frame stored as String, which can be later analysed or filtered, by other transformations.

If the number of exceptions that can occur are minimal compared to success cases, using an accumulator is a good option, however for large number of failed cases, an accumulator would be slower.

#### Using Collection Accumulator

**Spark Accumulators**

Spark provides accumulators which can be used as counters or to accumulate values across executors. Accumulators have a few drawbacks and hence we should be very careful while using it.
 - The accumulator is stored locally in all executors, and can be updated from executors.
 - Only the driver can read from an accumulator.
 - The accumulators are updated once a task completes successfully.
 - If a stage fails, for a node getting lost, then it is updated more than once.
 - If an accumulator is used in a transformation in Spark, then the values might not be reliable. If multiple actions use the transformed data frame, they would trigger multiple tasks (if it is not cached) which would lead to multiple updates to the accumulator for the same task.
 - In cases of speculative execution, Spark might update more than once.
 - The values from different executors are brought to the driver and accumulated at the end of the job. Thus there are no distributed locks on updating the value of the accumulator.
 - If the data is huge, and doesn't fit in memory, then parts of might be recomputed when required, which might lead to multiple updates to the accumulator.
 
Keeping the above properties in mind, we can still use Accumulators safely for our case considering that we immediately trigger an action after calling the accumulator. This prevents multiple updates.
In Spark 2.1.0, we can have the following code, which would handle the exceptions and append them to our accumulator.
We use Try - Success/Failure in the Scala way of handling exceptions. 

First we define our exception accumulator and register with the Spark Context
```scala
val exceptionAccumulator: CollectionAccumulator[(Any, Throwable)] = sparkSession.sparkContext.collectionAccumulator[(Any, Throwable)]("exceptions")
```

We cannot have Try[Int] as a type in our DataFrame, thus we would have to handle the exceptions and add them to the accumulator. Finally our code returns null for exceptions.
While storing in the accumulator, we keep the column name and original value as an element along with the exception. This would help in understanding the data issues later.

```scala
val df: DataFrame = ...

val errorHandledRdd: RDD[Row] = df.rdd.map { row: Row =>
  val memberIdStr = row.getAs[String]("member_id")
  val memberIdInt = Try(memberIdStr.toInt) match {
    case Success(integer) => integer
    case Failure(ex) => exceptionAccumulator.add((("member_id", memberIdStr), ex))
      null // Because other boxed types are not supported
  }
  Row.fromSeq(row.toSeq.toList :+ memberIdInt)
}

val errorHandledSchema = StructType(df.schema.fields ++ Array(StructField("member_id_int", IntegerType, true)))

val castDf: Dataset[Row] = sparkSession.sqlContext.createDataFrame(errorHandledRdd, errorHandledSchema) 
// Note: Ideally we must call cache on the above df, and have sufficient space in memory so that this is not recomputed.
// Everytime the above map is computed, exceptions are added to the accumulators resulting in duplicates in the accumulator. 

castDf.printSchema()
castDf.show()
```

This works fine, and loads a null for invalid input.

To see the exceptions, I borrowed this utility function:
```scala
def printExceptions(acc: CollectionAccumulator[(Any, Throwable)]): Unit = {
  acc.value.toArray.foreach { case (i: Any, e: Throwable) =>
    // using org.apache.commons.lang3.exception.ExceptionUtils
    println(s"--- Exception on input: $i : ${ExceptionUtils.getRootCauseMessage(e)}") // ExceptionUtils.getStackTrace(e) for full stack trace
  }
}

// calling the above to print the exceptions
printExceptions(exceptionAccumulator)
```

This looks good, for the example. But say we are caching or calling multiple actions on this error handled df. This would result in invalid states in the accumulator.
To demonstrate this lets analyse the following code:
```scala
castDf.show()

println("Show has been called once, the exceptions are : ")
printExceptions(exceptionAccumulator)

println("Cache and show the df again")
castDf.cache().show()
println("Now the contents of the accumulator are : ")
printExceptions(exceptionAccumulator)
```

The output is as follows:
```console
+---------+-------------+
|member_id|member_id_int|
+---------+-------------+
|      981|          981|
|        a|         null|
+---------+-------------+

Show has been called once, the exceptions are : 
--- Exception on input: (member_id,a) :  NumberFormatException: For input string: "a"
Cache and show the df again
+---------+-------------+
|member_id|member_id_int|
+---------+-------------+
|      981|          981|
|        a|         null|
+---------+-------------+

Now the contents of the accumulator are : 
--- Exception on input: (member_id,a) :  NumberFormatException: For input string: "a"
--- Exception on input: (member_id,a) :  NumberFormatException: For input string: "a"

Process finished with exit code 0
```
It is clear that for multiple actions, accumulators are not reliable and should be using only with actions or call actions right after using the function. When a cached data is being taken, at that time it doesn't recalculate and hence doesn't update the accumulator.

#### Log Exceptions to a new Column
Another interesting way of solving this is to log all the exceptions in another column in the data frame, and later analyse or filter the data based on this column.
In the following code, we create two extra columns, one for output and one for the exception.
```scala
val df: DataFrame = ...

val rddWithExcep = df.rdd.map { row: Row =>
  val memberIdStr = row.getAs[String]("member_id")
  val memberIdInt = Try(memberIdStr.toInt) match {
    case Success(integer) => List(integer, null)
    case Failure(ex) => List(null, ex.toString)
  }
  Row.fromSeq(row.toSeq.toList ++ memberIdInt)
}

val castWithExceptionSchema = StructType(df.schema.fields ++ Array(StructField("member_id_int", IntegerType, true)
  , StructField("exceptions", StringType, true)))

val castExcepDf = sparkSession.sqlContext.createDataFrame(rddWithExcep, castWithExceptionSchema)

castExcepDf.printSchema()
castExcepDf.show()

```
Now we have the data as follows, which can be easily filtered for the exceptions and processed accordingly

```
+---------+-------------+------------------------------------------------------+
|member_id|member_id_int|exceptions                                            |
+---------+-------------+------------------------------------------------------+
|981      |981          |null                                                  |
|a        |null         |java.lang.NumberFormatException: For input string: "a"|
+---------+-------------+------------------------------------------------------+
```

Would love to hear more ideas about improving on these.

### References
 - http://danielwestheide.com/blog/2012/12/26/the-neophytes-guide-to-scala-part-6-error-handling-with-try.html
 - https://www.nicolaferraro.me/2016/02/18/exception-handling-in-apache-spark/
 - http://rcardin.github.io/big-data/apache-spark/scala/programming/2016/09/25/try-again-apache-spark.html
 - http://stackoverflow.com/questions/29494452/when-are-accumulators-truly-reliable

