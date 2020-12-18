---
title:  "Using HashSet based indexes in Apache Spark"
date:   2017-05-14 00:08:49 +0530
categories: spark
tags: spark scala join optimization indexes hashset
comments: true
---

This post is about de-duplication of data while loading to tables using HashSet based indexes in Apache Spark.

### Introduction
Indexes in R-DBMS systems are used to improve query performance and access times of the data stored. But while using systems like Spark which is primarily an in-memory computation engine, we don't have indexes. Data sources on the other hand implement indexing and in Spark we can do predicate push down to take advantage of this indexing.
Loading data to indexed tables take more time since it rebuilds the index. In most cases indexes are also not optimized to load data faster, but to enhance faster reads.
Here I try an use case of incremental loading of data, where I eliminate duplicates without using a join or a full table scan of the existing data set.


### The problem statement
A table exists in Hive or any destination system and data is loaded every hour (or day) to this table. As per the source systems, it can generate duplicate data. However the final table should de-duplicate the data while loading. We assume that data is immutable, but can be delivered more than once, and we need a logic to filter these duplicates before appending the data to our master store.

_Assumption:_ We have the data partitioned and data doesn't get repeated across partition. This just makes the problem a simpler to optimize as it would help in reducing shuffles. For other use cases we can very well consider the whole data to be in one partition.

_Sample use case:_ I used this on click stream data with an at least once delivery guarantee. However since the data is immutable, the source timestamp doesn't change, and I partition the data based on this source timestamp.

### SQL based solution
A typical query to do this require a join of the existing and new data sets. Indexes do speed up joins in R-DBMS systems.

A simple sql based solution of doing a de-duplicated insert is often found in data bases which appends new data from a source table to a destination table. However to realize this insert in a SQL query, it essentially means to do an outer join of the two data sets and then choose only the records that are actually new and discard the old. Also we must make sure we take only a distinct set of records from the new data, which means a distinct before joining or a group by after joining to completely remove duplicates.

The same query in expressed in terms of the map reduce paradigm, transforms to a very costly join and multiple shuffles. The distinct or a group by to eliminate the duplicates from the new data set also means more complex reduce or shuffle and reduce operation respectively. Over all the whole query is not the most optimized way of doing things.


### The data size
With a very high data size, traditional data bases don't scale well, so we go for a map reduce based solution, but we don't have any indexes here. Spark cannot take advantage of indexes to optimize the join even if those are defined in the data sources.


### A Spark native solution
Here I will discuss a solution for managing and maintaining a HashSet of the composite keys for each partition using which we can eliminate a full table scan of the existing data to calculate the subset of the new data that should be appended after removing the duplicates.
The whole code base is [here](https://github.com/anish749/spark-indexed-dedup/) and can be downloaded and tested.
The main starting point of this etl is the runEtl() method in DeDuplicateMergeData class.

#### The Logic
When the data is being loaded, create and maintain another pair RDD where the key is the partition and the value is one single HashSet of the composite keys present in that partition of the data.

While loading the data first we convert it into a pair RDD and partition the incoming data in the same way as the existing data. Then for each partition we de-duplicate the data using a HashSet for each partition. (Explained with code later)

In the first run, there is no data present, so we go ahead and load this de-duplicated data and generate the index to be used for the next load.

If there is data existing, but the HashSet index is not available, as a recovery option, we create the HashSet of the existing data and then proceed.

Now we co-group the new data along with the HashSet index and then filter the duplicate records from the new data based on the HashSet. This lookup now happens in O(1) time since we are having a HashSet present.

After having the filtered data, append it to the existing data and merge the index of the new data with the index of the old data. Write the existing data first and then if successful, save the index to a file system.

Now let us look at the individual steps in detail to understand further.

#### The HashSet based index
The HashSet based index is essentially another RDD which stores a HashSet of the composite keys for each partition of the data that has already been loaded.
Since a Sequence of multiple values form the composite key, we maintain one HashSet of these keys. So the type is ```HashSet[Seq[String]]```, and since this HashSet is for only one partition, our pair RDD for the whole data is of type  ```(Int, HashSet[Seq[String]])``` where the data is partitioned based on a column of type Int.

We create the HashSet index RDD from a partitioned pair RDD. (The partitioning strategy is discussed after this) 
```scala
private def createHashSetIndex(partitionedPairRDD: RDD[(Int, Row)],
                                 partitioner: Partitioner,
                                 primaryKeys: Seq[String]): RDD[(Int, HashSet[Seq[String]])] = {
    // Create a HashSet (or a Bloom Filter) index for each partition
    val createCombiner = (row: Row) => HashSet(SerializableHelpers.getKeysFromRow(row.asInstanceOf[GenericRowWithSchema], primaryKeys))
    val mergeValue = (C: HashSet[Seq[String]], V: Row) => C + SerializableHelpers.getKeysFromRow(V.asInstanceOf[GenericRowWithSchema], primaryKeys)
    val mergeCombiners = (C1: HashSet[Seq[String]], C2: HashSet[Seq[String]]) => C1 ++ C2

    // Pass our partitioner to prevent repartitioning / shuffle
    partitionedPairRDD.combineByKey(
      createCombiner = createCombiner,
      mergeValue = mergeValue,
      mergeCombiners = mergeCombiners,
      partitioner = partitioner)
  }
```
Here the function getKeysFromRow is defined as follows and is used to extract and make a sequence of the composite keys from one Spark Row object.
```scala
def getKeysFromRow(row: GenericRowWithSchema, primaryKeys: Seq[String]): Seq[String] =
    primaryKeys.map(fieldName => row.getAs[String](fieldName))
```

The ```createCombiner``` function creates a HashSet from the row object.
The ```mergeValues``` function is used to combine a Row into a HashSet.
The ```mergeCombiners``` are used to merge two HashSets.
Using these three functions, Spark runs the operation of creating the HashSets for each partition in parallel. The resulting output of ```rdd.combineByKey``` is one pair RDD where the key is the partition column value and the value is a HashSet of the composite keys in that partition. 

#### Custom Partitioner
To make sure that every step in creation of the index or other co-group operation doesn't not trigger a new Shuffle, we use the same partitioner everywhere and have most operations as a OneOnOneDependency on the previous RDD rather than a ShuffleDependency. Our idea is to make sure that the internal partitioning logic of RDDs and the partitioning that we want to maintain is the same. For example if we want our data to be partitioned by date, the RDD should internally keep values of the same date in the same partition. We can increase or decrease the granularity of the partitioning column to control the size of the data in one partition. 
By default Spark uses HashPartitioner which takes the data and puts to a constant number of partitions based on the hashcode of the key, in case of a key value pair RDD. This means data with multiple values of the key can end up with the same value of ```nonNegativeMod(key.hashcode, numPartitions)``` which means that these would be present in the same partition. We can decide on the number of partitions by using estimators described later, but it would still result in irregular partitions.

For this we implement an exact value based partitioner. The would make sure that the data from one key value would only go to one partition. (One trick to implement this is to use RangePartitioner in Spark, with a very high value for the number of partitions, but this does a sampling underneath and scans a part of the data which is not necessary for our purpose.) [Our implentation](https://github.com/anish749/spark-indexed-dedup/blob/master/src/main/scala/org/anish/spark/indexeddedup/partitioner/SimpleModuloPartitioner.scala) is to simple do a ```nonNegativeMod(key, numPartitions)``` and make sure that we are using an Integer or a Double value as the partition key. If we were to partition based on geographical regions, the HashPartitioner with a high value in the numPartitions would probably be good enough. 
```scala
override def getPartition(key: Any): Int = key match {
    case null => 0
    case key: Int => nonNegativeMod(key.asInstanceOf[Int], numPartitions)
    case key: Double => nonNegativeMod(key.asInstanceOf[Double].toInt, numPartitions)
    // Long is not defined as numPartitions should be Int as per Spark Partitioner class
    case _ =>
      throw new RuntimeException("Key is not Int or Double or it is not supported yet")
  }
```
In this partitioner implementation all records having the same partition column value ends up in the same partition of the RDD. This helps us in more optimized filtering after we co-group with the HashSet RDD.

#### Partition size estimators
Our next step is to find out what should be our number of partitions. We can either to a group by or a distinct (in Spark v2.0 and later, distincts are optimized to use groupBy internally) and then count the values of the partitioning column.
Or we can use a high enough number and have some empty partitions which gets ignore while processing producing no output, and is not really a huge overhead.

The other option is to get this information from the metadata of the source. In this case I used it with weblogs and extracted the date from the log file name. The idea was to keep the data originating in all the log files in one day in a single partition.
Estimating from dates in the file name somewhat looks like this in the implementation [here](https://github.com/anish749/spark-indexed-dedup/blob/master/src/main/scala/org/anish/spark/indexeddedup/estimator/PartitionEstimator.scala):
```scala
lazy val estimate: Int = {
    val datePatternRegex = datePattern.r
    val datesInFiles = sparkContext
      .wholeTextFiles(inputFilePath)
      .map(_._1)
      .map {
        case datePatternRegex(date) => date
        case _ => println("ERROR : did not match given regex")
      }
      .distinct
      .collect
      .toList

    datesInFiles.length
  }
```
```datePattern``` is a regex which has the date pattern in the file name. This however doesn't look very optimized for blob stores like S3 and can be optimized further to uses FileStatus of Hadoop.


#### Finding distinct using HashSets
Given a pair RDD which is partitioned using the above partitioning techniques, we are sure that data of the same key is always there in only one partition and one partition has values with only one key. Finding the distinct records for each partition now becomes very easy as we don't need a shuffle to find the distinct. Our aim is to find the distinct values in one given partition only. This means that the partition information is not needed and the logic is stateless and can run independently for each partition. Think of this as data from China can never be duplicate in India, if the data is partitioned by countries. And if the date is generated at source, the data generated on one day may be send multiple times on the same day but never on different days, because the data is immutable and we have partitioned based on the time the data was generated. Data generation time (not when a server logs it) would always be immutable.

The above operation can be expressed as ```mapPartitions``` on our new data RDD. However I though of implementing this as a new type of RDD extending the actual RDD implementation. Here we directly define the way we want to compute the data from the previous RDD. The core logic looks as follows as is implemented [here](https://github.com/anish749/spark-indexed-dedup/blob/master/src/main/scala/org/anish/spark/indexeddedup/internal/DeDupPairRDD.scala):
```scala
@DeveloperApi
override def compute(split: Partition, context: TaskContext): Iterator[(K, V)] = {
    val iterList = firstParent[(K, V)].iterator(split, context).toList

    if (iterList.nonEmpty) {
      val hashSetForPartition: MHashSet[Seq[Any]] = MHashSet()

      iterList.filter(record => {
        // Cast to GenericRowWithSchema to enable access by field name
        val keysInRecord = getKeysFromRow(record._2.asInstanceOf[GenericRowWithSchema])
        if (hashSetForPartition.contains(keysInRecord)) {
          false
        }
        else {
          hashSetForPartition.+=(keysInRecord)
          true
        }
      }).toIterator
    }
    else firstParent[(K, V)].iterator(split, context) // The partition is empty
}
```
This is however a DeveloperAPI is Spark and should be used with caution. The method ```getKeysFromRow``` is same as the one described previously.

#### Merging data based on Hash Set index
Now comes the most interesting part where we skip the full table scan but still join using the index that we had created and filter the data that was already present.
```scala
private def filterOldRecordsUsingHashSet(deDupedNewData: RDD[(Int, Row)],
                                           existingHashSetRDD: RDD[(Int, HashSet[Seq[String]])],
                                           newDataPartitioner: Partitioner,
                                           primaryKeys: Seq[String]): RDD[(Int, Row)] = {
    // Step 3 - Merge - this is an incremental load, and old data is already available.
    // Step 3.1 - Cogroup the data. Passing the partitioner same as new data should prevent repartitioning the data
    val coGroupedRDD = deDupedNewData.cogroup(existingHashSetRDD, partitioner = newDataPartitioner)

    // Step 3.2 - Remove duplicates using old data HashSet - the actual merge operation
    coGroupedRDD.flatMapValues {
      case (vs, Seq()) => // This is a new partition and this wasn't present in old data
        vs.iterator
      case (vs, ws) => // This is a partition which is there in old data as well as new data
        val newRecordsIterator = vs.iterator
        val existingHashSet = ws.iterator.toList.headOption.getOrElse(HashSet()) // We expect only one HashSet
        newRecordsIterator.filter({ newRecord =>
          // Filter already existing data
          !existingHashSet.contains(SerializableHelpers.getKeysFromRow(newRecord.asInstanceOf[GenericRowWithSchema], primaryKeys))
        })
      // Ignore the case for only old partition with no new partition present -> case (Seq(), ws)
    }
}
```
This is implemented [here](https://github.com/anish749/spark-indexed-dedup/blob/master/src/main/scala/org/anish/spark/indexeddedup/DeDuplicateMergeData.scala). We first co-group the new data and the index to bring the HashSet to the partition we are working on. Now we use ```rdd.flatMapValues``` to implement our actual filter operation. This is much like a left outer join operation which internally calls the cogroup and flatMapValues API of RDDs. While doing the flatMap we check if the new data is already present in the HashSet of the old data for that partition. Note that we are only using the composite keys to check this, much like the join operation would be on some primary keys in any relation.

#### Merge HashSets
This is a another operation we need because we also have to merge the new HashSet index that was added to the existing HashSet index.
We use the following implented [here](https://github.com/anish749/spark-indexed-dedup/blob/master/src/main/scala/org/anish/spark/indexeddedup/DeDuplicateMergeData.scala) to achieve this:
```scala
private def mergeHashSetIndex(rdds: Seq[RDD[(Int, HashSet[Seq[String]])]],
                                partitioner: Partitioner): RDD[(Int, HashSet[Seq[String]])] = {
    // Apply reduce to union all the given rdds into one rdd.
    // This would forget the previous partitioning and simply increase the number of partitions.
    val unionedRDD = rdds.reduce(_.union(_))

    // The following function for merging two HashSets would work for merging values as well as Combiners
    val mergeVaulesAndCombiners = (C1: HashSet[Seq[String]], C2: HashSet[Seq[String]]) => C1 ++ C2
    // Because after co grouping we expect only one HashSet

    unionedRDD.combineByKey(createCombiner = (row: HashSet[Seq[String]]) => row,
      mergeValue = mergeVaulesAndCombiners,
      mergeCombiners = mergeVaulesAndCombiners,
      partitioner = partitioner) // This is required to force repartitioning as union would have likely increased the partitions
}
```
This is fairly simple because we first do a union of all the RDDs and then partition it to bring all the HashSets to the same partition as the key, and then merge the HashSets. The function used for merging values and combiners are the same in this case.

#### Storing and reading the Hash Set index
We also need to store and read this RDD of HashSets. For this we use ```rdd.saveAsObjectFile``` and ```sparkContext.objectFile[(Int, HashSet[Seq[String]])](hashSetIndexLocation)``` to save and read the index.
We also built some fault tolerance and around the reading to handle cases like first execution when no index would be present and cases when the we are applying the logic for the first time and data is already existing, so the index needs to be rebuilt. This logic is implemented [here](https://github.com/anish749/spark-indexed-dedup/blob/master/src/main/scala/org/anish/spark/indexeddedup/DeDuplicateMergeData.scala).

#### Building the complete ETL and handling failures
Now we try and implement all the above logic into one ETL which should solve our initial problem. 
The steps can be described as follows:
1. Partitioning and de-duplicating new data
    1. Prepare (make a pair rdd) and partition (spark memory partition) the new data.
    2. Remove duplicates using Hash table for new data. (Remove duplicates present in incoming raw data)
    3. Create a HashSet of the new data.
    4. Check if it is the first load, then return the de-duplicated data and the HashSet. End of story
2. Get ready to merge with old data. -> We know it is not a first load
    1. Load HashSet of Old Data. If this doesn't exist it should return empty. This might have got deleted.
    2. Check if existing HashSet is present, rebuild HashSet.
3. Merge
    1. If no old data is available, this is the fist load, load the new data as is.
    2. Co group the two data sets. Remove data that was already loaded before, based on available HashSet.
    3. Now we have the actual data that needs to be appended
4. Create/Update HashSet index
    1. Create the HashSet Index, for the incoming data set.
    2. If this is the first run, store this HashSet index.
    3. If HashSet already existed for old data, update and overwrite the new HashSet.
    4. Handle failures for writing and storing this HashSet.

_Output:_
1. The de-duplicated data that should be appended
2. The total HashSet index that would be valid after appending the data.

This is implemented in the runEtl method [here](https://github.com/anish749/spark-indexed-dedup/blob/master/src/main/scala/org/anish/spark/indexeddedup/DeDuplicateMergeData.scala)

#### Conclusion
The overall aim of this project was to speed up duplicated data loading to a master table from where data could be reported or used for various purposes. An optimization which decreases the time complexity from a full table scan to an O(1) look up is fairly complex to think, code and explain in a blog post. It would take quite a few iterations to go through this and understand the overall optimization and how it benefits by using less computation.


### References
 - http://blog.madhukaraphatak.com/extending-spark-api/
 - http://rahulkavale.github.io/blog/2014/12/01/extending-rdd-for-fun-and-profit/
 - http://codingjunkie.net/spark-combine-by-key/
 - http://abshinn.github.io/python/apache-spark/2014/10/11/using-combinebykey-in-apache-spark/
 - http://apachesparkbook.blogspot.in/2015/12/cogroup-example.html
 
 