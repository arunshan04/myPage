---
title:  "Geo Location Batch Search in Spark"
date:   2017-04-22 21:08:49 +0530
categories: spark
tags: spark mapreduce scala spacial join optimization
comments: true
---

A module written in Scala for Apache Spark v2.0.0 to batch process mapping of Geo Locations in two skewed data sets.
Link to code: https://github.com/anish749/geo-search-spark

## Introduction
Apache Spark application written in Scala to map given latitude longitude values to nearest latitude longitude values in a given set using functional programming simulating a map side join of two data sets.
This takes two sets of data as input. A master set of latitude longitude values (which are considered constant), and a set of latitude longitudes which are to be searched and mapped to this master set. This is expected to change on every run of the ETL/ join code.
##### Assumption: 
 - The master data set can be cached in memory, thus not very large. This is the index being used, and only has the available pairs of latitude and longitudes. An id can also be attached to this to track each point. This data set is already sorted (or can be sorted using the code provided) based on latitudes and then optionally on longitudes.
 - The master set has all unique coordinates.

##### Mapping logic
The two sets of coordinates are mapped based on the distance calculated by using the Haversine formula. The max limits considered as near can be specified while using the utility:
 - maxDistMiles - maximum distance that is considered as near (in miles) [used 30 here]
 - latLimit, lonLimit - To increase efficiency, the area in which the actual distances are calculated is narrowed down by using these limits. Eg: 0.5 latLimit means the distance of points beyond searchLat +- 0.5 are ignored, since 0.5 degree means 34.5 miles approx, the mapped lat lon values would not lie beyond this range. Similarly for longitude, till 83 deg latitude the values for 30 miles is 3.9 degree

Geosearch in databases and SQL queries (if written in Hive) use theta joins which are not supported in Hive, thus forcing cross products. This version using broadcasting of a sorted index to reduce the need for a cross product using a map side join.

A map reduce version of the code with a map side theta join implementation is available at https://github.com/anish749/geo-search-mapreduce

## Build
The project is written in Scala 2.11.6, and dependencies are managed using Maven 3.3.9. This is written in Apache Spark v2.0.0
```bash
$ mvn clean install
```

## Usage

### Adding this package as a dependency
Once installed via maven, this package can be added as a dependency as follows:
```xml
<dependency>
    <groupId>com.anish.spark</groupId>
    <artifactId>geosearch</artifactId>
    <version>1.0-SNAPSHOT</version>
</dependency>
```
Note: This is not available as a package in Maven Central repository.

### As a stand alone application
##### Create index
The first step is to create an index (a sorted array available to all executors during mapping) using the data for available latitude longitudes.
This is provided by the com.anish.spark.geosearch.Indexer class. The path where the index would be stored and where the available latitude longitudes are present are configured as variables in this class. (Should be changed / taken as arguments before usage in projects)
This class reads the files in the 'input/availableLatLon' folder which are configured to be ';' separated pairs of latitude longitudes to which data would be mapped. It then creates an index out of this data and saves it as files in the index Path given after sorting this based on latitudes and then on longitudes.

##### Run batch search
This is the actual batch job which searches for nearest latitude longitudes and maps them. This is implemented in the class named com.anish.spark.geosearch.Search.
This can be run as follows:
```bash
$ java -jar target/geosearch-1.0-SNAPSHOT.jar
```
It would automatically set the following configurations for demo as described in the Mapping Logic section:
```scala
val latLimit = 0.5
val lonLimit = 4.0
val maxDistMiles = 30.0
```
It would take the unknownLatLons as input and map them to the nearest available lat lon as read from the index and write the output in the folder configured.

### Batch Search in Spark applications (Scala API)
From a Scala/Java Application where the Spark Session is not available, we can call the following function to execute the search:
```scala
runSparkBatchSearch(indexFilePath, unknownLatLonPath, outputPath, latLimit, lonLimit, maxDistMiles)
```
If the Spark Session is available, then the actual search logic is implemented in the following function and can be called directly:
```scala
/**
 * @param sparkSession  The spark session. This is used for importing spark implicits
 * @param indexBc       Available latitude longitude Spark Broadcast variable
 * @param unknownLatLon A spark Dataset of the Coordinates that are to be mapped.
 * @return A DataSet of Row, which has all given Coordinates mapped to available Coordinates. (null for those which were not mapped)
 */

search(sparkSession: SparkSession, indexBc: Broadcast[List[Coordinate]], unknownLatLon: Dataset[Coordinate], latLimit: Double, lonLimit: Double, maxDistMiles: Double): Dataset[Row]

```
___