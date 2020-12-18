---
title:  "Big Data Lake in the AWS Cloud"
excerpt: "Using AWS S3 as a Big Data Lake and its alternatives"
date:   2018-03-18 00:08:49 +0530
categories: big-data
tags: data data-lake cloud system-design big-data s3 gcs aws gcp
comments: true
---

In this post I try to talk about my experience of building a (Big) Data Lake on the AWS Cloud using S3 as a data store, and solutions to common problems that we faced.

I had worked with Philips Health Tech to build the data lake, however most of the problems we faced are universal and can be applied to almost any large scale  data engineering product built these days.

## Moving into a cloud first world
As the size of data collected by businesses increased day by day, the cost required to manage these kept going up, to a point when businesses seriously started to think of cost effective ways to store and process the data.

Hadoop turned out to be a good bet for quite a few years, and almost every company had tried to use this to solve their problems. It did a great job but it didn't solve the Ops part of the story. Hadoop as a tool required managing and maintaining data centers, replacing old servers and disks as they wear out, making sure everything is running from day to day, etc.
With Hadoop you didn't have to worry about data security, data loss, data availability, but you were always required to have an Operations team to manage the infrastructure and keep it up and running.
However the landscape of Big Data changed rapidly with technologies getting outdated faster than ever and new processing frameworks solving newer problems every other day. So this meant upgrading clusters and up-skill teams, which required effort, experimentation and risks.

The cloud first world aims to solve these problems with the use of managed services and decoupling everything. A Data Lake in the cloud aims to be a true No-Ops data store which is virtually of infinite scale, and doesn't require managing separate teams monitoring or maintaining clusters. This is achieved by storing your data in a highly secure, durable and available shared data center. Also you have separate services for storing your data, and computing. With almost all kinds of services available for storing, caching, fast look ups, archived storage, on demand processing available, you can process almost anything without having to do anything about managing operations.

## Huge data in On-Premise servers and the shift to Public Cloud
Before we could trust on cloud, software wasn't really a service but a product you buy and keep in your office. This meant having space to stack up servers and maintaining and managing them. With data growing faster than ever, technologies becoming obsolete sooner than you think of, companies had to get into the business of managing, developing and maintaining huge data centers. It made all the sense for companies like Google, Facebook which operate at hyper scale, and face problems which are almost always unsolved. But not all business can or need to have separate potentially huge data center operations running with the primary motive being to manage servers.
This is the time, when going to a shared data center model, provides huge benefits in terms of costs, scalability and operations, and this is where the cloud steps in.

This can be thought of huge data centers around the world, which you can rent from time to time when you need and pay exactly for what you use. Other than saving costs, you also save all the idle time your servers were consuming and you were paying. Also this looks more environment friendly, because as a small business you would be having (say) a 100 servers which you won't switch off on the weekend.
On the other hand, in a cloud platform with a service based model, you pay for only the seconds you use a product. And this is from a pool of tens of thousands of servers running in a data center shared by all. On the weekend when the load is low, they turn these down to low power mode. Or at night, someone from the other side of the world gets to use the same compute power when you sleep.
With these simple optimizations the companies offering the shared data centers to you, can lower the cost, without you noticing and you save your money and the environment (our mother Earth, not dev-prod) is happy.

## The world of HDFS
With HDFS being a standard of data storage when it came to Big Data, lets try and understand what it had to offer and how it solved most problems.
 - It is a File System and has all characteristics of a File System.
 - Provides good durability which can be controlled based on the replication you configure.
 - Is consistent. You would know if a write is successful in the same request you are writing the file.
 - Can run in high availability mode, with fail over masters.
 - Reliable - Has a good reputation of being capable of storing peta bytes of data without corrupting it.
 - Listing files is a trivial operation.
 - Uses blocks underneath to store files. Though the block size is configured, its wasteful for files which are very small (few bytes / mega bytes)
 - Moving files is an atomic operation.
 - Organizes files into hierarchical folders.

## The world of Object Stores
The most popular data stores in the cloud are not files systems but object stores. An object is a set of bytes with some metadata, and files are stored as objects.
Data Storage for long term and sometime short term in the cloud is mostly based on Object Stores.
There are certain properties about object stores which makes it a very efficient solution for a large number of use cases while keeping costs low and only slightly compromising on certain characteristics.

Some of the common properties (ref. AWS S3):
 - Store data of any size up to a fixed upper limit (usually 5 TB)
 - Good for very small files as well as large.
 - Durability guarantees which is practically "forever".
 - Cost based on the exact amount of data stored.
 - Very highly available.
 - Options to create instances in the same region to process data in the same data center.
 - Listing files is a non-trivial operation, and becomes costly for buckets with large number of files.
 - Moving files is non atomic.
 - It does not have directory (folder) based hierarchical structure.
 - Keys can have similar prefixes with a '/' delimiter to make it look like folders, but there is no concept of folders.

## The amazing S3 from AWS
S3 is the offering from Amazon as part of the AWS and is probably the largest and most used Object Store in the internet today. So much so, that a small outage can take down literally half the internet.

The use of S3 is virtually unlimited and I have noticed it being successfully used to:
 - store user generated content (pdfs, file scans, images, thumbnails, videos etc)
 - store music or any form of digital media
 - host of static websites
 - store files and use CDN over S3 to deliver content
 - store web logs
 - store big data
 - as a data lake behind external tables of data warehouses (Hive)

However for companies running in Hyper Scale, and using object stores for data lake did face problems which required specialized solutions before being successful.

## The Caveats of S3
The most important things to keep in mind while using S3 is that:
 - It is an eventually consistent data store. (and there is no guarantee on when this eventual becomes consistent)
 - It is not good for storing temporary files (files written incrementally in between stages in a job / pipelines)
 - It not ready to use out of the box for use cases which involve multi step writes of data.

Note: There is Native S3, which solves a few more problems but is slow and has limitations on the file size. There is S3A, which is more suitable for a data lake however still has issues with consistency.

## What I learnt from using S3 as a Data Lake
It is super reliable and durable and truly no-ops. For using as a Big Data Lake, where you would have potentially hundreds of pipelines reading and writing data, which are more often than not connected to each other, you would face the consistency problem.
Non Atomic move / copy operation becomes an issue when used with Map Reduce, Spark, or other popular big data processing frameworks.

To get in detail, most data processing frameworks like Map Reduce and Spark first write data temporarily to the associated file system, and when the job is successfully completed, it moves the data to the correct folder, and creates a '_SUCCESS' file.
Since the move operation is built as atomic in HDFS, it runs as one call from one thread. In the Object Store world, this operation essentially means copy and delete. And copy here means copying the data by reading in memory and streaming to another file in S3. It is not a managed operation by S3, and hence is handled by the client. This also makes the operation run on the master which is performing the final commit operation and is therefore not distributed. For large data sets written directly from a Spark job or a MR job to S3 results in unusually slow commit times and also is prone to network failure.

One option to solve this problem is to use HDFS as a temporary store and finally have another job to copy the data to S3 (distcp).
The other options is to somehow have the listing and discovery of files handled separately and not use S3's features to list files. This is what Netflix did ([S3mper](https://github.com/Netflix/s3mper)) and partly Amazon does in the implementation of S3 consistent view in an EMRFS cluster.

## GCS from Google and how it compares
In the Google Cloud Platform world, it offers Google Cloud Storage (GCS) which is also an object store competing with S3 in the market.
However in terms of architecture, implementation and use, it is vastly different from Amazon S3.
For instance it provides solutions to a lot more problems in the consistency aspect of a data store when compared with Amazon S3:
 - GCS provides strong global consistency for read after write / delete vs eventually consistency for S3
 - Bucket and Object Listing are consistent GCS and not in S3
 - Atomic moves are not supported by GCS or S3

The first point means that in S3, you may not be able to list / read an object just milliseconds after having successfully written it. And you don't know your write was a success until you have read it, because replication is in process.
This also means that after delete, stale reads are possible. S3 offers no guarantees when a write or delete operation would be complete, and there is also no way to track that.

This makes using S3 as a data store to write files and make atomic commits very difficult. To over come this Netflix uses DyanamoDB to store a secondary index for managing files in the S3 bucket. The tool is [S3mper](https://github.com/Netflix/s3mper) and is open sourced.

## Conclusion
S3 is probably one of the best products solving a very wide variety of use cases. However before using this as the best alternative to HDFS there are certain things which should be kept in mind.
In this article I try to uncover some of the things I learnt while using S3 as a Data Lake.

## References
 - https://cloud.google.com/storage/docs/consistency
 - https://docs.aws.amazon.com/AmazonS3/latest/dev/Introduction.html#ConsistencyModel
 - https://github.com/Netflix/s3mper
