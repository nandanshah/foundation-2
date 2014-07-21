ElasticSearch-MovieSimilarity
=============================

This application finds movies similar to a given movie with the use of more-like-this-field(mlt-field) query in ElasticSearch. This is further paralleled through the use of SPARK. SPARK adds parallelism to the similarity finding task through the use of Resilient Distributed dataset (RDD). ElasticSearch provides a ESInputFormat which is a hadoop input format. This can be used to create an RDD in SPARK through the use of Hadoop RDD. The similarity result is written to Cassandra database. 
> The [more like this field query] in ElasticSearch lets you 
> find document similar to a given document. The similarity 
> is based on customizable field and other parameter which
> filters the documents returned.

***Cassandra Schema***
```sh
CREATE KEYSPACE IF NOT EXISTS catalog WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS similarmovies( movieid TEXT PRIMARY KEY, reco LIST<TEXT>);
```
The similarmovies column family stores similarity results. movieid is the primary key which is the movie id and reco contains movie ids similar to the movie as returned by elasticsearch.

The SPARK,CASSANDRA, and ElasticSearch parameters file are stored in src/main/resources folder 
* src/main/resources/Cassandra.properties : Cassandra configuration parameters.
* src/main/resources/ES.properties: ElasticSearch configuration parameters.
* src/main/resources/Spark.properties: SPARK configuration parameters.

[more like this field query]:http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/query-dsl-mlt-field-query.html#query-dsl-mlt-field-query