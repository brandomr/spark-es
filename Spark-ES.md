# Integrating PySpark and Elasticsearch

Have you tried to make Spark and Elasticsearch play well together but run into snags? You're not alone. On [StackOverflow](https://stackoverflow.com/search?q=spark+elasticsearch) there are over 500 questions about integrating Spark and Elasticsearch. This post walks through how to do this seemlessly. We'll focus on doing this with PySpark as opposed to Spark's other APIs (Java, Scala, etc.).

## Elasticsearch-Hadoop

First, you need to ensure that the [Elasticsearch-Hadoop](https://www.elastic.co/products/hadoop) connector library is installed across your Spark cluster. It **must** be available at the same path on each of your nodes. You can install it with something like:

```
wget http://download.elastic.co/hadoop/elasticsearch-hadoop-6.1.1.zip
unzip elasticsearch-hadoop-6.1.1.zip
```

Now, when you run `spark-submit` ensure that the `elasticsearch-hadoop` jar is specified:

```
spark-submit --jars /path_to/elasticsearch-hadoop-6.1.1/dist/elasticsearch-spark-20_2.11-6.1.1.jar
```
Unfortunately, there is no `pip install`'ing `elasticsearch-hadoop`; you must have the binary available and the jar path specified for Spark. If you are running a Jupyter Notebook you can add the jar to the `environment` prior to establishing a Spark Context with:

```
import os  

# set environment variable PYSPARK_SUBMIT_ARGS
os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars elasticsearch-hadoop-6.1.1/dist/elasticsearch-spark-20_2.11-6.1.1.jar pyspark-shell'

# invoke SparkContext (using environment variable)
sc = SparkContext(appName="PythonSparkStreaming")  
```
Now, our SparkContext `sc` has access to the `elasticsearch-hadoop` library. Bingo!

## Writing to Elasticsearch

#### Specifying a configuration
To write an RDD to Elasticsearch you need to first specify a configuration. This includes the location of the Elasticsearch cluster's Master Node, it's port, and the `index` and `document type` you are writing to. There are numerous things that can be configured prior to writing to Elasticsearch but we'll save those for later. Your `conf` should look something like:

```
es_write_conf = {
	
	# specify the node that we are sending data to (this should be the master)
	"es.nodes" : 'localhost',
            
	# specify the port in case it is not the default port
	"es.port" : '9200',
            
	# specify a resource in the form 'index/doc-type'
	"es.resource" : 'testindex/testdoc',

	# is the input JSON?
	"es.input.json" : "yes",
            
	# is there a field in the mapping that should be used to specify the ES document ID
	"es.mapping.id": "doc_id"

}
```  

We have indicated that our Elasticsearch Master is at `localhost:9200`, we are writing to the `testindex` index with `testdoc` document type. We plan to write `JSON` and there is a field called `doc_id` in the `JSON` within our `RDD` which we wish to use for the Elasticsearch `document id`.

#### Prepping the data
Now, we need to ensure that our RDD has records of the type:

```
(0, "{'some_key': 'some_value', 'doc_id': 123}")
``` 

Note that we have an RDD of tuples. The first value in the tuple (the key) actually doesn't matter. `elasticsearch-hadoop` only cares about the value. You need to ensure that the value is a valid `JSON` string. So, you could try starting with data like:

```
data = [
		{'some_key': 'some_value', 'doc_id': 123},
		{'some_key': 'some_value', 'doc_id': 456},
		{'some_key': 'some_value', 'doc_id': 789}
		]

```

Now we can create an RDD from this:

```
rdd = sc.parallelize(data)	
```

Now we can define a function to format this into an `elasticsearch-hadoop` compatible RDD:

```
def format_data(x):
    return (data['doc_id'], json.dumps(data))

rdd = rdd.map(lambda x: format_data(x))
```
We have just set the `doc_id` to the first value in the tuple and the second value to be the `JSON` encoded string of the dictionary we began with. If we pass a Python dictionary to `elasticsearch-hadoop` it will puke. Make sure you've run `json.dumps()` on that record!

#### Executing the Write Operation
Now we're in business. You just need to execute the following `action` on your `RDD` to write it to Elasticsearch:

```
rdd.saveAsNewAPIHadoopFile(
	path='-',
	outputFormatClass="org.elasticsearch.hadoop.mr.EsOutputFormat",
	keyClass="org.apache.hadoop.io.NullWritable",
	valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable",

	# critically, we must specify our `es_write_conf` 
	conf=es_write_conf)
```                

## Reading from Elasticsearch
Now that we have some data in Elasticsearch, we can read it back in using the `elasticsearc-hadoop` connector.

```
es_read_conf = {
	
	# specify the node that we are sending data to (this should be the master)
	"es.nodes" : 'localhost',
            
	# specify the port in case it is not the default port
	"es.port" : '9200',
            
	# specify a resource in the form 'index/doc-type'
	"es.resource" : 'testindex/testdoc'
	
}
```

We're simply telling Spark to use the same Elasticsearch Master Node and resource set as the one we just wrote to. Now we can generate an RDD from this Elasticsearch data:

```
es_rdd = sc.newAPIHadoopRDD(
    inputFormatClass="org.elasticsearch.hadoop.mr.EsInputFormat",
    keyClass="org.apache.hadoop.io.NullWritable", 
    valueClass="org.elasticsearch.hadoop.mr.LinkedMapWritable", 
    conf=es_read_conf)

```

To test that everything worked, try running `es_rdd.take(1)` on the `RDD`. You should be returned something like `(123, "{'some_key': 'some_value', 'doc_id': 123}")`.

## Final Thoughts
The mechanics of reading and writing from Spark to Elasticseach can be confusing. Even more confusing are the performance tuning aspects of this. Spark is extremely powerful and can easily overwhelm an Elasticsearch cluster. Combined, these technologies power many of the largest tech companies in the world. If you're interested in learning more about how you can combine Spark and Elasticsearch or are facing issues with speed or reliability with your instantiation let us know!