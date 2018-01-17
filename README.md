# Integrating Spark and Elasticsearch
The [Elasticsearch for Apache Hadoop](https://www.elastic.co/guide/en/elasticsearch/hadoop/current/reference.html) library lets you connect your Elasticsearch cluster with a Hadoop cluster or a cluster Apache Spark or Apache Storm. Reading and writing data to and from Elasticsearch using Spark is the focus of this guide. If you follow along, you will learn how to generate random data that can be read by Spark Streaming, how to push this data to Elasticsearch, and how to then use Spark to read the data back out. Welcome comments/questions!

## Contents

1. `docker-compose.yml`: this file sets up the appropriate docker containers for the project. These include a Spark node, an Elasticsearch node, and a Kibana node.
2. `Spark-ES.ipynb`: this notebook runs the Spark code
3. `random_writer.ipynb`: this notebook generates random data to stream into Spark


## Running the Guide
First, run `docker-compose up` to spin up the Docker containers.

Next, in the Docker logs, you should see the URL and token for the Jupyter Notebook Server. It will look something like:

```
spark-node       |     Copy/paste this URL into your browser when you connect for the first time,
spark-node       |     to login with a token:
spark-node       |         http://localhost:8888/?token=6550c679badf9c9f823370381e5e941c691d8f7553e52efd
```

Now you can walk through the code in the notebooks. Make sure you run `random_writer.ipynb` before trying to stream data to Spark.

You can also check that data is written correctly to Elasticsearch using Kibana (accessible at `localhost:5601`.