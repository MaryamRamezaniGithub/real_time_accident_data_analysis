# real_time_accident_data_analysis
# Introduction
Data streaming is a popular topic in today's data engineering world. If you've been reading data-related articles on Medium or looking for jobs on LinkedIn, you've probably come across it. It's usually mentioned in job requirements like Kafka, Flink, Spark, or other big data tools.

These tools are used by major companies to improve their data processing abilities. Recently, I've been learning about IT in the Brazilian public sector. This got me thinking about how data streaming can help not just companies, but also society.

I think the best way to learn these tools and test my ideas is by working on a real (or almost real) project. In this post, we will look at using ksqlDB (a tool related to Kafka) to make it easier to analyze road accident data, using real data from the Brazilian Federal Highway Police.

We'll create a data pipeline inspired by the Medallion Architecture to allow for ongoing (and possibly real-time) analysis of accident data.

Our goal is to learn more about data streaming, Kafka, and ksqlDB.

I hope you enjoy this project!

# Problem
The Brazilian Federal Highway Police (PRF) is responsible for monitoring the highways. Each year, they collect and release data on traffic accidents (under a CC BY-ND 3.0 License). This data includes information about the victims (age, gender, condition), weather conditions, time and location, causes, and effects of the accidents.

I first encountered this dataset during an undergraduate course, and it proved to be a great learning resource for ETL (Extract, Transform, Load) processes. The dataset is rich in information but problematic in terms of formatting. It has many missing values, inconsistent data types, varying column formats, non-standardized values, typos, and other issues.

Now, let's suppose the government wants to create better accident prevention policies. To do this, they need to answer the following questions:

How many people are involved in accidents each month?
What are the counts and percentages of people who are unhurt, lightly injured, severely injured, or dead each month?
What are the percentages of people involved in accidents by gender each month?
What is the death rate for each type of accident?
However, waiting for the yearly report isn't feasible, so we need to set up an incremental report that updates in real-time by communicating with their internal database. This way, the results shown on dashboards will be updated as soon as new accidents are recorded.

Since the data entered into the system may have similar issues as the released datasets, we need to clean and transform the records to make them useful for the final report.

This is where ksqlDB comes into play.

# The role of Apache Kafka and ksqlDB
Apache Kafka is an open-source distributed event streaming platform used for sending and receiving messages. In Kafka, messages are organized into topics. These topics contain messages (essentially strings of bytes) that are written by producers and read by consumers.

Kafka is a crucial tool for data streaming, particularly in Big Data contexts, as it can handle millions of messages with high throughput. Companies like Uber and Netflix use Kafka to enhance their data processing capabilities, enabling real-time data analysis and machine learning.

ksqlDB is a database specifically designed for stream processing applications on top of Apache Kafka. It allows us to treat Kafka topics like traditional tables in relational databases, enabling SQL-like queries on streaming data. This makes it easier to process and analyze data in real-time.

<<<<<<< HEAD
![](C:\Users\Mediamonster\Downloads\1.jpg)
=======
![1](https://github.com/user-attachments/assets/e88a12cc-09f5-4949-9c2b-c93a3b98c051)
>>>>>>> 5096ae0cff20af843961a241351479b4667804ed

ksqlDB's storage is based on two main structures: Streams and Tables.

**Streams** are similar to standard Kafka topics, functioning as immutable, append-only collections. They are essentially ever-growing lists of messages, making them suitable for representing historical sequences of events, such as bank transactions.

**Tables**, in contrast, are mutable collections that represent the current state or snapshot of a dataset. They use primary keys to manage data. When a table receives messages, it updates to store only the latest value for each key, reflecting the most recent state.

![](C:\Users\Mediamonster\Downloads\2.jpg)

Despite their differences, Streams and Tables in ksqlDB are both based on Kafka's basic topic structure.

ksqlDB is fully SQL-based, so unless you're working on a very specialized task, you don't need any additional programming languages. This means that if you already have SQL experience, you can easily transition from a traditional relational environment to a streaming environment with minimal effort.

While similar functionality can be achieved with other tools like Apache Spark or manually-coded consumers, ksqlDB stands out for its simplicity and beginner-friendly interface.

### The Implementation

The main idea of this project is to use ksqlDB to create a streaming ETL pipeline. The pipeline will follow the Medallion Architecture, which organizes data into progressively refined states: bronze, silver, and gold.

- **Bronze Layer:** This layer stores the raw data.
- **Silver Layer:** This layer contains the cleaned data.
- **Gold Layer:** This layer holds the enriched and aggregated data.

For long-term storage, we’ll be using MongoDB.

![](C:\Users\Mediamonster\Downloads\3.jpg)

In addition to Streams and Tables, we’ll also use database connectors to move data between layers. These connectors manage the transfer of records from a database (in this case, MongoDB) to Kafka topics, a process known as Change Data Capture (CDC), and vice versa.

I already transformed the data from 2015–2020 into a *parquet* file, to reduce its size and improve reading time, this file will be available at the GitHub repository.

### Setting Up the Environment

To set up the environment for this project, follow the instructions from the official ksqlDB tutorial's Docker files.

**What you need:**

1. **Docker and Docker Compose**: Ensure both are installed on your system.

2. MongoDB Sink and Source Connectors for Kafka

   You will need:

   - Debezium MongoDB CDC Source Connector
   - MongoDB Connector (sink)

3. **(Optional) Python 3.8+**: This is used for inserting entries into MongoDB.

**Instructions:**

- Place the downloaded connectors in the `/plugins` folder, which should be in the same directory as your `docker-compose` file.

  Then, the containers can be started normally with *docker-compose up.*

![](C:\Users\Mediamonster\Downloads\4.jpg)

After that, connect to the MongoDB shell with the command *mongo -u mongo -p mongo* inside the container and start the database with *rs.initiate()*.

## Bronze Layer for Extracting the raw data

The bronze layer stores the raw data extracted from the transactional environment, without any transformation or cleaning, just a *ctrl+c ctrl+v* process. In our case, this layer should extract information from the database where the accidents are originally registered.

For simplicity, we’ll create the records directly on the bronze layer.

This layer will be represented by a MongoDB collection named *accidents_bronze* inside the *accidents* database.

![](C:\Users\Mediamonster\Downloads\5.jpg)

To move records from MongoDB to ksqlDB, you'll need to configure a source connector. This connector monitors the MongoDB collection and streams any changes—such as insertions, deletions, and updates—as structured messages (in AVRO or JSON) to a Kafka topic.

**Steps to Start:**

1. **Configure the Source Connector**: Set up the source connector to watch the MongoDB collection and send changes to a Kafka topic.

2. **Connect to the ksqlDB Server**: Use the ksqlDB client to connect to the server instance. You can do this by running the following Docker command:

     docker exec -it <ksqlDB_container_name> ksql http://localhost:8088

   Replace `<ksqlDB_container_name>` with the name of your ksqlDB container. If everything goes well, you should see a big KSQLDB on your screen with a ‘RUNNING’ message.

   ![](C:\Users\Mediamonster\Downloads\6.jpg)

   Before continuing, we need to run the following command

     SET 'auto.offset.reset' = 'earliest';

Then, creating a connector is just a matter of describing some configurations.

![](C:\Users\Mediamonster\Downloads\7.jpg)

The command opens with the CREATE SOURCE CONNECTOR clause, followed by the connector name and configurations. The WITH clause specifies the configurations used.

First, the *connector.class* is defined. This is the connector itself, the Java class that implements its logic. We’ll be using the Debezium MongoDB connector, which was included in the plugins folder earlier.

Second, we pass the MongoDB address (host + name) and credentials (login + password).

Then, we define which collections in our database will be watched.

Finally, the *transforms* parameter specifies a simplification in the messages produced by the Debezium connector and the *errors.tolerance* defines the connector behavior for messages that produce errors (the default behavior is to halt the execution).

![](C:\Users\Mediamonster\Downloads\8.jpg)

With the connector created, let’s execute a DESCRIBE CONNECTOR query to see its current status. Any errors that occur in its execution should be prompted here.

![](C:\Users\Mediamonster\Downloads\9.jpg)

Now that our connector is running, it will start streaming all the changes in the *accidents_bronze* collection to the topic
**replica-set.accidents.accidents_bronze**.

ksqlDB is not able to process Kafka topics directly, so we need to define a STREAM using it.

Defining a STREAM in ksqlDB is almost equal to creating a table in SQL. You need to pass a name, a list of columns with their respective types, and some configurations in the WITH clause.

In our case, we need to configure which topic will feed our stream. Because of that, the column’s names and types should match the fields in the original topic messages.

![](C:\Users\Mediamonster\Downloads\10.jpg)

![](C:\Users\Mediamonster\Downloads\11.jpg)

### Recap of What We’ve Done So Far

1. **Configured a MongoDB Source Connector**: We set up the connector to stream changes from the `accidents_bronze` collection as structured messages.
2. **Defined a STREAM in ksqlDB**: We created a STREAM called `ACCIDENTS_BRONZE_STREAM` using the Kafka topic `replica-set.accidents.accidents_bronze`, enabling us to process and query the streaming data.

With the STREAM now set up, you can run `SELECT` queries to analyze the data. This is where the real power of ksqlDB comes into play, allowing for dynamic and real-time data analysis.

For example, let’s select the *data* and *id* of each message.

![](C:\Users\Mediamonster\Downloads\12.jpg)

![](C:\Users\Mediamonster\Downloads\13.jpg)

In ksqlDB, these normal SQL statements are called [PULL QUERIES](https://docs.ksqldb.io/en/latest/developer-guide/ksqldb-reference/select-pull-query/), because they return a response based on the stream's current state and finishes.

By adding EMIT CHANGES at the end of a PULL QUERY it is turned into a [PUSH QUERIES](https://docs.ksqldb.io/en/latest/developer-guide/ksqldb-reference/select-push-query/). Unlike its counterpart, it never finishes, and it is always computing new rows based on the arriving messages. Let’s see this working.

![](C:\Users\Mediamonster\Downloads\14.jpg)

On the left, we have the PUSH QUERY and on the right, a simple python script inserting records in MongoDB. As new records are inserted, they automatically pop up in the query’s answer.

Push and Pull queries will be our building blocks to create all the transformations needed in the following sections.

## Moving to the Silver Layer for Cleaning the data

The goal of the Silver Layer is to store cleansed data that is easy for other applications, such as machine learning projects and the Gold Layer, to consume. This layer will be represented by a MongoDB collection named `accidents_silver` in the `accidents` database.

Our main objective is to ensure the data is correctly formatted so that downstream tasks can focus on their specific business rules without worrying about data quality.

To achieve this, follow these two steps:

1. **Create a Stream to Clean the Messages**: Define a new STREAM in ksqlDB that processes and cleans the incoming messages from the bronze stream. This step will ensure the data is formatted correctly.

2. **Create a Sink Connector to Save the Messages**: Set up a sink connector to write the cleaned messages into the `accidents_silver` collection in MongoDB.

   

Now the true power of ksqlDB is explored — Stream processing.

It’s possible to define a stream using a query made over other streams. The new stream will be populated with the query result.

Let’s see this working.

For example, if we want a new STREAM containing only the *_id* and *date* where it is not null, we could make this:

![](C:\Users\Mediamonster\Downloads\15.jpg)

Using this functionality, it’s possible to create a transformation (*bronze_to_silver*) STREAM that is responsible for selecting and cleaning the messages from the bronze stream.

![](C:\Users\Mediamonster\Downloads\16.jpg)

Our example needs to clean the fields: **sexo** (gender), **tipo_acidente** (accident type), **ilesos** (unhurt), **feridos_leves** (lightly injured), **feridos_graves** (strongly_injured), **mortos** (dead), and **data_inversa** (date).

After looking into the database (I’ve made this offscreen), it is possible to note the following problems:

1. The gender column contains multiple values representing the same gender: male could be either ‘*masculino*’ or ‘*m*’, and female could be either ‘*feminino*’ or ‘*f*’.
2. The accident type also contains multiple values for the ‘same type’.
3. The date could be formatted in one of the following ways: 2019–12–20, 20/12/2019 or 20/12/19
4. Missing values are encoded as NULL, the string ‘NULL’ or the string ‘(null)’

*Besides these transformations, I gonna also (try to) translate the fields and values to ease comprehension.*

The fix to these problems are implemented in the **accidents_bronze_to_silver** STREAM defined below:

![](C:\Users\Mediamonster\Downloads\17.jpg)

We’re able to build a powerful transformation process over a stream of messages with (almost) only SQL knowledge!

![](C:\Users\Mediamonster\Downloads\18.jpg)

The final step is to save the data in MongoDB using a **Sink Connector**.

![](C:\Users\Mediamonster\Downloads\19.jpg)

For the connector above, the Kafka MongoDB connector is used, and the rest of the configurations are self-explanatory.

The *accidents_silver* is automatically created, and the results can be seen below.

![](C:\Users\Mediamonster\Downloads\real_time_accident_data_analysis\20.jpg)

## **Gold Layer for Business Rules and Aggregations**

The gold layer contains business-specific rules, focusing on solving the problems of a specific project.

In our project, two ‘Gold Layers’ will be defined, one focusing on answering the monthly-aggregated questions, and another answering the death rates of each accident, each stored in a separate collection.

From an architectural perspective, this step brings no novelty. It’s just like the previous step, where we consume data from a stream, transformed it to our needs, and saved the results in the database.

What makes this step different are the **aggregations** needed.

To answer our questions, we do not need to store every single accident, only the current count of deaths and injuries accidents for each month (*example*). So, instead of using STREAMS, we’ll be using TABLES.

Luckily, in terms of syntax, there is not much difference between table and stream definitions.

In ksqlDB, aggregations can only be made in PUSH QUERIES, so ‘EMIT CHANGES’ is needed on the query’s end.

Let’s start with the monthly aggregated table.

![](C:\Users\Mediamonster\Downloads\21.jpg)

As new records are inserted, the table  automatically updates the counts of each month. Let’s see the results closely.

![](C:\Users\Mediamonster\Downloads\22.jpg)

The same logic goes for the death rate table, where we calculate the probability of dying in each type of accident.

![](C:\Users\Mediamonster\Downloads\23.jpg)

![](C:\Users\Mediamonster\Downloads\24.jpg)

Finally, all that rest is to save each table in their respective MongoDB collection.

![](C:\Users\Mediamonster\Downloads\25.jpg)

This sink connector has some different configurations (*transforms* and *document.id.strategy*) used to create an *_id* field in MongoDB matching the table’s primary key.

![](C:\Users\Mediamonster\Downloads\26.jpg)

And the results should start showing up in the collections.

![](C:\Users\Mediamonster\Downloads\27.jpg)

### Conclusion

`Stream` processing is a key reality for many companies today, particularly among major industry players. Apache Kafka stands out as a leading technology for streaming messages between applications and databases, supported by a broad ecosystem of tools for managing intensive data tasks. ksqlDB is a significant part of this ecosystem.

In this post, we explored ksqlDB through a hands-on project using a real dataset. We used a dataset from the Brazilian Open Government Data on road accidents to demonstrate how to apply these concepts in a practical scenario.

We employed the Medallion Architecture to transform raw, unformatted data into actionable insights, enabling incremental (and potentially real-time) data analysis. This architecture was chosen to showcase different aspects of ksqlDB and its capabilities.

Throughout the project, we learned about ksqlDB’s main storage units—streams and tables—how to perform push and pull queries, and how this tool can address a real-world data engineering problem.

As always, I’m not an expert on every topic covered, and I encourage further exploration and reading. Refer to the sources below for more information.

Thank you for reading! 😉

# References

> *All the code is available in this* [*GitHub repository*](https://github.com/jaumpedro214/posts/tree/main/real_time_analysis_accidents)*.*
>
> 

[1] [Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture) — Databricks Glossary

[2] [What is the medallion lakehouse architecture?](https://learn.microsoft.com/en-us/azure/databricks/lakehouse/medallion) — Microsoft Learn

[3] [Streaming ETL pipeline ](https://docs.ksqldb.io/en/latest/tutorials/etl/)— ksqlDB official documentation

[4] [Streams and Tables ](https://developer.confluent.io/learn-kafka/ksqldb/streams-and-tables/)— Confluent ksqlDB tutorial

[5] [Featuring Apache Kafka in the Netflix Studio and Finance World ](https://www.confluent.io/blog/how-kafka-is-used-by-netflix/)— Confluent blog

[6] [MongoDB Kafka Sink connector](https://www.mongodb.com/docs/kafka-connector/current/sink-connector/) — Official Mongo Docs

[7] [MongoDB source and sink connector](https://www.confluent.io/hub/mongodb/kafka-connect-mongodb) — Confluent Hub

[8] [Sink Connector Post Processors, configure document id in sink connector ](https://www.mongodb.com/docs/kafka-connector/current/sink-connector/fundamentals/post-processors/#configure-the-document-id-adder-post-processor)— Official Mongo Docs

[9] [Presto® on Apache Kafka® At Uber Scale ](https://www.uber.com/en-TT/blog/presto-on-apache-kafka-at-uber-scale/)— Uber Blog
