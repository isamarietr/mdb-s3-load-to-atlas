# Create an Atlas Search index with data loaded from AWS S3

A few options:

1. [Load using Atlas Data Federation](s3_to_cluster_via_data_federation)
2. [Pipe AWS S3 to MongoDB using mongoimport utility tool](s3_to_cluster_via_awscli_pipe_mongoimport)
3. Copy files from S3 locally and use the [mongoimport](https://www.mongodb.com/docs/database-tools/mongoimport/) utility tool
4. Read the data from S3 files and use the MongoDB Driver to do [`insertOne`](https://www.mongodb.com/docs/manual/reference/method/db.collection.insertOne/#examples) or [`insertMany`](https://www.mongodb.com/docs/manual/reference/method/db.collection.insertMany/#examples) operations

To load your data efficiently, paralellize/multi-thread the process regardless of which option you decide to use.

