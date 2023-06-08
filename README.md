# Load data from AWS S3 to Atlas

A few options:

1. [Load using Atlas Data Federation](s3_to_cluster_via_data_federation)
2. [Pipe AWS S3 to MongoDB using mongoimport utility tool](s3_to_cluster_via_awscli_pipe_mongoimport)
3. [Read files from S3 and insert them using the MongoDB driver](s3_to_cluster_via_driver)
4. Copy files from S3 locally and use the [mongoimport](https://www.mongodb.com/docs/database-tools/mongoimport/) utility tool

To load your data efficiently, paralellize/multi-thread the process regardless of which option you decide to use.