require('dotenv').config()

const { MongoClient } = require('mongodb');
const AWS = require('aws-sdk');

// source
const AWS_ACCESS_KEY_ID = process.env.AWS_ACCESS_KEY_ID
const AWS_SECRET_ACCESS_KEY = process.env.AWS_SECRET_ACCESS_KEY
const AWS_S3_BUCKET_NAME = process.env.AWS_S3_BUCKET_NAME
const AWS_S3_BUCKET_PREFIX = process.env.AWS_S3_BUCKET_PREFIX

// target
const MDB_URI = process.env.MDB_URI
const TARGET_DATABASE = process.env.TARGET_DATABASE
const TARGET_COLLECTION = process.env.TARGET_COLLECTION

async function main() {
  // connection to target cluster
  const client = new MongoClient(MDB_URI)
  // connection to aws sdk
  const s3_client = new AWS.S3({
    accessKeyId: AWS_ACCESS_KEY_ID,
    secretAccessKey: AWS_SECRET_ACCESS_KEY
  });
 
  try {
    // Connect to the MongoDB cluster
    await client.connect();

    const start = new Date().toLocaleString()
    console.log(`=> START TIME ${start}`);

    await loadDataFromS3(s3_client);

    let end = new Date().toLocaleString()
    console.log(`=> DONE!!`);
    console.log(`=> START TIME ${start}`);
    console.log(`=> END TIME ${end}`);

  } finally {
    console.log(`closing connections`);
    // Close the connection to the MongoDB cluster
    await client.close();
  }
}

main().catch(console.error);

/**
 * Read AWS files and insert data to Atlas 
 * @param {*} s3_client 
 */
async function loadDataFromS3(s3_client) {

  const collection = client.db(TARGET_DATABASE).collection(TARGET_COLLECTION);
  const params = { 
    Bucket: AWS_S3_BUCKET_NAME,
    Prefix: AWS_S3_BUCKET_PREFIX
  };
  const objects = await s3_client.listObjects(params).promise();

  for (const object of objects.Contents) {
    // Download the file from S3
    const fileParams = {
      Bucket: S3_BUCKET_NAME,
      Key: object.Key
    };
    const fileData = await s3_client.getObject(fileParams).promise();

    // Parse the file data (assuming it's JSON)
    const jsonData = JSON.parse(fileData.Body.toString());

    // DO SOMETHING HERE WITH JSON DATA IF YOU WANT TO PROCESS THE CONTENTS OF THE FILE
    
    // Insert the data into MongoDB collection
    await collection.insertOne(jsonData);

    console.log(`Inserted file ${object.Key} into MongoDB`);
  }
}