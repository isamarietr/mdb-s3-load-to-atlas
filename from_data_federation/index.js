require('dotenv').config()
const { request } = require('urllib');
const mappings = require('../mappings.json');

const { MongoClient, ObjectId } = require('mongodb');

const uri = process.env.MDB_URI
const df_uri = process.env.DATA_FEDERATION_URI
const databaseName = process.env.TARGET_DATABASE
const collectionName = process.env.TARGET_COLLECTION
const virtualDatabase = process.env.SOURCE_VIRTUAL_DB
const virtualCollection = process.env.SOURCE_VIRTUAL_COLLECTION
const ATLAS_BASE_URL = process.env.ATLAS_BASE_URL
const ATLAS_API_VERSION = process.env.ATLAS_API_VERSION
const ATLAS_PROJECT_ID = process.env.ATLAS_PROJECT_ID
const ATLAS_CLUSTER_NAME = process.env.ATLAS_CLUSTER_NAME
const ATLAS_API_KEY = process.env.ATLAS_API_KEY
const MERGE_FIELD = "id"

async function main() {
  const client = new MongoClient(uri)
  const dataFedClient = new MongoClient(df_uri)

  try {
    // Connect to the MongoDB cluster
    await client.connect();

    const start = new Date().toLocaleString()
    console.log(`=> START TIME ${start}`);

    await createTargetCollection(client);

    const indexName = "default"

    await createSearchIndexes(mappings, indexName);

    await loadDataFromS3(dataFedClient);
    let end = new Date().toLocaleString()

    console.log(`=> DONE!!`);
    console.log(`=> START TIME ${start}`);
    console.log(`=> END TIME ${end}`);

  } finally {
    console.log(`closing connections`);
    // Close the connection to the MongoDB cluster
    await client.close();
    await dataFedClient.close();
  }
}

main().catch(console.error);

async function createTargetCollection(client) {

  const db = client.db(databaseName)
  // const existingCollections = await db.listCollections().toArray().map(e => e.name)
  // console.log(existingCollections);
  // if(existingCollections.includes(collectionName)){
  // await db.dropCollection(collectionName)
  // }
  const collection = await client.db(databaseName).createCollection(collectionName)
  collection.createIndex({"id": 1}, { unique: true })
}

async function createSearchIndexes(mappings, indexName) {
  const start = new Date()
  console.log(`====> CREATING ATLAS SEARCH INDEX - START TIME ${start.toLocaleString()}`);

  const url = `${ATLAS_BASE_URL}/api/atlas/${ATLAS_API_VERSION}/groups/${ATLAS_PROJECT_ID}/clusters/${ATLAS_CLUSTER_NAME}/fts/indexes`

  var content = JSON.stringify({
    "collectionName": collectionName,
    "database": databaseName,
    "mappings": mappings,
    "name": indexName
  });

  await request(url, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json'
    },
    content: content,
    digestAuth: ATLAS_API_KEY
  });

  const end = new Date()
  console.log(`====> CREATED ATLAS SEARCH INDEX - END TIME ${end.toLocaleString()}. Duration: ${(end.getTime() - start.getTime()) / 1000} seconds`);
}
async function loadDataFromS3(client) {
  const start = new Date()
  console.log(`====> LOADING DATA - START TIME ${start.toLocaleString()}`);

  const collection = client.db(virtualDatabase).collection(virtualCollection)
  const pipeline = [
    {
      '$merge': {
        'into': {
          'atlas': {
            'clusterName': ATLAS_CLUSTER_NAME, 
            'db': databaseName, 
            'coll': collectionName
          }
        }, 
        'on': 'id', 
        'whenMatched': 'replace', 
        'whenNotMatched': 'insert'
      }
    }
  ]
  
  await collection.aggregate(pipeline).toArray()

  const end = new Date()
  console.log(`====> LOADED DATA  - END TIME ${end.toLocaleString()}. Duration: ${(end.getTime() - start.getTime()) / 1000} seconds`);
}
