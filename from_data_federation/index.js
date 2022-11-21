require('dotenv').config()
const { request } = require('urllib');
const mappings = require('../mappings.json');

const { MongoClient } = require('mongodb');

const MDB_URI = process.env.MDB_URI
const DATA_FEDERATION_URI = process.env.DATA_FEDERATION_URI
const TARGET_DATABASE = process.env.TARGET_DATABASE
const TARGET_COLLECTION = process.env.TARGET_COLLECTION
const SOURCE_VIRTUAL_DB = process.env.SOURCE_VIRTUAL_DB
const SOURCE_VIRTUAL_COLLECTION = process.env.SOURCE_VIRTUAL_COLLECTION
const ATLAS_BASE_URL = process.env.ATLAS_BASE_URL
const ATLAS_API_VERSION = process.env.ATLAS_API_VERSION
const ATLAS_PROJECT_ID = process.env.ATLAS_PROJECT_ID
const ATLAS_CLUSTER_NAME = process.env.ATLAS_CLUSTER_NAME
const ATLAS_API_KEY = process.env.ATLAS_API_KEY

async function main() {
  // connection to target cluster
  const client = new MongoClient(MDB_URI)
  // connection to source data federation
  const dataFedClient = new MongoClient(DATA_FEDERATION_URI)

  try {
    // Connect to the MongoDB cluster
    await client.connect();

    const start = new Date().toLocaleString()
    console.log(`=> START TIME ${start}`);

    await createTargetCollection(client);
    await createSearchIndexes(mappings);
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

/**
 * Create the target collection
 * @param {*} client 
 */
async function createTargetCollection(client) {
  await client.db(TARGET_DATABASE).createCollection(TARGET_COLLECTION)
}

/**
 * Create Atlas Search index with specified mappings definition
 * @param {*} mappings 
 */
async function createSearchIndexes(mappings) {
  const start = new Date()
  console.log(`====> CREATING ATLAS SEARCH INDEX - START TIME ${start.toLocaleString()}`);

  const url = `${ATLAS_BASE_URL}/api/atlas/${ATLAS_API_VERSION}/groups/${ATLAS_PROJECT_ID}/clusters/${ATLAS_CLUSTER_NAME}/fts/indexes`
  var content = JSON.stringify({
    "TARGET_COLLECTION": TARGET_COLLECTION,
    "database": TARGET_DATABASE,
    "mappings": mappings,
    "name": "default"
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

/**
 * Run aggregation pipeline to output data to Atlas cluster
 * @param {*} client 
 */
async function loadDataFromS3(client) {
  const start = new Date()
  console.log(`====> LOADING DATA - START TIME ${start.toLocaleString()}`);

  const collection = client.db(SOURCE_VIRTUAL_DB).collection(SOURCE_VIRTUAL_COLLECTION)
  const pipeline = [
    // {
    //   '$match': {
    //     //some condition here. helpful for parallelizing load
    //   }
    // },
    {
      '$out': {
        'atlas': {
          'clusterName': ATLAS_CLUSTER_NAME,
          'db': TARGET_DATABASE,
          'coll': TARGET_COLLECTION
        }
      }
    }
  ]

  await collection.aggregate(pipeline).toArray()

  const end = new Date()
  console.log(`====> LOADED DATA  - END TIME ${end.toLocaleString()}. Duration: ${(end.getTime() - start.getTime()) / 1000} seconds`);
}
