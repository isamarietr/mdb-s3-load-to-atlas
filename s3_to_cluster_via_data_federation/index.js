require('dotenv').config()

const { MongoClient } = require('mongodb');

// source
const DATA_FEDERATION_URI = process.env.DATA_FEDERATION_URI
const SOURCE_VIRTUAL_DB = process.env.SOURCE_VIRTUAL_DB
const SOURCE_VIRTUAL_COLLECTION = process.env.SOURCE_VIRTUAL_COLLECTION

// target
const TARGET_CLUSTER_NAME = process.env.TARGET_CLUSTER_NAME
const TARGET_DATABASE = process.env.TARGET_DATABASE
const TARGET_COLLECTION = process.env.TARGET_COLLECTION

async function main() {
  // connection to source data federation
  const dataFedClient = new MongoClient(DATA_FEDERATION_URI)

  try {
    const start = new Date().toLocaleString()
    console.log(`=> START TIME ${start}`);

    await loadDataFromS3(dataFedClient);

    let end = new Date().toLocaleString()
    console.log(`=> DONE!!`);
    console.log(`=> START TIME ${start}`);
    console.log(`=> END TIME ${end}`);

  } finally {
    console.log(`closing connections`);
    // Close the connections
    await dataFedClient.close();
  }
}

main().catch(console.error);

/**
 * Run aggregation pipeline to output data to Atlas cluster
 * @param {*} dataFedClient 
 */
async function loadDataFromS3(dataFedClient) {
  const start = new Date()
  console.log(`====> LOADING DATA - START TIME ${start.toLocaleString()}`);

  const virtual_collection = dataFedClient.db(SOURCE_VIRTUAL_DB).collection(SOURCE_VIRTUAL_COLLECTION)
  const pipeline = [
    // some condition here. helpful for parallelizing load, 
    // you can read these from command line parameters
    // {
    //   '$match': {
    //      'df_filename': [ 'sample.json' ]
    //   }
    // },
    {
      '$out': {
        'atlas': {
          'clusterName': TARGET_CLUSTER_NAME,
          'db': TARGET_DATABASE,
          'coll': TARGET_COLLECTION
        }
      }
    }
  ]

  // toArray is needed to trigger an I/O request, since agg pipeline has $out,
  // it does not load results to memory. The agg pipeline is just executed.
  await virtual_collection.aggregate(pipeline).toArray()

  const end = new Date()
  console.log(`====> LOADED DATA  - END TIME ${end.toLocaleString()}. Duration: ${(end.getTime() - start.getTime()) / 1000} seconds`);
}
