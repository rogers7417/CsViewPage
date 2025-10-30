const { MongoClient } = require('mongodb');

let mongoClientPromise = null;
let partnersCollectionPromise = null;

async function getMongoClient() {
  const uri =
    process.env.PARTNERS_LEADS_MONGO_URI ||
    process.env.SNAPSHOT_MONGO_URI ||
    process.env.MONGO_URI;

  if (!uri) {
    console.error('[partnersLeadStore] Mongo URI is not configured');
    throw new Error('PARTNERS_LEADS_MONGO_URI is not configured');
  }

  if (!mongoClientPromise) {
    const poolSize =
      Number(process.env.PARTNERS_LEADS_MONGO_POOL_SIZE) ||
      Number(process.env.SNAPSHOT_MONGO_POOL_SIZE) ||
      5;

    const client = new MongoClient(uri, { maxPoolSize: poolSize });
    mongoClientPromise = client.connect().catch((err) => {
      console.error('[partnersLeadStore] MongoDB connection failed', err);
      mongoClientPromise = null;
      throw err;
    });
  }

  return mongoClientPromise;
}

async function getPartnersCollection() {
  if (!partnersCollectionPromise) {
    partnersCollectionPromise = (async () => {
      const client = await getMongoClient();
      const dbName =
        process.env.PARTNERS_LEADS_MONGO_DB ||
        process.env.SNAPSHOT_DB_NAME ||
        process.env.SNAPSHOT_MONGO_DB ||
        'salesforeLighting';
      const collectionName =
        process.env.PARTNERS_LEADS_COLLECTION || 'partners_and_leads';

      console.info('[partnersLeadStore] using collection', { dbName, collectionName });
      return client.db(dbName).collection(collectionName);
    })().catch((err) => {
      console.error('[partnersLeadStore] collection init failed', err);
      partnersCollectionPromise = null;
      throw err;
    });
  }

  return partnersCollectionPromise;
}

async function fetchAllPartnersAndLeads() {
  try {
    const collection = await getPartnersCollection();
    const docs = await collection.find({}).toArray();
    return docs.map((doc) => {
      const { _id, ...rest } = doc;
      return {
        _id: _id ? String(_id) : null,
        ...rest,
      };
    });
  } catch (err) {
    console.error('[partnersLeadStore] fetchAllPartnersAndLeads error', err);
    throw err;
  }
}

module.exports = {
  fetchAllPartnersAndLeads,
};
