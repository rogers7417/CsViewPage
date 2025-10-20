const { MongoClient } = require('mongodb');

let mongoClientPromise = null;

async function getMongoClient() {
    if (!process.env.SNAPSHOT_MONGO_URI) {
        throw new Error('SNAPSHOT_MONGO_URI is not configured');
    }

    if (!mongoClientPromise) {
        const client = new MongoClient(process.env.SNAPSHOT_MONGO_URI, {
            maxPoolSize: Number(process.env.SNAPSHOT_MONGO_POOL_SIZE || 5),
        });
        mongoClientPromise = client.connect().catch((err) => {
            mongoClientPromise = null;
            throw err;
        });
    }

    return mongoClientPromise;
}

async function initSnapshotMongo() {
    const client = await getMongoClient();
    const dbName = process.env.SNAPSHOT_MONGO_DB || 'opportunitySnapshot';
    const collectionName = process.env.SNAPSHOT_MONGO_COLLECTION || 'opportunitySnapshot';
    return client.db(dbName).collection(collectionName);
}

async function fetchLatestSnapshot() {
    const collection = await initSnapshotMongo();
    return collection.find().sort({ date: -1 }).limit(1).next();
}

module.exports = {
    initSnapshotMongo,
    fetchLatestSnapshot,
};
