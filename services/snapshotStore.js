const { MongoClient } = require('mongodb');

let mongoClientPromise = null;

async function getMongoClient() {
    const uri = process.env.SNAPSHOT_MONGO_URI;
    if (!uri) {
        console.error('[snapshotStore] SNAPSHOT_MONGO_URI is not configured');
        throw new Error('SNAPSHOT_MONGO_URI is not configured');
    }

    if (!mongoClientPromise) {
        console.info('[snapshotStore] connecting to MongoDB', { uri });
        const client = new MongoClient(uri, {
            maxPoolSize: Number(process.env.SNAPSHOT_MONGO_POOL_SIZE || 5),
        });
        mongoClientPromise = client.connect().catch((err) => {
            console.error('[snapshotStore] MongoDB connection failed', err);
            mongoClientPromise = null;
            throw err;
        });
    }

    return mongoClientPromise;
}

async function initSnapshotMongo() {
    const dbName = process.env.SNAPSHOT_MONGO_DB || 'salesforeLighting';
    const collectionName = process.env.SNAPSHOT_MONGO_COLLECTION || 'opportunitySnapshot';
    try {
        const client = await getMongoClient();
        console.info('[snapshotStore] using collection', { dbName, collectionName });
        return client.db(dbName).collection(collectionName);
    } catch (err) {
        console.error('[snapshotStore] initSnapshotMongo error', err);
        throw err;
    }
}

async function fetchLatestSnapshot() {
    try {
        const collection = await initSnapshotMongo();
        const doc = await collection.find().sort({ date: -1 }).limit(1).next();
        if (!doc) {
            console.warn('[snapshotStore] latest snapshot not found');
        }
        return doc;
    } catch (err) {
        console.error('[snapshotStore] fetchLatestSnapshot error', err);
        throw err;
    }
}

module.exports = {
    initSnapshotMongo,
    fetchLatestSnapshot,
};
