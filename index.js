require('dotenv').config();

const express = require('express');
const cookieParser = require('cookie-parser');
const path = require('path');
const { fetchLatestSnapshot } = require('./services/snapshotStore');
const { generateLeadSummaryInsight } = require('./services/leadSummaryInsight');
const { getContracts } = require('./services/contractService');
const { getToken } = require('./services/salesforceSession');

const app = express();

app.use((req, res, next) => {
    const timestamp = new Date().toISOString();
    console.log(`[${timestamp}] ${req.method} ${req.originalUrl}`);
    next();
});
const csRouter = require('./routes/cs');

app.use(cookieParser());
app.use(express.json({ limit: '1mb' }));
app.use('/cs/static', express.static(path.join(__dirname, 'views')));

async function handleContracts(req, res) {
    try {
        const token = getToken();
        if (!token?.access_token || !token?.instance_url) {
            return res.status(401).json({ error: 'Salesforce authentication required' });
        }
        const list = await getContracts(req.query || {}, token);
        res.setHeader('Content-Type', 'application/json; charset=utf-8');
        res.status(200).json(list);
    } catch (err) {
        const status = err.status || err.response?.status || 500;
        const message = err.message || err.response?.data || 'Failed to fetch contracts';
        console.error('GET /contracts error', { message, code: err.code, status });
        if (err.response?.data) {
            console.error('GET /contracts error payload', err.response.data);
        }
        res.status(status).json({ error: message, code: err.code || 'UNEXPECTED' });
    }
}

async function handleLeadSummary(req, res) {
    try {
        const token = getToken();
        if (!token?.access_token || !token?.instance_url) {
            return res.status(401).json({ error: 'Salesforce authentication required' });
        }

        const result = await generateLeadSummaryInsight(req.body || {}, token);
        res.setHeader('Content-Type', 'application/json; charset=utf-8');
        res.status(200).json(result);
    } catch (err) {
        const status = err.status || err.response?.status || 500;
        const message = err.message || err.response?.data || 'Failed to create lead summary insight';
        console.error('POST /insights/lead-summary error', {
            message,
            code: err.code,
            status,
            openai: err?.response?.status || null,
        });
        if (err.response?.data) {
            console.error('POST /insights/lead-summary error payload', err.response.data);
        }
        res.status(status).json({ error: message, code: err.code || 'UNEXPECTED' });
    }
}

// app.post('/insights/lead-summary', handleLeadSummary);
app.post('/cs/insights/lead-summary', handleLeadSummary);
// app.get('/contracts', handleContracts);
app.get('/cs/contracts', handleContracts);

app.use('/cs', csRouter);

function hasSnapshotAccess(req) {
    if (req.cookies?.sf_logged_in === '1') return true;

    const expectedKey = process.env.SNAPSHOT_ACCESS_KEY;
    if (!expectedKey) return false;

    const headerKey = req.get('x-snapshot-key') || req.get('x-api-key');
    return headerKey === expectedKey;
}

async function handleSnapshotRequest(req, res) {
    if (!hasSnapshotAccess(req)) {
        console.warn('[snapshot] unauthorized access', { path: req.originalUrl, cookies: req.cookies ? Object.keys(req.cookies) : [] });
        res.status(401).json({ error: 'Unauthorized' });
        return;
    }

    try {
        console.log('[snapshot] fetching latest snapshot', req.originalUrl);
        const doc = await fetchLatestSnapshot();
        if (!doc) {
            console.warn('[snapshot] document not found');
            res.status(404).json({ error: 'Snapshot not found' });
            return;
        }
        res.json(doc);
    } catch (err) {
        console.error('GET /snapshot/latest handler error', err.message || err);
        res.status(500).json({ error: err.message || 'Failed to fetch snapshot' });
    }
}

// app.get('/snapshot/latest', handleSnapshotRequest);
app.get('/cs/snapshot/latest', handleSnapshotRequest);

app.listen(3003, () => {
    console.log('✅ 서버 실행 중: http://localhost:3003');
});
