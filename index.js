const express = require('express');
const cookieParser = require('cookie-parser');
const path = require('path');
const axios = require('axios');
const { fetchLatestSnapshot } = require('./services/snapshotStore');
const { generateLeadSummaryInsight } = require('./services/leadSummaryInsight');
require('dotenv').config();

const app = express();

app.use((req, res, next) => {
    const timestamp = new Date().toISOString();
    console.log(`[${timestamp}] ${req.method} ${req.originalUrl}`);
    next();
});
const csRouter = require('./routes/cs');

const apiBase = process.env.API_SERVER_URL || process.env.API_BASE_URL || 'http://localhost:4000';
const apiClient = axios.create({
    baseURL: apiBase.endsWith('/') ? apiBase : `${apiBase}/`,
    timeout: 30000,
});

app.use(cookieParser());
app.use(express.json({ limit: '1mb' }));
app.use('/cs', csRouter);
app.use('/cs/static', express.static(path.join(__dirname, 'views')));

// Proxy endpoints to align with API server routes.
app.get('/cs/contracts', async (req, res) => {
    try {
        const { data } = await apiClient.get('contracts', { params: req.query });
        res.json(data);
    } catch (err) {
        const status = err.response?.status || 500;
        const payload = err.response?.data || { error: 'Failed to fetch contracts' };
        console.error('GET /contracts proxy error', err.message || err);
        res.status(status).json(payload);
    }
});

async function handleLeadSummary(req, res) {
    try {
        const result = await generateLeadSummaryInsight(req.body || {});
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

app.post('/insights/lead-summary', handleLeadSummary);
app.post('/cs/insights/lead-summary', handleLeadSummary);

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

app.get('/snapshot/latest', handleSnapshotRequest);
app.get('/cs/snapshot/latest', handleSnapshotRequest);

app.listen(3003, () => {
    console.log('✅ 서버 실행 중: http://localhost:3003');
});
