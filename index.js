const express = require('express');
const cookieParser = require('cookie-parser');
const path = require('path');
const axios = require('axios');
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
app.use('/cs', csRouter);
app.use('/cs/static', express.static(path.join(__dirname, 'views')));

// Proxy endpoints to align with API server routes.
app.get('/contracts', async (req, res) => {
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

app.get('/snapshot/latest', async (req, res) => {
    try {
        const { data } = await apiClient.get('snapshot/latest');
        res.json(data);
    } catch (err) {
        const status = err.response?.status || 500;
        const payload = err.response?.data || { error: 'Failed to fetch snapshot' };
        console.error('GET /snapshot/latest proxy error', err.message || err);
        res.status(status).json(payload);
    }
});

app.listen(3003, () => {
    console.log('✅ 서버 실행 중: http://localhost:3003');
});
