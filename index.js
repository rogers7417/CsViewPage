const express = require('express');
const axios = require('axios');
const cookieParser = require('cookie-parser');
const path = require('path');
require('dotenv').config();

const app = express();

// 정적 파일 서빙: /cs 경로에서 제공
app.use('/cs', express.static(__dirname));
app.use(cookieParser());

let tokenCache = null;

// 진입점
app.get('/cs', (req, res) => {
    res.redirect('/cs/login');
});

app.get('/cs/login', (req, res) => {
    res.sendFile(path.join(__dirname, 'login.html'));
});

app.get('/cs/storeList', (req, res) => {
    console.log("cs/storeList 요청");
    res.sendFile(path.join(__dirname, 'index.html'));
});

app.get('/cs/callback', async (req, res) => {
    const code = req.query.code;
    if (!code) return res.status(400).send('Missing authorization code');

    try {
        const tokenRes = await axios.post('https://login.salesforce.com/services/oauth2/token', null, {
            params: {
                grant_type: 'authorization_code',
                code,
                client_id: process.env.SF_CLIENT_ID,
                client_secret: process.env.SF_CLIENT_SECRET,
                redirect_uri: process.env.SF_REDIRECT_URI
            }
        });

        const { access_token, instance_url } = tokenRes.data;

        tokenCache = { access_token, instance_url };
        res.cookie('sf_logged_in', '1', { maxAge: 3600000 });
        res.redirect('/cs/storeList');
    } catch (err) {
        console.error('토큰 오류:', err.response?.data || err.message);
        res.status(500).send('토큰 요청 실패');
    }
});

app.get('/cs/api/spaces', async (req, res) => {
    const keyword = req.query.keyword?.trim().toLowerCase();

    if (!tokenCache?.access_token) {
        return res.redirect('/cs/login');
    }

    try {
        let soql = 'SELECT Id, Name, OrderPlatformURL__c  FROM Space__c';
        if (keyword) {
            soql += ` WHERE Name LIKE '%${keyword.replace(/'/g, "\\'")}%'`;
        }

        const allRecords = [];
        let nextUrl = `/services/data/v58.0/query?q=${encodeURIComponent(soql)}`;

        while (nextUrl) {
            const response = await axios.get(`${tokenCache.instance_url}${nextUrl}`, {
                headers: { Authorization: `Bearer ${tokenCache.access_token}` }
            });
            allRecords.push(...response.data.records);
            nextUrl = response.data.nextRecordsUrl || null;
        }

        res.json(allRecords);
    } catch (err) {
        console.error('❌ Salesforce 쿼리 오류:', err.response?.data || err.message);
        res.status(500).json({ error: 'Salesforce 데이터 조회 실패' });
    }
});

app.listen(3003, () => {
    console.log('✅ 서버 실행 중: http://localhost:3003');
});