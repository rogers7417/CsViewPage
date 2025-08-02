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
app.get('/cs/auth/login', (req, res) => {
    const loginUrl = `https://login.salesforce.com/services/oauth2/authorize?response_type=code&client_id=${process.env.SF_CLIENT_ID}&redirect_uri=${encodeURIComponent(process.env.SF_REDIRECT_URI)}`;
    res.redirect(loginUrl);
});
app.get('/cs/login', (req, res) => {
    res.sendFile(path.join(__dirname, 'login.html'));
});

app.get('/cs/storeList', (req, res) => {
    console.log("cs/storeList 요청");
    res.sendFile(path.join(__dirname, 'index.html'));
});
app.get('/cs/api/accounts/:accountId', async (req, res) => {
    const accountId = req.params.accountId;

    if (!tokenCache?.access_token) {
        return res.redirect('/cs/login');
    }

    try {
        const soql = `SELECT Id, Name, Phone, Industry, OwnerId, CreatedDate FROM Account WHERE Id = '${accountId}'`;
        const url = `${tokenCache.instance_url}/services/data/v58.0/query?q=${encodeURIComponent(soql)}`;
        const response = await axios.get(url, {
            headers: { Authorization: `Bearer ${tokenCache.access_token}` }
        });

        const account = response.data.records?.[0];
        if (!account) {
            return res.status(404).json({ error: 'Account not found' });
        }

        res.json(account);
    } catch (err) {
        console.error('❌ Account 조회 오류:', err.response?.data || err.message);
        res.status(500).json({ error: 'Account 데이터 조회 실패' });
    }
});
app.get('/cs/api/accounts/:accountId/summary', async (req, res) => {
    const accountId = req.params.accountId;

    if (!tokenCache?.access_token) {
        return res.redirect('/cs/login');
    }

    try {
        const headers = {
            Authorization: `Bearer ${tokenCache.access_token}`
        };

        // ✅ describe 메타데이터로 필드 목록 가져오는 함수
        async function getAllFields(objectName) {
            const url = `${tokenCache.instance_url}/services/data/v58.0/sobjects/${objectName}/describe`;
            const res = await axios.get(url, { headers });
            return res.data.fields.map(f => f.name);
        }

        // 1. Account 모든 필드 조회
        const accountFields = await getAllFields('Account');
        const accountSOQL = `SELECT ${accountFields.join(',')} FROM Account WHERE Id = '${accountId}'`;
        const accountRes = await axios.get(
            `${tokenCache.instance_url}/services/data/v58.0/query?q=${encodeURIComponent(accountSOQL)}`,
            { headers }
        );
        const account = accountRes.data.records[0];

        // 2. Contract__c 모든 필드 조회
        const contractFields = await getAllFields('Contract__c');
        const contractSOQL = `
        SELECT ${contractFields.join(',')} 
        FROM Contract__c 
        WHERE Account__c = '${accountId}' 
        ORDER BY CreatedDate DESC
      `;
        const contractRes = await axios.get(
            `${tokenCache.instance_url}/services/data/v58.0/query?q=${encodeURIComponent(contractSOQL)}`,
            { headers }
        );
        const contracts = contractRes.data.records;

        // 3. 응답
        res.json({
            account,
            contracts
        });
    } catch (err) {
        console.error('❌ Account summary 오류:', err.response?.data || err.message);
        res.status(500).json({ error: 'Account 요약 조회 실패' });
    }
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
        let soql = 'SELECT Account__c,Id, Name, OrderPlatformURL__c  FROM Space__c';
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