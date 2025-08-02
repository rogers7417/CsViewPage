const express = require('express');
const axios = require('axios');
const cookieParser = require('cookie-parser');
const path = require('path');
require('dotenv').config();

const app = express();
app.use(cookieParser());
app.use(express.static(__dirname));

let tokenCache = null;

app.get('/', (req, res) => {
    res.redirect('/login');
});

app.get('/login', (req, res) => {
    res.sendFile(path.join(__dirname, 'login.html'));
});

app.get('/cs/storeList', (req, res) => {
    res.sendFile(path.join(__dirname, 'index.html'));
});
// 🔁 콜백에서 토큰 요청 → 쿠키 저장
app.get('/callback', async (req, res) => {
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

        // 👉 메모리에 저장 (또는 req.cookies에 저장해도 됨)
        tokenCache = { access_token, instance_url };

        // 🍪 브라우저에도 저장 (선택)
        res.cookie('sf_logged_in', '1', { maxAge: 3600000 });

        // 💨 홈 또는 매장 목록으로 이동
        res.redirect('/cs/storeList');
    } catch (err) {
        console.error('토큰 오류:', err.response?.data || err.message);
        res.status(500).send('토큰 요청 실패');
    }
});

// 🚀 매장 정보 API
app.get('/cs/api/spaces', async (req, res) => {
    const keyword = req.query.keyword?.trim().toLowerCase();

    if (!tokenCache?.access_token) {
        return res.redirect('/login');
    }

    try {
        // SOQL 구성
        let soql = 'SELECT Id, Name, OrderPlatformURL__c , IsActive__c FROM Space__c';
        if (keyword) {
            // Salesforce는 LIKE 연산 시 % 사용
            soql += ` WHERE Name LIKE '%${keyword.replace(/'/g, "\\'")}%'`;
        }

        // 반복해서 모든 레코드 수집 (pagination 처리)
        const allRecords = [];
        let nextUrl = `/services/data/v58.0/query?q=${encodeURIComponent(soql)}`;

        while (nextUrl) {
            const response = await axios.get(`${tokenCache.instance_url}${nextUrl}`, {
                headers: {
                    Authorization: `Bearer ${tokenCache.access_token}`
                }
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