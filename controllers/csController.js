const path = require('path');
const axios = require('axios');
const { getToken, setToken } = require('../services/salesforceSession');

const ROOT_DIR = path.resolve(__dirname, '..');
const VIEW_DIR = path.join(ROOT_DIR, 'views');

function ensureTokenOrRedirect(res) {
  const token = getToken();
  if (!token?.access_token) {
    res.redirect('/cs/login');
    return false;
  }
  return token;
}

exports.startOAuthLogin = (req, res) => {
  const loginUrl = `https://login.salesforce.com/services/oauth2/authorize?response_type=code&client_id=${process.env.SF_CLIENT_ID}&redirect_uri=${encodeURIComponent(process.env.SF_REDIRECT_URI)}`;
  res.redirect(loginUrl);
};

exports.renderLoginPage = (req, res) => {
  res.sendFile(path.join(VIEW_DIR, 'login.html'));
};

exports.renderStoreList = (req, res) => {
  console.log('cs/storeList 요청');
  res.sendFile(path.join(VIEW_DIR, 'index.html'));
};

exports.getAccount = async (req, res) => {
  const token = ensureTokenOrRedirect(res);
  if (!token) return;

  const accountId = req.params.accountId;

  try {
    const soql = `SELECT Id, Name, Phone, Industry, OwnerId, CreatedDate FROM Account WHERE Id = '${accountId}'`;
    const url = `${token.instance_url}/services/data/v58.0/query?q=${encodeURIComponent(soql)}`;
    const response = await axios.get(url, {
      headers: { Authorization: `Bearer ${token.access_token}` }
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
};

exports.getAccountSummary = async (req, res) => {
  const token = ensureTokenOrRedirect(res);
  if (!token) return;

  const accountId = req.params.accountId;

  try {
    const headers = {
      Authorization: `Bearer ${token.access_token}`
    };

    async function getAllFields(objectName) {
      const url = `${token.instance_url}/services/data/v58.0/sobjects/${objectName}/describe`;
      const describeRes = await axios.get(url, { headers });
      return describeRes.data.fields.map(f => f.name);
    }

    const accountFields = await getAllFields('Account');
    const accountSOQL = `SELECT ${accountFields.join(',')} FROM Account WHERE Id = '${accountId}'`;
    const accountRes = await axios.get(
      `${token.instance_url}/services/data/v58.0/query?q=${encodeURIComponent(accountSOQL)}`,
      { headers }
    );
    const account = accountRes.data.records[0];

    const contractFields = await getAllFields('Contract__c');
    const contractSOQL = `
        SELECT ${contractFields.join(',')} 
        FROM Contract__c 
        WHERE Account__c = '${accountId}' 
        ORDER BY CreatedDate DESC
      `;
    const contractRes = await axios.get(
      `${token.instance_url}/services/data/v58.0/query?q=${encodeURIComponent(contractSOQL)}`,
      { headers }
    );

    res.json({
      account,
      contracts: contractRes.data.records
    });
  } catch (err) {
    console.error('❌ Account summary 오류:', err.response?.data || err.message);
    res.status(500).json({ error: 'Account 요약 조회 실패' });
  }
};

exports.handleOAuthCallback = async (req, res) => {
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

    setToken({ access_token, instance_url });
    res.cookie('sf_logged_in', '1', { maxAge: 3600000 });
    res.redirect('/cs/lead/daily');
  } catch (err) {
    console.error('토큰 오류:', err.response?.data || err.message);
    res.status(500).send('토큰 요청 실패');
  }
};

exports.getSpaces = async (req, res) => {
  const token = ensureTokenOrRedirect(res);
  if (!token) return;

  const keyword = req.query.keyword?.trim().toLowerCase();

  try {
    let soql = 'SELECT Account__c,Id, Name, OrderPlatformURL__c  FROM Space__c';
    if (keyword) {
      soql += ` WHERE Name LIKE '%${keyword.replace(/'/g, "\\'")}%'`;
    }

    const allRecords = [];
    let nextUrl = `/services/data/v58.0/query?q=${encodeURIComponent(soql)}`;

    while (nextUrl) {
      const response = await axios.get(`${token.instance_url}${nextUrl}`, {
        headers: { Authorization: `Bearer ${token.access_token}` }
      });
      allRecords.push(...response.data.records);
      nextUrl = response.data.nextRecordsUrl || null;
    }

    res.json(allRecords);
  } catch (err) {
    console.error('❌ Salesforce 쿼리 오류:', err.response?.data || err.message);
    res.status(500).json({ error: 'Salesforce 데이터 조회 실패' });
  }
};
