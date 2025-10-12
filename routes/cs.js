const express = require('express');
const csController = require('../controllers/csController');

const router = express.Router();

router.get('/', csController.renderLoginPage);
router.get('/auth/login', csController.startOAuthLogin);
router.get('/login', csController.renderLoginPage);
router.get('/storeList', csController.renderStoreList);
router.get('/api/accounts/:accountId', csController.getAccount);
router.get('/api/accounts/:accountId/summary', csController.getAccountSummary);
router.get('/api/spaces', csController.getSpaces);
router.get('/callback', csController.handleOAuthCallback);

module.exports = router;
