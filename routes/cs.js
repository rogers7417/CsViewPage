const express = require('express');
const csController = require('../controllers/csController');
const leadController = require('../controllers/leadController');

const router = express.Router();

router.get('/', csController.renderLoginPage);
router.get('/auth/login', csController.startOAuthLogin);
router.get('/login', csController.renderLoginPage);
router.get('/storeList', csController.renderStoreList);
router.get('/lead', leadController.renderLeadPage);
router.get('/lead/daily', leadController.renderLeadDailyPage);
router.get('/lead/dashboard', leadController.renderLeadDashboard);
router.get('/lead/dashboard/daily', leadController.renderLeadDailyDashboard);
router.get('/lead/insights', leadController.renderLeadInsights);
router.get('/lead/contracts', leadController.renderLeadContracts);
router.get('/lead/tasks/outbound-sensitivity', leadController.renderOutboundSensitivityTasks);
router.get('/api/leads/daily-by-owner', leadController.getDailyByOwner);
router.get('/api/leads/count-by-owner', leadController.getCountByOwner);
router.get('/api/tasks/outbound-sensitivity', leadController.getOutboundSensitivityTasks);
router.get('/api/partners-and-leads', csController.getPartnersAndLeads);
router.get('/api/accounts/:accountId', csController.getAccount);
router.get('/api/accounts/:accountId/summary', csController.getAccountSummary);
router.get('/api/spaces', csController.getSpaces);
router.get('/callback', csController.handleOAuthCallback);
router.get('/opportunity/kanban', csController.renderOpportunityKanban);

module.exports = router;
