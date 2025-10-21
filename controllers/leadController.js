const path = require('path');
const axios = require('axios');
const { getToken } = require('../services/salesforceSession');

const API_VERSION = process.env.SF_API_VERSION || 'v58.0';
const VIEW_DIR = path.join(path.resolve(__dirname, '..'), 'views', 'lead');

function ensureTokenOrJson(res) {
  const token = getToken();
  if (!token?.access_token) {
    res.status(401).json({ error: '인증이 필요합니다.' });
    return null;
  }
  return token;
}

function ensureTokenOrRedirect(res) {
  const token = getToken();
  if (!token?.access_token) {
    res.redirect('/cs/login');
    return null;
  }
  return token;
}

async function queryAll(instanceUrl, accessToken, soql) {
  const headers = { Authorization: `Bearer ${accessToken}` };
  let url = `${instanceUrl}/services/data/${API_VERSION}/query?q=${encodeURIComponent(soql)}`;
  let response = await axios.get(url, { headers });
  const records = [...response.data.records];

  while (!response.data.done && response.data.nextRecordsUrl) {
    url = `${instanceUrl}${response.data.nextRecordsUrl}`;
    response = await axios.get(url, { headers });
    records.push(...response.data.records);
  }

  return records;
}

const esc = (value) => String(value ?? '').replace(/'/g, "\\'");

function computeRange({ month, start, end }) {
  if (month) {
    const [year, monthIndex] = month.split('-').map(Number);
    const s = new Date(Date.UTC(year, monthIndex - 1, 1));
    const e = new Date(Date.UTC(year, monthIndex, 1));
    return {
      start: s.toISOString().slice(0, 10),
      end: e.toISOString().slice(0, 10),
    };
  }

  if (start && end) {
    return { start, end };
  }

  const today = new Date();
  const currentMonth = new Date(Date.UTC(today.getUTCFullYear(), today.getUTCMonth(), 1));
  const prevMonth = new Date(Date.UTC(today.getUTCFullYear(), today.getUTCMonth() - 1, 1));
  return {
    start: prevMonth.toISOString().slice(0, 10),
    end: currentMonth.toISOString().slice(0, 10),
  };
}

function toKstDateKey(isoUtc) {
  if (!isoUtc) return null;
  const d = new Date(isoUtc);
  const kst = new Date(d.getTime() + 9 * 60 * 60 * 1000);
  return kst.toISOString().slice(0, 10);
}

function makeDateKeys(start, end) {
  const keys = [];
  let cursor = new Date(`${start}T00:00:00Z`);
  const endDate = new Date(`${end}T00:00:00Z`);
  while (cursor < endDate) {
    keys.push(cursor.toISOString().slice(0, 10));
    cursor = new Date(cursor.getTime() + 24 * 60 * 60 * 1000);
  }
  return keys;
}

function normalizeStatusLabel(label) {
  return String(label || '').replace(/\s+/g, '').toLowerCase();
}

exports.renderLeadPage = (req, res) => {
  if (!ensureTokenOrRedirect(res)) return;
  res.sendFile(path.join(VIEW_DIR, 'lead.html'));
};

exports.renderLeadDailyPage = (req, res) => {
  if (!ensureTokenOrRedirect(res)) return;
  res.sendFile(path.join(VIEW_DIR, 'leadByDaily.html'));
};
exports.renderLeadDashboard = (req, res) => {
  if (!ensureTokenOrRedirect(res)) return;
  res.sendFile(path.join(VIEW_DIR, 'leadDashboard.html'));
};

exports.renderLeadDailyDashboard = (req, res) => {
  if (!ensureTokenOrRedirect(res)) return;
  res.sendFile(path.join(VIEW_DIR, 'leadByDailyDashboard.html'));
};

exports.renderLeadInsights = (req, res) => {
  if (!ensureTokenOrRedirect(res)) return;
  res.sendFile(path.join(VIEW_DIR, 'lead-insights.html'));
};

exports.renderLeadContracts = (req, res) => {
  if (!ensureTokenOrRedirect(res)) return;
  res.sendFile(path.join(VIEW_DIR, 'contract.html'));
};

exports.getDailyByOwner = async (req, res) => {
  const token = ensureTokenOrJson(res);
  if (!token) return;

  try {
    const { month, start, end, ownerDept, isConverted } = req.query;
    const range = computeRange({ month, start, end });

    const START_DT = `${range.start}T00:00:00Z`;
    const END_DT = `${range.end}T00:00:00Z`;

    const deptList = String(ownerDept || '아웃바운드세일즈')
      .split(',')
      .map((s) => `'${esc(s.trim())}'`)
      .join(',');

    const conds = [
      `IsDeleted = FALSE`,
      `CreatedDate >= ${START_DT}`,
      `CreatedDate <  ${END_DT}`,
      `OwnerId IN (SELECT Id FROM User WHERE Department IN (${deptList}))`,
    ];

    if (isConverted === 'true') conds.push(`IsConverted = true`);
    if (isConverted === 'false') conds.push(`IsConverted = false`);

    const soql = `
        SELECT Id, OwnerId, Owner.Name, CreatedDate
        FROM Lead
        WHERE ${conds.join(' AND ')}
        ORDER BY Owner.Name ASC, CreatedDate ASC
      `.trim();

    const rows = await queryAll(token.instance_url, token.access_token, soql);
    const dateKeys = makeDateKeys(range.start, range.end);
    const byOwner = new Map();
    let footerTotal = 0;

    for (const r of rows) {
      const ownerId = r.OwnerId || 'UNKNOWN';
      const ownerName = (r.Owner && r.Owner.Name) || '미지정';
      const kstKey = toKstDateKey(r.CreatedDate);
      if (!kstKey) continue;

      if (!byOwner.has(ownerId)) {
        const daily = {};
        dateKeys.forEach((key) => {
          daily[key] = 0;
        });
        byOwner.set(ownerId, { ownerId, ownerName, daily, total: 0 });
      }

      const group = byOwner.get(ownerId);
      if (kstKey in group.daily) {
        group.daily[kstKey] += 1;
      }
      group.total += 1;
      footerTotal += 1;
    }

    const owners = [...byOwner.values()].sort(
      (a, b) => a.ownerName.localeCompare(b.ownerName, 'ko') || a.ownerId.localeCompare(b.ownerId),
    );

    const footerDaily = dateKeys.reduce((acc, key) => {
      acc[key] = owners.reduce((sum, owner) => sum + (owner.daily[key] || 0), 0);
      return acc;
    }, {});

    const footer = { total: footerTotal, daily: footerDaily };

    res.status(200).json({
      range,
      month: month || null,
      ownerDept: String(ownerDept || '아웃바운드세일즈'),
      isConverted: isConverted ?? null,
      dateKeys,
      owners,
      footer,
      soql,
    });
  } catch (err) {
    const msg = err.response?.data || err.message || String(err);
    console.error('❌ GET /cs/api/leads/daily-by-owner 실패:', msg);
    res.status(500).json({ error: msg });
  }
};

exports.getCountByOwner = async (req, res) => {
  const token = ensureTokenOrJson(res);
  if (!token) return;

  try {
    const { month, start, end, ownerDept, isConverted, groupBy } = req.query;
    const range = computeRange({ month, start, end });

    const START_DT = `${range.start}T00:00:00Z`;
    const END_DT = `${range.end}T00:00:00Z`;

    const FIXED_STATUSES = [
      '배정대기',
      '담당자 배정',
      '부재중',
      '리터치예정',
      '고민중',
      '장기부재',
      '종료',
      'Qualified',
    ];

    const statusKeyMap = new Map(FIXED_STATUSES.map((label) => [normalizeStatusLabel(label), label]));
    const qualifiedKey = normalizeStatusLabel('Qualified');

    const deptList = String(ownerDept || '아웃바운드세일즈')
      .split(',')
      .map((s) => `'${esc(s.trim())}'`)
      .join(',');

    const conds = [
      `IsDeleted = FALSE`,
      `CreatedDate >= ${START_DT}`,
      `CreatedDate <  ${END_DT}`,
      `OwnerId IN (SELECT Id FROM User WHERE Department IN (${deptList}))`,
    ];
    if (isConverted === 'true') conds.push(`IsConverted = true`);
    if (isConverted === 'false') conds.push(`IsConverted = false`);

    const soql = `
        SELECT Id, OwnerId, Owner.Name, Status, CreatedDate
        FROM Lead
        WHERE ${conds.join(' AND ')}
        ORDER BY Owner.Name ASC, CreatedDate ASC
      `.trim();

    const rows = await queryAll(token.instance_url, token.access_token, soql);

    const byOwner = new Map();
    const footerCounts = new Map(FIXED_STATUSES.map((label) => [label, 0]));
    let footerTotal = 0;

    for (const row of rows) {
      const ownerId = row.OwnerId || 'UNKNOWN';
      const ownerName = row.Owner?.Name || '미지정';
      const key = statusKeyMap.get(normalizeStatusLabel(row.Status));
      if (!key) continue;

      if (!byOwner.has(ownerId)) {
        const init = { ownerId, ownerName };
        FIXED_STATUSES.forEach((label) => {
          init[label] = 0;
        });
        init.total = 0;
        init.conversionRate = '0.0%';
        byOwner.set(ownerId, init);
      }

      const group = byOwner.get(ownerId);
      group[key] += 1;
      group.total += 1;

      footerCounts.set(key, (footerCounts.get(key) || 0) + 1);
      footerTotal += 1;
    }

    for (const group of byOwner.values()) {
      const qualified = Number(group['Qualified'] || 0);
      const total = Number(group.total || 0);
      group.conversionRate = total > 0 ? `${((qualified / total) * 100).toFixed(1)}%` : '0.0%';
    }

    const owners = [...byOwner.values()].sort(
      (a, b) => a.ownerName.localeCompare(b.ownerName, 'ko') || a.ownerId.localeCompare(b.ownerId),
    );

    const totalQualified = footerCounts.get(statusKeyMap.get(qualifiedKey)) || 0;
    const footerConversion =
      footerTotal > 0 ? `${((totalQualified / footerTotal) * 100).toFixed(1)}%` : '0.0%';

    const footer = { ownerName: '총 합계' };
    FIXED_STATUSES.forEach((label) => {
      footer[label] = footerCounts.get(label) || 0;
    });
    footer.total = footerTotal;
    footer.conversionRate = footerConversion;

    const mode = (groupBy || 'owner_status_fixed').toLowerCase();

    res.status(200).json({
      range,
      ownerDept: String(ownerDept || '아웃바운드세일즈'),
      isConverted: isConverted ?? null,
      groupBy: mode,
      fixedStatuses: FIXED_STATUSES,
      totalCount: footerTotal,
      rows: owners,
      footer,
      soql,
    });
  } catch (err) {
    const msg = err.response?.data || err.message || String(err);
    console.error('❌ GET /cs/api/leads/count-by-owner 실패:', msg);
    res.status(500).json({ error: msg });
  }
};
