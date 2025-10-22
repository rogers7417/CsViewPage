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

async function searchAll(instanceUrl, accessToken, sosl) {
  const headers = { Authorization: `Bearer ${accessToken}` };
  const url = `${instanceUrl}/services/data/${API_VERSION}/search?q=${encodeURIComponent(sosl)}`;
  const response = await axios.get(url, { headers });
  if (Array.isArray(response.data?.searchRecords)) {
    return response.data.searchRecords;
  }
  if (Array.isArray(response.data?.records)) {
    return response.data.records;
  }
  return Array.isArray(response.data) ? response.data : [];
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

exports.renderOutboundSensitivityTasks = (req, res) => {
  if (!ensureTokenOrRedirect(res)) return;
  res.sendFile(path.join(VIEW_DIR, 'outbound-sensitivity.html'));
};

exports.getOutboundSensitivityTasks = async (req, res) => {
  const token = ensureTokenOrJson(res);
  if (!token) return;

  try {
    const accessToken = token.access_token;
    const instanceUrl = token.instance_url;
    const searchTerm = '영업 AND 감도 AND (상) OR (중) ';
    const sosl = [
      `FIND {${searchTerm}} IN ALL FIELDS RETURNING`,
      ' Task(',
      'Id, Subject, Description, Status, ActivityDate, Owner.Name, WhoId, CreatedDate',
      " WHERE WhoId IN (SELECT Id FROM Lead WHERE LeadSource = '아웃바운드' AND Status != 'Qualified'  AND Status != '종료')",
      ' AND (CreatedDate = LAST_N_DAYS:60 OR ActivityDate = LAST_N_DAYS:60)',
      ' ORDER BY CreatedDate DESC',
      ' LIMIT 2000',
      ')',
    ].join('');

    const records = await searchAll(instanceUrl, accessToken, sosl);
    const urlRegex = /https?:\/\/[^\s]+/g;
    const imageExtensions = ['.jpg', '.jpeg', '.png', '.gif', '.svg', '.bmp', '.webp'];
    const audioExtensions = ['.mp3', '.wav', '.m4a', '.aac', '.flac', '.ogg'];

    const classifyUrl = (url) => {
      const lower = String(url || '').toLowerCase();
      if (imageExtensions.some((ext) => lower.endsWith(ext))) return 'image';
      if (audioExtensions.some((ext) => lower.endsWith(ext))) return 'audio';
      return 'other';
    };

    const sanitizeDescription = (text) => {
      if (!text) return '';
      return text
        .split(/\r?\n/)
        .map((line) => {
          if (!line) return '';
          let cleaned = line.replace(urlRegex, '').trim();
          if (!cleaned) return '';
          if (/^[📸🎧]/.test(cleaned)) return '';
          cleaned = cleaned.replace(/^(?:\[[^\]]*\]\s*)?-+\s*/, '').trim();
          return cleaned;
        })
        .filter(Boolean)
        .join('\n');
    };

    const rawTasks = records
      .filter((rec) => rec && rec.attributes?.type === 'Task')
      .map((rec) => {
        const descriptionRaw = rec.Description || '';
        const urls = descriptionRaw.match(urlRegex) || [];
        const images = [];
        const audios = [];
        urls.forEach((url) => {
          const type = classifyUrl(url);
          if (type === 'image') images.push(url);
          else if (type === 'audio') audios.push(url);
        });
        const description = sanitizeDescription(descriptionRaw);
        return {
          id: rec.Id,
          subject: rec.Subject || null,
          description: description || null,
          descriptionRaw: descriptionRaw || null,
          status: rec.Status || null,
          activityDate: rec.ActivityDate || null,
          createdDate: rec.CreatedDate || null,
          whoId: rec.WhoId || null,
          ownerName: rec.Owner?.Name || rec['Owner.Name'] || null,
          ownerId: rec.Owner?.Id || null,
          imageUrls: images,
          audioUrls: audios,
        };
      });

    const leadMap = new Map();
    const leadRangeSoql = `SELECT Id, Name, Company, Status, LeadSource, CreatedDate, OwnerId, Owner.Name, Sido__c, Sigugun__c, RoadAddress__c, Phone, MobilePhone, Store_Contact__c, Industry__c FROM Lead WHERE LeadSource = '아웃바운드' AND CreatedDate = LAST_N_DAYS:31 ORDER BY CreatedDate DESC`;
    const recentLeadRecords = await queryAll(instanceUrl, accessToken, leadRangeSoql);
    recentLeadRecords.forEach((lead) => {
      leadMap.set(lead.Id, {
        id: lead.Id,
        leadName: lead.Name || null,
        leadCompany: lead.Company || null,
        leadStatus: lead.Status || null,
        leadSource: lead.LeadSource || null,
        leadCreatedDate: lead.CreatedDate || null,
        leadOwnerId: lead.OwnerId || null,
        leadOwnerName: lead.Owner?.Name || null,
        leadSido: lead.Sido__c || null,
        leadSigugun: lead.Sigugun__c || null,
        leadRoadAddress: lead.RoadAddress__c || null,
        leadPhone: lead.Phone || null,
        leadMobilePhone: lead.MobilePhone || null,
        leadStoreContact: lead.Store_Contact__c || null,
        leadIndustry: lead.Industry__c || null,
      });
    });

    const missingLeadIds = Array.from(
      new Set(
        rawTasks
          .map((task) => task.whoId)
          .filter(Boolean)
          .filter((id) => !leadMap.has(id)),
      ),
    );

    if (missingLeadIds.length) {
      const chunkSize = 100;
      for (let i = 0; i < missingLeadIds.length; i += chunkSize) {
        const chunkIds = missingLeadIds.slice(i, i + chunkSize).map((id) => `'${esc(id)}'`).join(',');
        const soqlLead = `SELECT Id, Name, Company, Status, LeadSource, CreatedDate, OwnerId, Owner.Name, Sido__c, Sigugun__c, RoadAddress__c, Phone, MobilePhone, Store_Contact__c, Industry__c FROM Lead WHERE Id IN (${chunkIds})`;
        const leadRecords = await queryAll(instanceUrl, accessToken, soqlLead);
        leadRecords.forEach((lead) => {
          leadMap.set(lead.Id, {
            id: lead.Id,
            leadName: lead.Name || null,
            leadCompany: lead.Company || null,
            leadStatus: lead.Status || null,
            leadSource: lead.LeadSource || null,
            leadCreatedDate: lead.CreatedDate || null,
            leadOwnerId: lead.OwnerId || null,
            leadOwnerName: lead.Owner?.Name || null,
            leadSido: lead.Sido__c || null,
            leadSigugun: lead.Sigugun__c || null,
            leadRoadAddress: lead.RoadAddress__c || null,
            leadPhone: lead.Phone || null,
            leadMobilePhone: lead.MobilePhone || null,
            leadStoreContact: lead.Store_Contact__c || null,
            leadIndustry: lead.Industry__c || null,
          });
        });
      }
    }

    const tasksWithLead = rawTasks.map((task) => {
      const leadInfo = task.whoId ? leadMap.get(task.whoId) || null : null;
      if (!leadInfo) return task;
      const { id: leadId, ...restLead } = leadInfo;
      return {
        ...task,
        leadId,
        ...restLead,
      };
    });

    const seenLeadIds = new Set();
    const tasks = [];
    tasksWithLead.forEach((task) => {
      if (task.leadId) {
        if (seenLeadIds.has(task.leadId)) return;
        seenLeadIds.add(task.leadId);
      }
      tasks.push(task);
    });

    const leads = Array.from(leadMap.values()).sort((a, b) => {
      const aDate = a.leadCreatedDate ? new Date(a.leadCreatedDate) : null;
      const bDate = b.leadCreatedDate ? new Date(b.leadCreatedDate) : null;
      if (aDate && bDate) return bDate - aDate;
      if (bDate) return 1;
      if (aDate) return -1;
      return 0;
    });

    res.status(200).json({
      query: sosl,
      count: tasks.length,
      records: tasks,
      leadCount: leads.length,
    });
  } catch (err) {
    const message = err.response?.data || err.message || String(err);
    console.error('GET /cs/api/tasks/outbound-sensitivity error', message);
    res.status(500).json({ error: message });
  }
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
