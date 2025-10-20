const axios = require('axios');
const OpenAI = require('openai');

const API_VER = process.env.SF_API_VERSION || 'v60.0';
const OPENAI_API_KEY = process.env.OPENAI_API_KEY || null;
const openaiClient = OPENAI_API_KEY ? new OpenAI({ apiKey: OPENAI_API_KEY }) : null;

const esc = (s) => String(s ?? '').replace(/'/g, "\\'");

function makeError(message, status = 500, code = 'UNEXPECTED') {
    const err = new Error(message);
    err.status = status;
    err.code = code;
    return err;
}

async function getSalesforceToken() {
    const loginUrl = process.env.SF_LOGIN_URL;
    if (!loginUrl) {
        throw makeError('SF_LOGIN_URL not configured', 500, 'SF_LOGIN_URL_MISSING');
    }
    if (!process.env.SF_CLIENT_ID || !process.env.SF_CLIENT_SECRET || !process.env.SF_USERNAME || !process.env.SF_PASSWORD) {
        throw makeError('Salesforce credentials missing', 500, 'SF_CREDENTIALS_MISSING');
    }

    const url = `${loginUrl}/services/oauth2/token`;
    const params = new URLSearchParams();
    params.append('grant_type', 'password');
    params.append('client_id', process.env.SF_CLIENT_ID);
    params.append('client_secret', process.env.SF_CLIENT_SECRET);
    params.append('username', process.env.SF_USERNAME);
    params.append('password', decodeURIComponent(process.env.SF_PASSWORD));

    const res = await axios.post(url, params);
    return { accessToken: res.data.access_token, instanceUrl: res.data.instance_url };
}

async function sfGet(url, accessToken) {
    const { data } = await axios.get(url, {
        headers: { Authorization: `Bearer ${accessToken}` },
        timeout: 30000,
    });
    return data;
}

async function queryAll(instanceUrl, accessToken, soql) {
    const encoded = encodeURIComponent(soql);
    let url = `${instanceUrl}/services/data/${API_VER}/query?q=${encoded}`;
    let data = await sfGet(url, accessToken);
    const records = [...data.records];
    while (!data.done && data.nextRecordsUrl) {
        url = `${instanceUrl}${data.nextRecordsUrl}`;
        data = await sfGet(url, accessToken);
        records.push(...data.records);
    }
    return records;
}

function extractMonthKey(value) {
    if (!value) return null;
    const str = String(value);
    if (str.length < 7) return null;
    const key = str.slice(0, 7);
    return /^\d{4}-\d{2}$/.test(key) ? key : null;
}

function getRecentMonths(count = 3) {
    const result = [];
    if (count <= 0) return result;
    const base = new Date();
    base.setUTCDate(1);
    for (let offset = count - 1; offset >= 0; offset -= 1) {
        const d = new Date(Date.UTC(base.getUTCFullYear(), base.getUTCMonth() - offset, 1));
        const ym = `${d.getUTCFullYear()}-${String(d.getUTCMonth() + 1).padStart(2, '0')}`;
        result.push(ym);
    }
    return result;
}

function getMonthBoundaries(monthKey) {
    const [yearStr, monthStr] = monthKey.split('-');
    const year = Number(yearStr);
    const month = Number(monthStr);
    const start = new Date(Date.UTC(year, month - 1, 1));
    const end = new Date(Date.UTC(year, month, 1));
    return {
        startDateTime: start.toISOString(),
        endDateTime: end.toISOString(),
        startDate: start.toISOString().slice(0, 10),
        endDate: end.toISOString().slice(0, 10),
    };
}

async function resolveMonthlyMetrics({ months, ownerDept }) {
    const monthList = (Array.isArray(months) && months.length) ? months : getRecentMonths(3);
    if (!monthList.length) return [];

    const statsMap = new Map();
    monthList.forEach((key) => {
        statsMap.set(key, {
            month: key,
            leads: 0,
            opportunities: 0,
            contracts: 0,
            tablets: 0,
            discount: 0,
        });
    });

    const firstBounds = getMonthBoundaries(monthList[0]);
    const lastBounds = getMonthBoundaries(monthList[monthList.length - 1]);

    const normalizeDateTime = (iso) => {
        if (!iso) return null;
        const base = iso.substring(0, 19);
        return `${base}Z`;
    };

    const dateTimeStart = normalizeDateTime(firstBounds.startDateTime);
    const dateTimeEnd = normalizeDateTime(lastBounds.endDateTime);
    const dateStart = firstBounds.startDate;
    const dateEnd = lastBounds.endDate;

    const { accessToken, instanceUrl } = await getSalesforceToken();

    const deptClause = (function () {
        if (!ownerDept || typeof ownerDept !== 'string' || !ownerDept.trim()) return null;
        const parts = ownerDept.split(',').map((s) => s.trim()).filter(Boolean);
        if (!parts.length) return null;
        const list = parts.map((s) => `'${esc(s)}'`).join(',');
        return list;
    }());

    const leadWhere = [
        'IsDeleted = false',
        `CreatedDate >= ${dateTimeStart}`,
        `CreatedDate < ${dateTimeEnd}`,
    ];
    if (deptClause) {
        leadWhere.push(`OwnerId IN (SELECT Id FROM User WHERE Department IN (${deptClause}))`);
    }
    const leadSOQL = `
        SELECT Id, CreatedDate
        FROM Lead
        WHERE ${leadWhere.join(' AND ')}
    `.trim();
    const leadRows = await queryAll(instanceUrl, accessToken, leadSOQL);
    for (const row of leadRows) {
        const key = extractMonthKey(row.CreatedDate);
        if (key && statsMap.has(key)) {
            statsMap.get(key).leads += 1;
        }
    }

    const oppWhere = [
        'IsDeleted = false',
        `CreatedDate >= ${dateTimeStart}`,
        `CreatedDate < ${dateTimeEnd}`,
    ];
    if (deptClause) {
        oppWhere.push(`OwnerId IN (SELECT Id FROM User WHERE Department IN (${deptClause}))`);
    }
    const oppSOQL = `
        SELECT Id, CreatedDate
        FROM Opportunity
        WHERE ${oppWhere.join(' AND ')}
    `.trim();
    const oppRows = await queryAll(instanceUrl, accessToken, oppSOQL);
    for (const row of oppRows) {
        const key = extractMonthKey(row.CreatedDate);
        if (key && statsMap.has(key)) {
            statsMap.get(key).opportunities += 1;
        }
    }

    const contractWhere = [
        'ContractDateStart__c != NULL',
        `ContractDateStart__c >= ${dateStart}`,
        `ContractDateStart__c < ${dateEnd}`,
        `(ContractStatus__c = '계약서명완료' OR ContractStatus__c = '계약서명대기')`,
    ];
    if (deptClause) {
        contractWhere.push(`Opportunity__r.Owner_Department__c IN (${deptClause})`);
    }
    const contractSOQL = `
        SELECT Id, ContractDateStart__c, Opportunity__r.TotalNumberofEveryTablet__c,
        (SELECT Quantity__c, TotalPrice__c FROM ContractProductQuoteContract__r),
        (SELECT TotalAmount__c FROM ContractProductPromotionContract__r)
        FROM Contract__c
        WHERE ${contractWhere.join(' AND ')}
    `.trim();
    const contractRows = await queryAll(instanceUrl, accessToken, contractSOQL);
    for (const row of contractRows) {
        const key = extractMonthKey(row.ContractDateStart__c);
        if (!key || !statsMap.has(key)) continue;
        const entry = statsMap.get(key);
        entry.contracts += 1;
        const tabletsRaw = row.Opportunity__r?.TotalNumberofEveryTablet__c;
        const tabletsNum = Number(tabletsRaw ?? 0);
        if (Number.isFinite(tabletsNum)) {
            entry.tablets += tabletsNum;
        }

        let discountSum = 0;
        const productRecords = row.ContractProductQuoteContract__r?.records ?? [];
        for (const prod of productRecords) {
            const totalPrice = Number(prod.TotalPrice__c ?? 0);
            const quantity = Number(prod.Quantity__c ?? 1);
            if (totalPrice < 0) {
                const qty = Number.isFinite(quantity) && quantity > 0 ? quantity : 1;
                discountSum += Math.abs(totalPrice) * qty;
            }
        }

        const promoRecords = row.ContractProductPromotionContract__r?.records ?? [];
        for (const promo of promoRecords) {
            const amount = Number(promo.TotalAmount__c ?? 0);
            if (Number.isFinite(amount)) {
                discountSum += amount;
            }
        }

        if (Number.isFinite(discountSum)) {
            entry.discount += discountSum;
        }
    }

    return monthList.map((key) => statsMap.get(key));
}

async function generateLeadSummaryInsight({ monthlyData, targetTablets = 400, ownerDept = null }) {
    if (!openaiClient) {
        throw makeError('OPENAI_API_KEY not configured', 503, 'OPENAI_NOT_CONFIGURED');
    }

    let baseData = Array.isArray(monthlyData) ? monthlyData : [];
    if (!baseData.length) {
        baseData = await resolveMonthlyMetrics({ ownerDept });
    }

    const sanitized = baseData
        .map((item) => ({
            month: item?.month ?? null,
            leads: Number(item?.leads ?? 0),
            opportunities: Number(item?.opportunities ?? 0),
            contracts: Number(item?.contracts ?? 0),
            tablets: Number(item?.tablets ?? 0),
            discount: Number(item?.discount ?? item?.totalDiscount ?? 0),
        }))
        .filter((item) => typeof item.month === 'string' && item.month.length >= 7);

    if (!sanitized.length) {
        throw makeError('monthly data unavailable', 400, 'MONTHLY_DATA_UNAVAILABLE');
    }

    const totals = sanitized.reduce((acc, row) => {
        acc.leads += row.leads;
        acc.opportunities += row.opportunities;
        acc.contracts += row.contracts;
        acc.tablets += row.tablets;
        return acc;
    }, { leads: 0, opportunities: 0, contracts: 0, tablets: 0 });

    const avgLeadToOppRate = totals.leads > 0 ? totals.opportunities / totals.leads : 0;
    const avgOppToContractRate = totals.opportunities > 0 ? totals.contracts / totals.opportunities : 0;
    const avgTabletsPerContract = totals.contracts > 0 ? totals.tablets / totals.contracts : 0;

    const safeAvgLeadToOpp = avgLeadToOppRate > 0 ? avgLeadToOppRate : 0.01;
    const safeAvgOppToContract = avgOppToContractRate > 0 ? avgOppToContractRate : 0.5;
    const safeAvgTabletsPerContract = avgTabletsPerContract > 0 ? avgTabletsPerContract : Math.max(targetTablets, 1);

    const contractsNeeded = Math.ceil(targetTablets / safeAvgTabletsPerContract);
    const opportunitiesNeeded = Math.ceil(contractsNeeded / safeAvgOppToContract);
    const leadsNeeded = Math.ceil(opportunitiesNeeded / safeAvgLeadToOpp);

    const latestRow = sanitized.reduce((latest, row) => {
        if (!row.month) return latest;
        if (!latest) return row;
        return row.month > latest.month ? row : latest;
    }, null) || sanitized[sanitized.length - 1];

    const currentLeads = Number(latestRow?.leads ?? 0);
    const currentOpportunities = Number(latestRow?.opportunities ?? 0);
    const leadGap = Math.max(leadsNeeded - currentLeads, 0);
    const oppGap = Math.max(opportunitiesNeeded - currentOpportunities, 0);

    let tabletsPerContractRange;
    if (avgTabletsPerContract > 0) {
        const floor = Math.max(1, Math.floor(avgTabletsPerContract));
        const ceil = Math.max(floor, Math.ceil(avgTabletsPerContract));
        tabletsPerContractRange = floor === ceil
            ? `약 ${ceil}대`
            : `약 ${floor}~${ceil}대`;
    } else {
        tabletsPerContractRange = '약 1대';
    }

    const summaryForPrompt = {
        targetTablets: Number(targetTablets),
        contractsNeeded,
        tabletsPerContractRange,
        leadToOppRate: Number((avgLeadToOppRate * 100).toFixed(2)),
        oppToContractRate: Number((avgOppToContractRate * 100).toFixed(2)),
        opportunitiesNeeded,
        leadsNeeded,
        currentLeads,
        currentOpportunities,
        oppGap,
        leadGap,
        additionalOpportunitiesNeeded: oppGap,
        additionalLeadsNeeded: leadGap
    };

    const schema = {
        name: 'LeadInsight',
        schema: {
            type: 'object',
            properties: {
                monthlyNarratives: {
                    type: 'array',
                    items: {
                        type: 'object',
                        properties: {
                            month: { type: 'string' },
                            summary: { type: 'string' },
                            keyMetrics: {
                                type: 'object',
                                properties: {
                                    leadToOpportunityRate: { type: 'string' },
                                    leadToContractRate: { type: 'string' },
                                    opportunityToContractRate: { type: 'string' },
                                    tabletsPerContract: { type: 'string' }
                                },
                                required: [
                                    'leadToOpportunityRate',
                                    'leadToContractRate',
                                    'opportunityToContractRate',
                                    'tabletsPerContract'
                                ],
                                additionalProperties: false
                            }
                        },
                        required: ['month', 'summary', 'keyMetrics'],
                        additionalProperties: false
                    }
                },
                currentMonthOutlook: {
                    type: 'object',
                    properties: {
                        contractsNeeded: { type: 'integer' },
                        additionalLeadsNeeded: { type: 'integer' },
                        additionalOpportunitiesNeeded: { type: 'integer' },
                        commentary: { type: 'string' }
                    },
                    required: [
                        'contractsNeeded',
                        'additionalLeadsNeeded',
                        'additionalOpportunitiesNeeded',
                        'commentary'
                    ],
                    additionalProperties: false
                },
                highLevelSummary: {
                    type: 'array',
                    items: { type: 'string' }
                },
                strategicActions: {
                    type: 'array',
                    items: {
                        type: 'object',
                        properties: {
                            title: { type: 'string' },
                            bullets: {
                                type: 'array',
                                items: { type: 'string' }
                            }
                        },
                        required: ['title', 'bullets'],
                        additionalProperties: false
                    }
                }
            },
            required: [
                'monthlyNarratives',
                'currentMonthOutlook',
                'highLevelSummary',
                'strategicActions'
            ],
            additionalProperties: false
        }
    };

    const systemPrompt = [
        '당신은 SaaS 영업팀의 데이터 애널리스트입니다.',
        '월별 리드/기회/계약/태블릿 데이터를 분석해 목표 태블릿 달성 가능성을 평가하세요.',
        'JSON 응답은 지정된 스키마를 반드시 준수해야 합니다.',
        'currentMonthOutlook.commentary는 아래 템플릿을 Summary 값으로 치환해 작성합니다:',
        '"이번 달 목표 태블릿 {targetTablets}대 달성을 위해 계약 수를 {contractsNeeded}건 이상 확보해야 합니다. 과거 평균 계약당 태블릿 수({tabletsPerContractRange})를 고려하면 최소 {contractsNeeded}건 필요하며, 이를 위해 리드→기회 전환율 평균 {leadToOppRate}% , 기회→계약 전환율 {oppToContractRate}%를 적용하면 기회는 약 {opportunitiesNeeded}건, 리드는 약 {leadsNeeded}건이 요구됩니다. 현재 리드 {currentLeads}건 대비 {leadGap}건 증가가 필요하며, 기회 수 역시 {oppGap}건 이상 더 만들어야 합니다."',
        '템플릿 외의 문장을 commentary에 추가하지 말고, Summary 객체에 제공된 숫자만 사용해 중괄호를 치환하세요.',
        '나머지 필드(monthlyNarratives, highLevelSummary, strategicActions)는 데이터를 근거로 한 인사이트를 간결하게 제공하세요.'
    ].join(' ');

    const userMessage = [
        '월별 집계 데이터 JSON:',
        JSON.stringify(sanitized, null, 2),
        'Summary 값(JSON)은 commentary 템플릿을 채우기 위해 제공됩니다. 중괄호 변수는 이 값을 그대로 사용하세요.',
        JSON.stringify(summaryForPrompt, null, 2),
        '작성 지침:',
        '- monthlyNarratives, highLevelSummary, strategicActions에는 월별 추이를 해석한 간결한 인사이트를 제공합니다.',
        '- currentMonthOutlook 필드에서 contractsNeeded, additionalLeadsNeeded, additionalOpportunitiesNeeded 값은 Summary 값을 그대로 사용합니다.',
        '- commentary에는 템플릿 문장 외 다른 문장을 추가하지 않습니다.',
        '- 숫자는 Summary에 없는 경우 월별 데이터를 이용해 합리적인 값으로 채웁니다.'
    ].join('\n');

    const response = await openaiClient.responses.create({
        model: 'gpt-4.1-mini',
        input: [
            { role: 'system', content: systemPrompt },
            { role: 'user', content: userMessage }
        ],
        text: {
            format: {
                type: 'json_schema',
                name: schema.name,
                schema: schema.schema
            }
        },
        max_output_tokens: 1200
    });

    const raw =
        response?.output_text ||
        response?.output?.[0]?.content?.[0]?.text ||
        null;

    let parsed = null;
    try {
        parsed = raw ? JSON.parse(raw) : null;
    } catch (err) {
        throw makeError('Failed to parse OpenAI response', 502, 'OPENAI_PARSE_FAILED');
    }

    return {
        generatedAt: new Date().toISOString(),
        source: 'openai',
        input: { monthlyData: sanitized, targetTablets, summary: summaryForPrompt },
        insight: parsed
    };
}

module.exports = {
    generateLeadSummaryInsight,
    resolveMonthlyMetrics,
};
