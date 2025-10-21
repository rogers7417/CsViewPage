const axios = require('axios');

const API_VER = process.env.SF_API_VERSION || 'v60.0';
const BASE_PRICE = 648000;

const esc = (s) => String(s ?? '').replace(/'/g, "\\'");

function makeError(message, status = 500, code = 'UNEXPECTED') {
    const err = new Error(message);
    err.status = status;
    err.code = code;
    return err;
}

function parseSfDate(dateStr) {
    return dateStr ? new Date(`${dateStr}T00:00:00.000Z`) : null;
}

function parseSfDateTime(dtStr) {
    if (!dtStr) return null;
    const fixed = dtStr.replace(/([+-]\d{2})(\d{2})$/, '$1:$2'); // +0900 → +09:00
    const d = new Date(fixed);
    return Number.isNaN(d.getTime()) ? null : d;
}

function diffDaysWithReason(startISO, endISO) {
    if (!startISO || !endISO) return { days: null, reason: 'missing-date' };
    const s = parseSfDateTime(startISO) || parseSfDate(startISO);
    const e = parseSfDateTime(endISO) || parseSfDate(endISO);
    if (!s || !e) {
        return { days: null, reason: 'parse-error' };
    }
    const msPerDay = 24 * 60 * 60 * 1000;
    const days = Math.ceil((e.getTime() - s.getTime()) / msPerDay);
    return { days, reason: 'ok' };
}

function parseUtmParams(raw) {
    if (!raw || typeof raw !== 'string') {
        return { utmSource: null, utmContent: null, utmTerm: null };
    }
    const qs = raw.includes('?') ? raw.split('?').pop() : raw;
    let params;
    try {
        params = new URLSearchParams(qs);
    } catch (err) {
        return { utmSource: null, utmContent: null, utmTerm: null };
    }
    const getOrNull = (key) => {
        const v = params.get(key);
        return v != null && v !== '' ? v : null;
    };
    return {
        utmSource: getOrNull('utm_source'),
        utmContent: getOrNull('utm_content'),
        utmTerm: getOrNull('utm_term'),
    };
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

function normalize(record) {
    const opportunity = record.Opportunity__r || {};
    const account = record.Account__r || {};
    const opportunityAccount = opportunity.Account || {};

    const productRows = record.ContractProductQuoteContract__r?.records ?? [];
    const promoRows = record.ContractProductPromotionContract__r?.records ?? [];

    const oppCreated = opportunity?.CreatedDate || null;
    const contractStart = record.CreatedDate;

    const leadTime = diffDaysWithReason(oppCreated, contractStart);
    const products = [];
    const promotionsFromNegativeProducts = [];

    for (const p of productRows) {
        const quantity = Number(p.Quantity__c ?? 1);
        const unitActual = Number(p.TotalPrice__c ?? 0);

        if (unitActual < 0) {
            promotionsFromNegativeProducts.push({
                id: p.Id,
                totalAmount: Math.abs(unitActual) * (quantity > 0 ? quantity : 1),
                promotionName: p.fm_ContractProductFamily__c || `프로모션(${p.Id})`,
                _source: 'product',
            });
            continue;
        }

        const optionExtraPerUnit = Math.max(0, unitActual - BASE_PRICE);
        const option = optionExtraPerUnit > 0
            ? [{ hasOption: true, optionExtraTotal: optionExtraPerUnit }]
            : [];

        products.push({
            id: p.Id,
            family: p.fm_ContractProductFamily__c,
            quantity,
            totalPrice: unitActual,
            unitPrice: BASE_PRICE,
            option,
        });
    }

    const nativePromotions = promoRows.map(pr => ({
        id: pr.Id,
        totalAmount: Number(pr.TotalAmount__c ?? 0),
        promotionName: pr.PromotionName__r?.Name || '프로모션',
        _source: 'native',
    }));

    const productsTotal = products.reduce(
        (sum, it) => sum + Number(it.totalPrice || 0) * Number(it.quantity || 0), 0
    );
    const promotionsFromProductsTotal = promotionsFromNegativeProducts.reduce(
        (sum, it) => sum + Number(it.totalAmount || 0), 0
    );
    const promotionsNativeTotal = nativePromotions.reduce(
        (sum, it) => sum + Number(it.totalAmount || 0), 0
    );

    const purchaseAmount = productsTotal - promotionsFromProductsTotal;
    const vat = Math.floor(purchaseAmount * 0.1);
    const totalWithVat = purchaseAmount + vat;

    return {
        id: record.Id,
        name: record.Name,
        recordTypeId: opportunity?.RecordTypeId,
        recordTypeName: opportunity?.RecordType?.Name,
        accountName: account?.Name,
        accountBranchName: account?.BranchName__c,
        plIndustryFirst: account.PLIndustry_First__c || opportunityAccount.PLIndustry_First__c,
        plIndustrySecond: account.PLIndustry_Second__c || opportunityAccount.PLIndustry_Second__c,
        typeOfBusiness: account.TypeofB__c || opportunityAccount.TypeofB__c,
        createdDate: record.CreatedDate,
        contractDateStart: record.ContractDateStart__c,
        contractDateEnd: record.ContractDateEnd__c,
        contractStatus: record.ContractStatus__c,
        TotalNumberofEveryTablet__c: opportunity?.TotalNumberofEveryTablet__c,
        FieldUser: opportunity?.FieldUser__r?.Name,
        BOUser: opportunity?.BOUser__r?.Name,
        convertedLeadId: opportunity?.ConvertedLeadID__c || null,
        leadTime,
        leadTimeSource: { oppCreated, contractStart },
        leadSourceOpportunity: opportunity?.LeadSource,
        fmSido: opportunity?.fm_sido__c,
        fmSigugun: opportunity?.fm_Sigugun__c,
        fmStoreType: opportunity?.fm_StoreType__c,
        opportunity: {
            id: opportunity?.Id,
            stageName: opportunity?.StageName,
            ownerId: opportunity?.OwnerId,
            ownerName: opportunity?.Owner?.Name,
            ownerDepartment: opportunity?.Owner_Department__c,
            accountId: opportunity?.AccountId,
            accountName: opportunityAccount?.Name,
            accountBranchName: opportunityAccount?.BranchName__c,
            ru_TabletQty__c: opportunity?.ru_TabletQty__c,
            ru_MasterTabletQty__c: opportunity?.ru_MasterTabletQty__c,
            totalEveryTablet: opportunity?.TotalNumberofEveryTablet__c,
            createdDate: opportunity?.CreatedDate,
            leadSource: opportunity?.LeadSource,
            fmSido: opportunity?.fm_sido__c,
            fmSigugun: opportunity?.fm_Sigugun__c,
            fmStoreType: opportunity?.fm_StoreType__c,
            plIndustryFirst: opportunityAccount?.PLIndustry_First__c,
            plIndustrySecond: opportunityAccount?.PLIndustry_Second__c,
            typeOfBusiness: opportunityAccount?.TypeofB__c,
        },
        productsTotal,
        promotionsFromProductsTotal,
        promotionsNativeTotal,
        promotionsTotal: promotionsFromProductsTotal + promotionsNativeTotal,
        totalDiscount: promotionsFromProductsTotal + promotionsNativeTotal,
        purchaseAmount,
        vat,
        totalWithVat,
        products,
        promotions: [...nativePromotions, ...promotionsFromNegativeProducts],
    };
}

function computeRange({ month, start, end }) {
    if (month) {
        const [y, m] = month.split('-').map(Number);
        const s = new Date(Date.UTC(y, (m - 1), 1));
        const e = new Date(Date.UTC(y, (m - 1) + 1, 1));
        const sStr = s.toISOString().slice(0, 10);
        const eStr = e.toISOString().slice(0, 10);
        return { start: sStr, end: eStr };
    }
    if (start && end) return { start, end };
    const today = new Date();
    const curMonth1 = new Date(Date.UTC(today.getUTCFullYear(), today.getUTCMonth(), 1));
    const prevMonth1 = new Date(Date.UTC(today.getUTCFullYear(), today.getUTCMonth() - 1, 1));
    return {
        start: prevMonth1.toISOString().slice(0, 10),
        end: curMonth1.toISOString().slice(0, 10),
    };
}

function normStage(s) {
    return String(s || '')
        .toLowerCase()
        .replace(/\s+/g, '');
}
const targets = {
    closedWon: new Set([
        'closedwon',
        '계약완료',
        '계약 완료'
    ].map(normStage)),
};
function isClosedWon(stage) { return targets.closedWon.has(normStage(stage)); }

async function fetchOppHistoriesByIds(instanceUrl, accessToken, oppIds) {
    if (!oppIds || oppIds.length === 0) return new Map();
    const chunkSize = 100;
    const allRows = [];

    for (let i = 0; i < oppIds.length; i += chunkSize) {
        const chunk = oppIds.slice(i, i + chunkSize).map(id => `'${esc(id)}'`).join(',');
        const soql = `
      SELECT CloseDate, CreatedDate, OpportunityId, PrevCloseDate, StageName
      FROM OpportunityHistory
      WHERE OpportunityId IN (${chunk})
      ORDER BY CreatedDate ASC
    `;
        const rows = await queryAll(instanceUrl, accessToken, soql);
        allRows.push(...rows);
    }

    const grouped = new Map();
    for (const r of allRows) {
        const id = r.OpportunityId;
        if (!grouped.has(id)) grouped.set(id, []);
        grouped.get(id).push(r);
    }
    return grouped;
}

async function fetchLeadsByConvertedOppIds(instanceUrl, accessToken, oppIds) {
    if (!oppIds?.length) return new Map();
    const chunk = 100;
    const out = [];
    for (let i = 0; i < oppIds.length; i += chunk) {
        const ids = oppIds.slice(i, i + chunk).map(id => `'${esc(id)}'`).join(',');
        const soql = `
        SELECT Id, CreatedDate, Company, LeadSource, utm__c, ConvertedOpportunityId
        FROM Lead
        WHERE IsConverted = true AND ConvertedOpportunityId IN (${ids})
      `;
        const rows = await queryAll(instanceUrl, accessToken, soql);
        out.push(...rows);
    }
    const byOpp = new Map();
    for (const r of out) byOpp.set(r.ConvertedOpportunityId, r);
    return byOpp;
}

async function fetchLeadsByIds(instanceUrl, accessToken, leadIds) {
    if (!leadIds?.length) return new Map();
    const chunk = 100;
    const out = [];
    for (let i = 0; i < leadIds.length; i += chunk) {
        const ids = leadIds.slice(i, i + chunk).map(id => `'${esc(id)}'`).join(',');
        const soql = `
        SELECT Id, CreatedDate, Company, LeadSource, utm__c, ConvertedOpportunityId
        FROM Lead
        WHERE Id IN (${ids})
      `;
        const rows = await queryAll(instanceUrl, accessToken, soql);
        out.push(...rows);
    }
    const byId = new Map();
    for (const r of out) byId.set(r.Id, r);
    return byId;
}

async function getContracts(params = {}, token) {
    if (!token?.access_token || !token?.instance_url) {
        throw makeError('Salesforce authentication required', 401, 'SF_TOKEN_MISSING');
    }

    const { month, start, end, ownerDept } = params;
    const { start: START, end: END } = computeRange({ month, start, end });

    const dept = (typeof ownerDept === 'string' && ownerDept.trim() !== '')
        ? ownerDept.trim()
        : null;

    const where = [
        "Opportunity__c != NULL",
        `ContractDateStart__c >= ${START}`,
        `ContractDateStart__c < ${END}`,
        `(ContractStatus__c = '계약서명완료' OR ContractStatus__c = '계약서명대기')`
    ];

    if (dept && dept !== 'ALL' && dept !== '*') {
        where.push(`Opportunity__r.Owner_Department__c = '${esc(dept)}'`);
    }

    const soql = `
  SELECT
    Id, Name, CreatedDate,
    ContractDateStart__c, ContractDateEnd__c,
    ContractStatus__c,
    Opportunity__c,
    Opportunity__r.LeadSource,
    Opportunity__r.RecordTypeId,
    Opportunity__r.RecordType.Name,
    Opportunity__r.BOUser__c,
    Opportunity__r.BOUser__r.Name,
    Opportunity__r.FieldUser__c,
    Opportunity__r.FieldUser__r.Name,
    Opportunity__r.StageName,
    Opportunity__r.Id,
    Opportunity__r.OwnerId,
    Opportunity__r.Owner.Name,
    Opportunity__r.Owner_Department__c,
    Opportunity__r.AccountId,
    Opportunity__r.Account.Name,
    Opportunity__r.Account.BranchName__c,
    Opportunity__r.ru_TabletQty__c,
    Opportunity__r.ru_MasterTabletQty__c,
    Opportunity__r.TotalNumberofEveryTablet__c,
    Opportunity__r.CreatedDate,
    Opportunity__r.ConvertedLeadID__c,
    Opportunity__r.fm_sido__c,
    Opportunity__r.fm_Sigugun__c,
    Opportunity__r.fm_StoreType__c,
    Account__c,
    Account__r.Name,
    Account__r.BranchName__c,
    Account__r.PLIndustry_First__c,
    Account__r.PLIndustry_Second__c,
    Account__r.TypeofB__c,
    (
      SELECT Id, fm_ContractProductFamily__c, TotalPrice__c, Quantity__c
      FROM ContractProductQuoteContract__r
    ),
    (
      SELECT Id, TotalAmount__c, PromotionName__r.Name
      FROM ContractProductPromotionContract__r
    )
  FROM Contract__c
  WHERE ${where.join(' AND ')}
  ORDER BY CreatedDate ASC
  `.trim();

    const accessToken = token.access_token;
    const instanceUrl = token.instance_url;

    const raw = await queryAll(instanceUrl, accessToken, soql);
    const list = raw.map(normalize);

    const oppIds = list.map(it => it.opportunity?.id).filter(Boolean);
    const histMap = await fetchOppHistoriesByIds(instanceUrl, accessToken, oppIds);

    for (const it of list) {
        const oppId = it.opportunity?.id;
        const hist = oppId ? (histMap.get(oppId) || []) : [];
        hist.sort((a, b) => new Date(a.CreatedDate) - new Date(b.CreatedDate));
        const idx = hist.findIndex(h => isClosedWon(h.StageName));
        const firstClosedWonAt = idx >= 0 ? hist[idx].CreatedDate : null;
        const beforeFirstClosedWonAt = (idx > 0) ? hist[idx - 1].CreatedDate : null;
        it.firstClosedWonAt = firstClosedWonAt;
        it.beforeFirstClosedWonAt = beforeFirstClosedWonAt;
        it.prevToFirstClose = diffDaysWithReason(beforeFirstClosedWonAt, firstClosedWonAt);
    }

    const leadIds = Array.from(new Set(
        list.map(it => it.convertedLeadId).filter(Boolean)
    ));
    const leadMapById = await fetchLeadsByIds(instanceUrl, accessToken, leadIds);

    const needsFallbackOppIds = list
        .filter(it => {
            const lid = it.opportunity?.convertedLeadId;
            if (!lid) return true;
            return !leadMapById.get(lid);
        })
        .map(it => it.opportunity.id)
        .filter(Boolean);

    const leadMapByOpp = await fetchLeadsByConvertedOppIds(instanceUrl, accessToken, needsFallbackOppIds);

    for (const it of list) {
        const lid = it.opportunity?.convertedLeadId;
        let lead = null;

        if (lid && leadMapById.has(lid)) {
            lead = leadMapById.get(lid);
        } else if (it.opportunity?.id && leadMapByOpp.has(it.opportunity.id)) {
            lead = leadMapByOpp.get(it.opportunity.id);
        }

        const utmRaw = lead?.utm__c ?? lead?.UTM__c ?? lead?.Utm__c ?? null;
        const { utmSource, utmContent, utmTerm } = parseUtmParams(utmRaw);

        it.lead = lead ? {
            id: lead.Id,
            createdDate: lead.CreatedDate,
            company: lead.Company || null,
            leadSource: lead.LeadSource || null,
            utm: utmRaw,
            utmSource,
            utmContent,
            utmTerm
        } : null;

        if (!it.lead) {
            it.leadReason = !lid ? 'missing-convertedLeadId'
                : (!/^00Q/i.test(lid) ? 'invalid-id-format'
                    : 'not-found-by-id-and-opportunity');
        }
        if (lead && it.opportunity?.createdDate) {
            it.leadToOpportunity = diffDaysWithReason(
                lead.CreatedDate,
                it.opportunity.createdDate
            );
        } else {
            it.leadToOpportunity = { days: null, reason: 'missing-date' };
        }
    }

    return list;
}

module.exports = {
    getContracts,
};
