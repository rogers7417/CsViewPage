// server.js
require('dotenv').config();
const express = require('express');
const cors = require('cors');
const axios = require('axios');
const { randomUUID } = require('crypto');
const { createClient } = require('redis');

const app = express();
app.use(cors());
app.use(express.json({ limit: '1mb' }));

function log(level, message, meta = {}) {
    const entry = {
        ts: new Date().toISOString(),
        level,
        message,
        ...meta,
    };
    const line = JSON.stringify(entry);
    if (level === 'error') console.error(line);
    else if (level === 'warn') console.warn(line);
    else console.log(line);
}

const redisUrl = process.env.REDIS_URL || 'redis://127.0.0.1:6379';
const redis = createClient({ url: redisUrl });
redis.on('error', (err) => {
    log('error', 'redis error', { err: err.message });
});
redis.connect().catch((err) => {
    log('error', 'redis connection error', { err: err.message });
});

const CATEGORY_LIMIT = 40;
const SUBCATEGORY_LIMIT = 120;
const PRODUCT_LIMIT = 400;
const OPTION_LIMIT = 200;

const templateKey = (brandId) => `brand:${brandId}:templates`;
const assignmentKey = (brandId) => `brand:${brandId}:assignments`;

function safeParse(json, fallback) {
    if (!json) return fallback;
    try {
        return JSON.parse(json);
    } catch (err) {
        console.warn('[redis] JSON parse error', err);
        return fallback;
    }
}

async function getBrandTemplates(brandId) {
    log('info', 'redis get brand templates', { brandId });
    const raw = await redis.get(templateKey(brandId));
    const templates = safeParse(raw, []);
    if (!Array.isArray(templates)) return [];
    return templates.map((tpl) => {
        const structure = normalizeStructure(tpl?.structure ?? tpl?.sections);
        const next = {
            ...tpl,
            structure,
        };
        delete next.sections;
        return next;
    });
}

async function setBrandTemplates(brandId, list) {
    log('info', 'redis set brand templates', { brandId, count: Array.isArray(list) ? list.length : 0 });
    const normalized = Array.isArray(list)
        ? list.map((tpl) => {
            const structure = normalizeStructure(tpl?.structure ?? tpl?.sections);
            const next = { ...tpl, structure };
            delete next.sections;
            return next;
        })
        : [];
    await redis.set(templateKey(brandId), JSON.stringify(normalized));
}

async function getBrandAssignments(brandId) {
    log('info', 'redis get assignments', { brandId });
    const raw = await redis.get(assignmentKey(brandId));
    const map = safeParse(raw, {});
    return map && typeof map === 'object' ? map : {};
}

async function setBrandAssignments(brandId, assignments) {
    log('info', 'redis set assignments', { brandId, count: assignments ? Object.keys(assignments).length : 0 });
    await redis.set(assignmentKey(brandId), JSON.stringify(assignments ?? {}));
}

function normalizeStructure(structure) {
    if (!Array.isArray(structure)) return [];
    return structure
        .slice(0, CATEGORY_LIMIT)
        .map((cat) => {
            const subcategories = Array.isArray(cat?.subcategories)
                ? cat.subcategories.slice(0, SUBCATEGORY_LIMIT).map((sub) => {
                    const products = Array.isArray(sub?.products)
                        ? sub.products.slice(0, PRODUCT_LIMIT).map((prod) => {
                            const rawGroups = Array.isArray(prod?.optionGroups)
                                ? prod.optionGroups
                                : (Array.isArray(prod?.options)
                                    ? [{
                                        id: randomUUID(),
                                        name: '기본 옵션',
                                        required: false,
                                        min: 0,
                                        max: 0,
                                        items: prod.options,
                                    }]
                                    : []);

                            const optionGroups = rawGroups.slice(0, OPTION_LIMIT).map((group) => ({
                                id: String(group?.id || randomUUID()),
                                name: String(group?.name ?? '').trim(),
                                required: Boolean(group?.required ?? false),
                                min: Number(group?.min ?? 0),
                                max: Number(group?.max ?? 0),
                                items: Array.isArray(group?.items)
                                    ? group.items.slice(0, OPTION_LIMIT).map((item) => ({
                                        id: String(item?.id || randomUUID()),
                                        name: String(item?.name ?? '').trim(),
                                    }))
                                    : [],
                            }));

                            return {
                                id: String(prod?.id || randomUUID()),
                                name: String(prod?.name ?? '').trim(),
                                isSale: prod?.isSale !== false,
                                isVisible: prod?.isVisible !== false,
                                optionGroups,
                            };
                        })
                        : [];
                    return {
                        id: String(sub?.id || randomUUID()),
                        title: String(sub?.title ?? '').trim(),
                        isVisible: sub?.isVisible !== false,
                        products,
                    };
                })
                : [];

            return {
                id: String(cat?.id || randomUUID()),
                title: String(cat?.title ?? '').trim(),
                isVisible: cat?.isVisible !== false,
                subcategories,
            };
        });
}

function pruneAssignments(assignments = {}, validTemplateIds = new Set()) {
    return Object.entries(assignments).reduce((acc, [storeId, templateId]) => {
        if (templateId && validTemplateIds.has(templateId)) {
            acc[storeId] = templateId;
        }
        return acc;
    }, {});
}

// ─────────────────────────────────────────────────────────────
// ① Salesforce 토큰
async function getSalesforceToken() {
    const url = `${process.env.SF_LOGIN_URL}/services/oauth2/token`;
    const params = new URLSearchParams();
    params.append('grant_type', 'password');
    params.append('client_id', process.env.SF_CLIENT_ID);
    params.append('client_secret', process.env.SF_CLIENT_SECRET);
    params.append('username', process.env.SF_USERNAME);
    params.append('password', decodeURIComponent(process.env.SF_PASSWORD));
    const res = await axios.post(url, params);
    return { accessToken: res.data.access_token, instanceUrl: res.data.instance_url };
}

const API_VER = process.env.SF_API_VERSION || 'v60.0';

// ─────────────────────────────────────────────────────────────
// ② 공통 GET + 페이지네이션
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

// ── 유틸: SOQL IN/리터럴 안전 처리
const esc = (s) => String(s ?? '').replace(/'/g, "\\'");


// ── 날짜 유틸: SF Date/DateTime 안전 파싱 + 일수 계산(이유 포함)
function parseSfDate(dateStr) {
    return dateStr ? new Date(`${dateStr}T00:00:00.000Z`) : null;
}
function parseSfDateTime(dtStr) {
    if (!dtStr) return null;
    const fixed = dtStr.replace(/([+-]\d{2})(\d{2})$/, '$1:$2'); // +0900 → +09:00
    const d = new Date(fixed);
    return isNaN(d) ? null : d;
}


function diffDaysWithReason(startISO, endISO) {
    if (!startISO || !endISO) {
        return { days: null, reason: "missing-date" };
    }
    const s = parseSfDateTime(startISO) || parseSfDate(startISO);
    const e = parseSfDateTime(endISO) || parseSfDate(endISO);
    if (!s || !e) {
        return { days: null, reason: "parse-error" };
    }
    const msPerDay = 24 * 60 * 60 * 1000;
    const days = Math.ceil((e.getTime() - s.getTime()) / msPerDay);
    return { days, reason: "ok" };
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

// ─────────────────────────────────────────────────────────────
// ③ OpportunityHistory 배치 조회 (설치진행/Closed Won)
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
        // console.log(soql);
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
// Lead 배치 조회 (100개 단위 IN)
async function fetchLeadsByConvertedOppIds(instanceUrl, accessToken, oppIds) {
    if (!oppIds?.length) return new Map();
    const chunk = 100, out = [];
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

// ─────────────────────────────────────────────────────────────
// ④ 정규화 (마이너스 상품 → 프로모션, 합계 계산 + 리드타임)
function normalize(record) {
    const opportunity = record.Opportunity__r || {};
    const account = record.Account__r || {};
    const opportunityAccount = opportunity.Account || {};

    const productRows = record.ContractProductQuoteContract__r?.records ?? [];
    const promoRows = record.ContractProductPromotionContract__r?.records ?? [];

    const BASE_PRICE = 648000;

    // Lead → Contract 소요일 계산: Opportunity.CreatedDate → ContractDateStart__c
    const oppCreated = opportunity?.CreatedDate || null;   // DateTime
    //const contractStart = record.ContractDateStart__c || null;         // Date
    const contractStart = record.CreatedDate;
    //const leadTimeDays = daysBetweenUTC(oppCreated, contractStart);

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
    const totalDiscount = promotionsFromProductsTotal + promotionsNativeTotal;

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
        // 리드타임(기존)
        convertedLeadId: opportunity?.ConvertedLeadID__c || null,
        leadTime,                       // { days, reason }
        leadTimeSource: {               // 프론트 디버깅 편의
            oppCreated,
            contractStart
        },
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

        // 금액 요약
        productsTotal,
        totalDiscount,
        promotionsFromProductsTotal,
        promotionsNativeTotal,
        purchaseAmount,
        vat,
        totalWithVat,

        products,
        promotions: [...nativePromotions, ...promotionsFromNegativeProducts],
    };
}

// ─────────────────────────────────────────────────────────────
// ⑤ 유틸: month → start/end, 파라미터 파싱
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
        .replace(/\s+/g, '');        // 공백 제거
}
const targets = {
    install: new Set([
        '설치진행',
        '계약진행',
        '재견적',
        '출고진행',
        'installationinprogress'
    ].map(normStage)),

    closedWon: new Set([
        'closedwon',
        '계약완료',
        '계약 완료'
    ].map(normStage)),
};

function isInstall(stage) { return targets.install.has(normStage(stage)); }
function isClosedWon(stage) { return targets.closedWon.has(normStage(stage)); }

// ─────────────────────────────────────────────────────────────
// ⑥ API: GET /contracts
app.get('/contracts', async (req, res) => {
    try {
        log('info', 'GET /contracts start', { query: req.query });
        const { month, start, end, ownerDept } = req.query;
        const { start: START, end: END } = computeRange({ month, start, end });

        // 1) 부서 파라미터 정규화 (빈 문자열/공백 → null)
        const dept = (typeof ownerDept === 'string' && ownerDept.trim() !== '')
            ? ownerDept.trim()
            : null;

        // 2) 공통 WHERE 절은 배열로 구성
        const where = [
            "Opportunity__c != NULL",
            `ContractDateStart__c >= ${START}`,
            `ContractDateStart__c < ${END}`,
            `(ContractStatus__c = '계약서명완료' OR ContractStatus__c = '계약서명대기')`
        ];

        // 3) 부서가 있으면 부서 조건을 추가, 없으면 전체 조회
        if (dept && dept !== 'ALL' && dept !== '*') {
            where.push(`Opportunity__r.Owner_Department__c = '${esc(dept)}'`);
        }

        const SOQL = `
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

        const { accessToken, instanceUrl } = await getSalesforceToken();
        const raw = await queryAll(instanceUrl, accessToken, SOQL);
        const list = raw.map(normalize);

        // ── 설치진행 → Closed Won 리드타임 계산 (OpportunityHistory 배치조회)
        const oppIds = list.map(it => it.opportunity?.id).filter(Boolean);
        const histMap = await fetchOppHistoriesByIds(instanceUrl, accessToken, oppIds);

        for (const it of list) {
            const oppId = it.opportunity?.id;
            const hist = oppId ? (histMap.get(oppId) || []) : [];

            // 안전: 시간 오름차순 보장
            hist.sort((a, b) => new Date(a.CreatedDate) - new Date(b.CreatedDate));

            // 최초 Closed Won의 인덱스
            const idx = hist.findIndex(h => isClosedWon(h.StageName));

            const firstClosedWonAt = idx >= 0 ? hist[idx].CreatedDate : null;
            const beforeFirstClosedWonAt = (idx > 0) ? hist[idx - 1].CreatedDate : null;

            // 원천값(옵션)
            it.firstClosedWonAt = firstClosedWonAt;
            it.beforeFirstClosedWonAt = beforeFirstClosedWonAt;

            // ⬅️ “직전 → 최초 Closed Won” 소요일
            it.prevToFirstClose = diffDaysWithReason(beforeFirstClosedWonAt, firstClosedWonAt); // { days, reason }

            // 기존 필드가 더 이상 필요 없으면 제거 또는 주석처리
            // delete it.installProgressAt;
            // delete it.closedWonAt;
            // delete it.installToClose;
            // delete it.installToCloseDays;
        }
        //  Lead 배치 조회해서 붙이기
        // 기존: convertedLeadId로 1차 조회



        // 신규: convertedLeadId가 없거나 조회 실패한 것들을 oppId로 백필
        const needsFallbackOppIds = list
            .filter(it => {
                const lid = it.opportunity?.convertedLeadId;
                if (!lid) return true;
                return !leadMapById.get(lid); // id가 있어도 못 찾은 케이스
            })
            .map(it => it.opportunity.id)
            .filter(Boolean);

        const leadMapByOpp = await fetchLeadsByConvertedOppIds(instanceUrl, accessToken, needsFallbackOppIds);

        // 최종 매핑
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

            // (선택) 왜 null인지 이유를 내려주고 싶다면:
            if (!it.lead) {
                it.leadReason = !lid ? 'missing-convertedLeadId'
                    : (!/^00Q/i.test(lid) ? 'invalid-id-format'
                        : 'not-found-by-id-and-opportunity');
            }
            // Lead → Opportunity 생성일 리드타임
            if (lead && it.opportunity?.createdDate) {
                it.leadToOpportunity = diffDaysWithReason(
                    lead.CreatedDate,
                    it.opportunity.createdDate
                ); // { days, reason }
            } else {
                it.leadToOpportunity = { days: null, reason: "missing-date" };
            }
        }

        res.setHeader('Content-Type', 'application/json; charset=utf-8');
        res.status(200).send(JSON.stringify(list, null, 2));
    } catch (err) {
        const msg = err.response?.data || err.message || String(err);
        res.status(500).json({ error: msg });
    }
});

// 헬스체크
app.get('/healthz', (req, res) => res.json({ ok: true }));

app.get('/opps/by-owner', async (req, res) => {
    try {
        log('info', 'GET /opps/by-owner start', { query: req.query });
        const { ownerId, month, start, end, stage, isWon, isClosed } = req.query;
        if (!ownerId) return res.status(400).json({ error: "ownerId is required" });

        // 기간 계산 (기존 함수 재사용)
        const { start: START, end: END } = computeRange({ month, start, end });

        // 다중 오너 지원: "id1,id2" → ('id1','id2')
        const ownerIds = ownerId.split(',').map(x => `'${esc(x.trim())}'`).join(',');

        // 필터 절 추가
        const conds = [
            `OwnerId IN (${ownerIds})`

        ];
        if (stage) conds.push(`StageName = '${esc(stage)}'`);
        if (isWon === 'true') conds.push(`IsWon = true`);
        if (isWon === 'false') conds.push(`IsWon = false`);
        if (isClosed === 'true') conds.push(`IsClosed = true`);
        if (isClosed === 'false') conds.push(`IsClosed = false`);

        const SOQL = `
        SELECT
          Id, Name, StageName, Probability,
          OwnerId, Owner.Name, Owner_Department__c,
          AccountId, Account.Name,
          LeadSource, Amount, Type,
          CreatedDate, CloseDate, IsClosed, IsWon
        FROM Opportunity
        WHERE ${conds.join(' AND ')}
        ORDER BY Owner.Name ASC, CreatedDate ASC
      `.trim();
        const { accessToken, instanceUrl } = await getSalesforceToken();
        const rows = await queryAll(instanceUrl, accessToken, SOQL);

        // 파생값: 경과일(나이), (성사 시) 생성→성사 소요일(올림)
        function ceilDays(a, b) {
            if (!a || !b) return null;
            const ms = (new Date(b)) - (new Date(a));
            return Math.ceil(ms / 86400000);
        }
        const nowISO = new Date().toISOString();

        const list = rows.map(r => ({
            id: r.Id,
            name: r.Name,
            stageName: r.StageName,
            probability: r.Probability,
            ownerId: r.OwnerId,
            ownerName: r.Owner?.Name,
            ownerDepartment: r.Owner_Department__c,
            accountId: r.AccountId,
            accountName: r.Account?.Name,
            leadSource: r.LeadSource,
            amount: r.Amount,
            type: r.Type,
            createdDate: r.CreatedDate,
            closeDate: r.CloseDate,
            isClosed: r.IsClosed,
            isWon: r.IsWon,
            // 파생
            ageDays: ceilDays(r.CreatedDate, nowISO),                 // 지금까지 경과일
            daysToClose: r.IsWon ? ceilDays(r.CreatedDate, r.CloseDate) : null // 성사 소요일(올림)
        }));

        res.setHeader('Content-Type', 'application/json; charset=utf-8');
        res.status(200).send(JSON.stringify(list, null, 2));
        log('info', 'GET /opps/by-owner success', { count: list.length });
    } catch (err) {
        const msg = err.response?.data || err.message || String(err);
        log('error', 'GET /opps/by-owner failed', { error: msg });
        res.status(500).json({ error: msg });
    }
});

// ⑧ (개정) API: GET /leads/count-by-owner  → groupBy 확장 (owner | status | owner_status | owner_status_nested)
app.get('/leads/count-by-owner', async (req, res) => {
    try {
        log('info', 'GET /leads/count-by-owner start', { query: req.query });
        const { month, start, end, ownerDept, isConverted, groupBy } = req.query;
        const { start: START, end: END } = computeRange({ month, start, end });

        const START_DT = `${START}T00:00:00Z`;
        const END_DT = `${END}T00:00:00Z`;

        // 🔹 고정 Stage 라벨 (프론트 컬럼명 그대로 사용)
        const FIXED_STATUSES = [
            '배정대기',
            '담당자 배정',
            '부재중',
            '리터치예정', // 공백 없이 통일(아래 정규화로 '리터치 예정'도 매칭)
            '고민중',
            '장기부재',
            '종료',
            'Qualified'
        ];
        const norm = s => String(s || '').replace(/\s+/g, '').toLowerCase();
        const FIXED_KEYS = new Map(FIXED_STATUSES.map(label => [norm(label), label]));
        const QUAL_KEY = norm('Qualified');

        // 부서 필터: User.Department, 콤마 다중
        const deptList = String(ownerDept || '아웃바운드세일즈')
            .split(',')
            .map(s => `'${esc(s.trim())}'`)
            .join(',');

        const conds = [
            `IsDeleted = FALSE`,
            `CreatedDate >= ${START_DT}`,
            `CreatedDate <  ${END_DT}`,
            `OwnerId IN (SELECT Id FROM User WHERE Department IN (${deptList}))`
        ];
        if (isConverted === 'true') conds.push(`IsConverted = true`);
        if (isConverted === 'false') conds.push(`IsConverted = false`);

        // 원본 Leads
        const SOQL = `
        SELECT Id, OwnerId, Owner.Name, Status, CreatedDate
        FROM Lead
        WHERE ${conds.join(' AND ')}
        ORDER BY Owner.Name ASC, CreatedDate ASC
      `.trim();

        const { accessToken, instanceUrl } = await getSalesforceToken();
        const rows = await queryAll(instanceUrl, accessToken, SOQL);

        // ─────────────────────────────────────────────
        // 1) ownerId → { ownerId, ownerName, ...고정Stage, total, conversionRate } 집계
        const byOwner = new Map();
        // footer(전체 합계)
        const footerCounts = new Map(FIXED_STATUSES.map(label => [label, 0]));
        let footerTotal = 0;

        for (const r of rows) {
            const ownerId = r.OwnerId || 'UNKNOWN';
            const ownerName = r.Owner?.Name || '미지정';
            const keyNorm = norm(r.Status);

            const label = FIXED_KEYS.get(keyNorm);
            if (!label) continue; // 고정 목록 외 상태는 스킵(필요시 '기타'로 묶어도 됨)

            if (!byOwner.has(ownerId)) {
                const init = { ownerId, ownerName };
                FIXED_STATUSES.forEach(lab => init[lab] = 0);
                init.total = 0;
                init.conversionRate = '0.0%';
                byOwner.set(ownerId, init);
            }

            const g = byOwner.get(ownerId);
            g[label] += 1;
            g.total += 1;

            footerCounts.set(label, (footerCounts.get(label) || 0) + 1);
            footerTotal += 1;
        }

        // 2) 전환율 계산 (Qualified / Total)
        for (const g of byOwner.values()) {
            const qualified = Number(g['Qualified'] || 0);
            const total = Number(g.total || 0);
            g.conversionRate = total > 0 ? ((qualified / total) * 100).toFixed(1) + '%' : '0.0%';
        }

        // 3) rows 정렬
        const outRows = [...byOwner.values()].sort((a, b) =>
            a.ownerName.localeCompare(b.ownerName, 'ko') || a.ownerId.localeCompare(b.ownerId)
        );

        // 4) footer(Grand Totals)
        const totalQualified = footerCounts.get(FIXED_KEYS.get(QUAL_KEY)) || 0;
        const footerConversion = footerTotal > 0
            ? ((totalQualified / footerTotal) * 100).toFixed(1) + '%'
            : '0.0%';

        const footer = { ownerName: '총 합계' };
        FIXED_STATUSES.forEach(label => { footer[label] = footerCounts.get(label) || 0; });
        footer.total = footerTotal;
        footer.conversionRate = footerConversion;

        // 5) 모드 정보(선택)
        const mode = (groupBy || 'owner_status_fixed').toLowerCase();

        res.setHeader('Content-Type', 'application/json; charset=utf-8');
        res.status(200).send(JSON.stringify({
            range: { start: START, end: END },
            ownerDept: String(ownerDept || '아웃바운드세일즈'),
            isConverted: (isConverted ?? null),
            groupBy: mode,
            fixedStatuses: FIXED_STATUSES,
            totalCount: footerTotal,
            rows: outRows,
            footer
        }, null, 2));
        log('info', 'GET /leads/count-by-owner success', { owners: outRows.length, totalCount: footerTotal });
    } catch (err) {
        const msg = err.response?.data || err.message || String(err);
        log('error', 'GET /leads/count-by-owner failed', { error: msg });
        res.status(500).json({ error: msg });
    }
});

app.get('/accounts/fr-accounts', async (req, res) => {
    try {
        log('info', 'GET /accounts/fr-accounts start', { query: req.query });
        // 기본값: 법인 / 프랜차이즈본사
        const businessType = (req.query.businessType ?? '법인').toString();
        const accountType = (req.query.accountType ?? '프랜차이즈본사').toString();

        const where = [
            `fm_TypeofBusiness__c = '${esc(businessType)}'`,
            `fm_AccountType__c = '${esc(accountType)}'`
        ].join(' AND ');

        const SOQL = `
        SELECT
          Id, Name,
          (SELECT Id, Name FROM FRAccounts__r)
        FROM Account
        WHERE ${where}
        ORDER BY Name ASC
      `.trim();

        const { accessToken, instanceUrl } = await getSalesforceToken();
        const rows = await queryAll(instanceUrl, accessToken, SOQL);

        // 그대로 내보내도 되지만, 사용 편의를 위해 간단 변환(옵션)
        // ?compact=true 파라미터가 있으면 children만 깔끔하게 정리
        if (String(req.query.compact || '').toLowerCase() === 'true') {
            const compacted = rows.map(r => ({
                id: r.Id,
                name: r.Name,
                children: (r.FRAccounts__r?.records || []).map(c => ({
                    id: c.Id,
                    name: c.Name,
                })),
            }));
            res.setHeader('Content-Type', 'application/json; charset=utf-8');
            log('info', 'GET /accounts/fr-accounts success', { count: compacted.length, compact: true });
            return res.status(200).send(JSON.stringify(compacted, null, 2));
        }

        // raw 형식 반환
        res.setHeader('Content-Type', 'application/json; charset=utf-8');
        res.status(200).send(JSON.stringify(rows, null, 2));
        log('info', 'GET /accounts/fr-accounts success', { count: rows.length, compact: false });
    } catch (err) {
        const msg = err.response?.data || err.message || String(err);
        log('error', 'GET /accounts/fr-accounts failed', { error: msg });
        res.status(500).json({ error: msg });
    }
});

// GET /stores/by-brand?brandId=xxx
app.get('/stores/by-brand', async (req, res) => {
    try {
        log('info', 'GET /stores/by-brand start', { query: req.query });
        const brandId = (req.query.brandId || '').trim();
        if (!brandId) return res.status(400).json({ error: 'brandId is required' });

        // 1) 매장 + 영업기회까지
        const soql = `
        SELECT Id, Name, Brand_Branch__c, ContractStatus__c, LicenseActive__c, ContractTabletQuantity__c,
          (
            SELECT Id, Name, StageName, TotalNumberofEveryTablet__c,ru_MITotalAmount2__c, CloseDate, OwnerId, Owner.Name
            FROM Opportunities
          )
        FROM Account
        WHERE FRBrand__c = '${esc(brandId)}'
        ORDER BY Name
      `.trim();

        const { accessToken, instanceUrl } = await getSalesforceToken();
        const accounts = await queryAll(instanceUrl, accessToken, soql);

        // 2) 모든 기회 Id 모아 계약(Contract__c) 배치 조회
        const allOppIds = [];
        for (const a of accounts) {
            const opps = a.Opportunities?.records || [];
            for (const o of opps) if (o?.Id) allOppIds.push(o.Id);
        }

        const contractMap = await fetchContractsByOppIds(instanceUrl, accessToken, allOppIds);

        // 3) 응답 매핑: 매장 → 기회[] → 계약[]
        const stores = accounts.map(a => {
            const opps = (a.Opportunities?.records || []).map(o => ({
                id: o.Id,
                name: o.Name,
                stage: o.StageName,
                TotalTablet: o.TotalNumberofEveryTablet__c,
                TotalAmount2: o.ru_MITotalAmount2__c,
                closeDate: o.CloseDate,
                ownerId: o.OwnerId,
                ownerName: o.Owner?.Name,
                // 이 기회에 연결된 커스텀 계약들
                contracts: (contractMap.get(o.Id) || []).map(c => ({
                    id: c.Id,
                    name: c.Name,
                    status: c.ContractStatus__c,
                    startDate: c.ContractDateStart__c,
                    endDate: c.ContractDateEnd__c
                }))
            }));

            return {
                id: a.Id,
                name: a.Name,
                branch: a.Brand_Branch__c ?? null,
                contractStatus: a.ContractStatus__c ?? null,
                licenseActive: a.LicenseActive__c === true,
                tabletCount: Number(a.ContractTabletQuantity__c ?? 0),
                opportunities: opps
            };
        });

        res.setHeader('Content-Type', 'application/json; charset=utf-8');
        log('info', 'GET /stores/by-brand success', { count: stores.length });
        return res.status(200).json(stores);
    } catch (err) {
        const msg = err.response?.data || err.message || String(err);
        log('error', 'GET /stores/by-brand failed', { error: msg });
        res.status(500).json({ error: msg });
    }
});

// ─────────────────────────────────────────────────────────────
// ⑤ 브랜드 템플릿 & 매장 매핑 (Redis)

app.get('/templates/by-brand', async (req, res) => {
    const brandId = String(req.query.brandId || '').trim();
    if (!brandId) return res.status(400).json({ error: 'brandId is required' });
    try {
        log('info', 'GET /templates/by-brand start', { brandId });
        const [templates, assignments] = await Promise.all([
            getBrandTemplates(brandId),
            getBrandAssignments(brandId),
        ]);
        res.setHeader('Content-Type', 'application/json; charset=utf-8');
        res.status(200).json({ templates, assignments });
        log('info', 'GET /templates/by-brand success', { brandId, templateCount: templates.length, assignmentCount: Object.keys(assignments || {}).length });
    } catch (err) {
        const msg = err.message || String(err);
        log('error', 'GET /templates/by-brand failed', { brandId, error: msg });
        res.status(500).json({ error: msg });
    }
});

app.post('/templates', async (req, res) => {
    const brandId = String(req.body?.brandId || '').trim();
    const name = String(req.body?.name || '').trim();
    const description = String(req.body?.description || '').trim();
    const structure = normalizeStructure(req.body?.structure ?? req.body?.sections);

    if (!brandId) return res.status(400).json({ error: 'brandId is required' });
    if (!name) return res.status(400).json({ error: '템플릿 제목은 필수입니다.' });

    try {
        log('info', 'POST /templates start', { brandId, name });
        const templates = await getBrandTemplates(brandId);
        const now = new Date().toISOString();
        const template = {
            id: randomUUID(),
            brandId,
            name,
            description,
            structure,
            createdAt: now,
            updatedAt: now,
        };

        templates.push(template);
        await setBrandTemplates(brandId, templates);

        const assignments = await getBrandAssignments(brandId);
        const validIds = new Set(templates.map((t) => t.id));
        await setBrandAssignments(brandId, pruneAssignments(assignments, validIds));

        res.status(201).json({ template });
        log('info', 'POST /templates success', { brandId, templateId: template.id });
    } catch (err) {
        const msg = err.message || String(err);
        log('error', 'POST /templates failed', { brandId, error: msg });
        res.status(500).json({ error: msg });
    }
});

app.put('/templates/:id', async (req, res) => {
    const templateId = String(req.params.id || '').trim();
    const brandId = String(req.body?.brandId || '').trim();
    const name = String(req.body?.name || '').trim();
    const description = String(req.body?.description || '').trim();
    const structure = normalizeStructure(req.body?.structure ?? req.body?.sections);

    if (!brandId) return res.status(400).json({ error: 'brandId is required' });
    if (!templateId) return res.status(400).json({ error: 'templateId is required' });
    if (!name) return res.status(400).json({ error: '템플릿 제목은 필수입니다.' });

    try {
        log('info', 'PUT /templates/:id start', { brandId, templateId, name });
        const templates = await getBrandTemplates(brandId);
        const idx = templates.findIndex((t) => t.id === templateId);
        if (idx === -1) return res.status(404).json({ error: 'template not found' });

        templates[idx] = {
            ...templates[idx],
            name,
            description,
            structure,
            updatedAt: new Date().toISOString(),
        };
        delete templates[idx].sections;

        await setBrandTemplates(brandId, templates);

        res.json({ template: templates[idx] });
        log('info', 'PUT /templates/:id success', { brandId, templateId });
    } catch (err) {
        const msg = err.message || String(err);
        log('error', 'PUT /templates/:id failed', { brandId, templateId, error: msg });
        res.status(500).json({ error: msg });
    }
});

app.delete('/templates/:id', async (req, res) => {
    const templateId = String(req.params.id || '').trim();
    const brandId = String((req.query.brandId || req.body?.brandId || '').trim());

    if (!brandId) return res.status(400).json({ error: 'brandId is required' });
    if (!templateId) return res.status(400).json({ error: 'templateId is required' });

    try {
        log('info', 'DELETE /templates/:id start', { brandId, templateId });
        const templates = await getBrandTemplates(brandId);
        const filtered = templates.filter((t) => t.id !== templateId);
        if (filtered.length === templates.length) return res.status(404).json({ error: 'template not found' });

        await setBrandTemplates(brandId, filtered);

        const assignments = await getBrandAssignments(brandId);
        const prunedAssignments = Object.entries(assignments).reduce((acc, [storeId, tid]) => {
            if (tid && tid !== templateId) acc[storeId] = tid;
            return acc;
        }, {});
        await setBrandAssignments(brandId, prunedAssignments);

        res.json({ ok: true });
        log('info', 'DELETE /templates/:id success', { brandId, templateId });
    } catch (err) {
        const msg = err.message || String(err);
        log('error', 'DELETE /templates/:id failed', { brandId, templateId, error: msg });
        res.status(500).json({ error: msg });
    }
});

app.post('/templates/assign', async (req, res) => {
    const brandId = String(req.body?.brandId || '').trim();
    const storeId = String(req.body?.storeId || '').trim();
    const templateId = String(req.body?.templateId || '').trim();

    if (!brandId) return res.status(400).json({ error: 'brandId is required' });
    if (!storeId) return res.status(400).json({ error: 'storeId is required' });
    if (!templateId) return res.status(400).json({ error: 'templateId is required' });

    try {
        log('info', 'POST /templates/assign start', { brandId, storeId, templateId });
        const templates = await getBrandTemplates(brandId);
        if (!templates.some((t) => t.id === templateId)) {
            return res.status(404).json({ error: 'template not found' });
        }

        const assignments = await getBrandAssignments(brandId);
        assignments[storeId] = templateId;
        await setBrandAssignments(brandId, assignments);

        res.json({ storeId, templateId });
        log('info', 'POST /templates/assign success', { brandId, storeId, templateId });
    } catch (err) {
        const msg = err.message || String(err);
        log('error', 'POST /templates/assign failed', { brandId, storeId, templateId, error: msg });
        res.status(500).json({ error: msg });
    }
});

app.delete('/templates/assign', async (req, res) => {
    const brandId = String((req.body?.brandId || req.query.brandId || '').trim());
    const storeId = String((req.body?.storeId || req.query.storeId || '').trim());

    if (!brandId) return res.status(400).json({ error: 'brandId is required' });
    if (!storeId) return res.status(400).json({ error: 'storeId is required' });

    try {
        log('info', 'DELETE /templates/assign start', { brandId, storeId });
        const assignments = await getBrandAssignments(brandId);
        if (storeId in assignments) delete assignments[storeId];
        await setBrandAssignments(brandId, assignments);
        res.json({ storeId, templateId: null });
        log('info', 'DELETE /templates/assign success', { brandId, storeId });
    } catch (err) {
        const msg = err.message || String(err);
        log('error', 'DELETE /templates/assign failed', { brandId, storeId, error: msg });
        res.status(500).json({ error: msg });
    }
});

app.get('/leads/daily-by-owner', async (req, res) => {
    try {
        const { month, start, end, ownerDept, isConverted } = req.query;
        const { start: START, end: END } = computeRange({ month, start, end });

        const START_DT = `${START}T00:00:00Z`;
        const END_DT = `${END}T00:00:00Z`;

        // 부서 필터(User.Department IN (...)) - 기본 아웃바운드세일즈
        const deptList = String(ownerDept || '아웃바운드세일즈')
            .split(',')
            .map(s => `'${esc(s.trim())}'`)
            .join(',');

        const conds = [
            `IsDeleted = FALSE`,
            `CreatedDate >= ${START_DT}`,
            `CreatedDate <  ${END_DT}`,
            `OwnerId IN (SELECT Id FROM User WHERE Department IN (${deptList}))`,
        ];
        if (isConverted === 'true') conds.push(`IsConverted = true`);
        if (isConverted === 'false') conds.push(`IsConverted = false`);

        const SOQL = `
        SELECT Id, OwnerId, Owner.Name, CreatedDate
        FROM Lead
        WHERE ${conds.join(' AND ')}
        ORDER BY Owner.Name ASC, CreatedDate ASC
      `.trim();

        const { accessToken, instanceUrl } = await getSalesforceToken();
        const rows = await queryAll(instanceUrl, accessToken, SOQL);

        // 월 전체 날짜키 (KST 기준으로 보여줄 키)
        const dateKeys = makeDateKeys(START, END);

        // ownerId → { ownerId, ownerName, daily{dateKey:count}, total }
        const byOwner = new Map();
        let footerTotal = 0;

        for (const r of rows) {
            const ownerId = r.OwnerId || 'UNKNOWN';
            const ownerName = (r.Owner && r.Owner.Name) || '미지정';
            const kstKey = toKstDateKey(r.CreatedDate);
            if (!kstKey) continue;

            if (!byOwner.has(ownerId)) {
                const daily = {};
                dateKeys.forEach(k => (daily[k] = 0));
                byOwner.set(ownerId, { ownerId, ownerName, daily, total: 0 });
            }

            const g = byOwner.get(ownerId);
            if (kstKey in g.daily) g.daily[kstKey] += 1;
            g.total += 1;
            footerTotal += 1;
        }

        const owners = [...byOwner.values()]
            .sort((a, b) => a.ownerName.localeCompare(b.ownerName, 'ko') || a.ownerId.localeCompare(b.ownerId));

        // footer 일자 합계
        const footerDaily = dateKeys.reduce((acc, k) => {
            acc[k] = owners.reduce((sum, o) => sum + (o.daily[k] || 0), 0);
            return acc;
        }, {});

        const footer = { total: footerTotal, daily: footerDaily };

        res.setHeader('Content-Type', 'application/json; charset=utf-8');
        res.status(200).send(JSON.stringify({
            range: { start: START, end: END }, // 'YYYY-MM-01' ~ '다음달-01'(미포함)
            month: month || null,
            ownerDept: String(ownerDept || '아웃바운드세일즈'),
            isConverted: (isConverted ?? null), // 요청 에코만, 응답 집계에는 미반영
            dateKeys,                           // x축용
            owners,                             // [{ ownerId, ownerName, daily{yyyy-MM-dd}, total }]
            footer,                             // { total, daily{...} }
            soql: SOQL,                         // (디버그용) 필요 없으면 제거
        }, null, 2));
        log('info', 'GET /contracts success', { count: list.length });
    } catch (err) {
        const msg = err.response?.data || err.message || String(err);
        log('error', 'GET /contracts failed', { error: msg });
        res.status(500).json({ error: msg });
    }
});
// ── UTC ISO → KST yyyy-MM-dd
function toKstDateKey(isoUtc) {
    if (!isoUtc) return null;
    const d = new Date(isoUtc);
    const kst = new Date(d.getTime() + 9 * 60 * 60 * 1000); // +09:00
    return kst.toISOString().slice(0, 10);
}

// ── 월 전체 날짜키 생성 ['YYYY-MM-01', ...]
function makeDateKeys(start, end) {
    const keys = [];
    let d = new Date(start + 'T00:00:00Z');
    const endDt = new Date(end + 'T00:00:00Z');
    while (d < endDt) {
        keys.push(d.toISOString().slice(0, 10));
        d = new Date(d.getTime() + 24 * 60 * 60 * 1000);
    }
    return keys;
}
async function fetchContractsByOppIds(instanceUrl, accessToken, oppIds) {
    if (!oppIds?.length) return new Map();
    const chunkSize = 100;
    const all = [];
    for (let i = 0; i < oppIds.length; i += chunkSize) {
        const ids = oppIds.slice(i, i + chunkSize).map(id => `'${esc(id)}'`).join(',');
        const soql = `
      SELECT Id, Name, ContractStatus__c, ContractDateStart__c, ContractDateEnd__c, Opportunity__c
      FROM Contract__c
      WHERE Opportunity__c IN (${ids}) 
    `;
        const rows = await queryAll(instanceUrl, accessToken, soql);
        all.push(...rows);
    }
    // Opportunity__c → [contracts]
    const byOpp = new Map();
    for (const r of all) {
        const k = r.Opportunity__c;
        if (!byOpp.has(k)) byOpp.set(k, []);
        byOpp.get(k).push(r);
    }
    return byOpp;
}
// ⑥ API: GET /contracts
app.get('/contracts', async (req, res) => {
    try {
        log('info', 'GET /contracts start', { query: req.query });
        const { month, start, end, ownerDept } = req.query;
        const { start: START, end: END } = computeRange({ month, start, end });

        // 1) 부서 파라미터 정규화 (빈 문자열/공백 → null)
        const dept = (typeof ownerDept === 'string' && ownerDept.trim() !== '')
            ? ownerDept.trim()
            : null;

        // 2) 공통 WHERE 절은 배열로 구성
        const where = [
            "Opportunity__c != NULL",
            `ContractDateStart__c >= ${START}`,
            `ContractDateStart__c < ${END}`,
            `(ContractStatus__c = '계약서명완료' OR ContractStatus__c = '계약서명대기')`
        ];

        // 3) 부서가 있으면 부서 조건을 추가, 없으면 전체 조회
        if (dept && dept !== 'ALL' && dept !== '*') {
            where.push(`Opportunity__r.Owner_Department__c = '${esc(dept)}'`);
        }

        const SOQL = `
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

        const { accessToken, instanceUrl } = await getSalesforceToken();
        const raw = await queryAll(instanceUrl, accessToken, SOQL);
        const list = raw.map(normalize);

        // ── 설치진행 → Closed Won 리드타임 계산 (OpportunityHistory 배치조회)
        const oppIds = list.map(it => it.opportunity?.id).filter(Boolean);
        const histMap = await fetchOppHistoriesByIds(instanceUrl, accessToken, oppIds);

        for (const it of list) {
            const oppId = it.opportunity?.id;
            const hist = oppId ? (histMap.get(oppId) || []) : [];

            // 안전: 시간 오름차순 보장
            hist.sort((a, b) => new Date(a.CreatedDate) - new Date(b.CreatedDate));

            // 최초 Closed Won의 인덱스
            const idx = hist.findIndex(h => isClosedWon(h.StageName));

            const firstClosedWonAt = idx >= 0 ? hist[idx].CreatedDate : null;
            const beforeFirstClosedWonAt = (idx > 0) ? hist[idx - 1].CreatedDate : null;

            // 원천값(옵션)
            it.firstClosedWonAt = firstClosedWonAt;
            it.beforeFirstClosedWonAt = beforeFirstClosedWonAt;

            // ⬅️ “직전 → 최초 Closed Won” 소요일
            it.prevToFirstClose = diffDaysWithReason(beforeFirstClosedWonAt, firstClosedWonAt); // { days, reason }

            // 기존 필드가 더 이상 필요 없으면 제거 또는 주석처리
            // delete it.installProgressAt;
            // delete it.closedWonAt;
            // delete it.installToClose;
            // delete it.installToCloseDays;
        }
        //  Lead 배치 조회해서 붙이기
        // 기존: convertedLeadId로 1차 조회



        // 신규: convertedLeadId가 없거나 조회 실패한 것들을 oppId로 백필
        const needsFallbackOppIds = list
            .filter(it => {
                const lid = it.opportunity?.convertedLeadId;
                if (!lid) return true;
                return !leadMapById.get(lid); // id가 있어도 못 찾은 케이스
            })
            .map(it => it.opportunity.id)
            .filter(Boolean);

        const leadMapByOpp = await fetchLeadsByConvertedOppIds(instanceUrl, accessToken, needsFallbackOppIds);

        // 최종 매핑
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

            // (선택) 왜 null인지 이유를 내려주고 싶다면:
            if (!it.lead) {
                it.leadReason = !lid ? 'missing-convertedLeadId'
                    : (!/^00Q/i.test(lid) ? 'invalid-id-format'
                        : 'not-found-by-id-and-opportunity');
            }
            // Lead → Opportunity 생성일 리드타임
            if (lead && it.opportunity?.createdDate) {
                it.leadToOpportunity = diffDaysWithReason(
                    lead.CreatedDate,
                    it.opportunity.createdDate
                ); // { days, reason }
            } else {
                it.leadToOpportunity = { days: null, reason: "missing-date" };
            }
        }

        res.setHeader('Content-Type', 'application/json; charset=utf-8');
        res.status(200).send(JSON.stringify(list, null, 2));
    } catch (err) {
        const msg = err.response?.data || err.message || String(err);
        res.status(500).json({ error: msg });
    }
});

app.get('/snapshot/latest', async (_req, res) => {
    try {
        const collSnap = await initSnapshotMongo();
        const doc = await collSnap.find().sort({ date: -1 }).limit(1).next();
        if (!doc) {
            return res.status(404).json({ error: 'Snapshot not found' });
        }
        res.json(doc);
    } catch (err) {
        log('error', 'snapshot latest error', { err: err.message || err });
        res.status(500).json({ error: err.message || String(err) });
    }
});
const PORT = process.env.PORT || 4000;
app.listen(PORT, () => {
    console.log(`✅ Listening on http://localhost:${PORT}`);
});
