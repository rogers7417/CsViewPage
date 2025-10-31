const { MongoClient } = require('mongodb');

const CONTRACT_STATUSES = [
    '계약서명완료',
    '계약서명대기',
];

const CONTRACT_MONGO_URI = process.env.CONTRACT_MONGO_URI
    || process.env.CONTRACT_MONGO_URL
    || process.env.SNAPSHOT_MONGO_URI
    || process.env.MONGO_URI;
const CONTRACT_MONGO_DB = process.env.CONTRACT_MONGO_DB
    || process.env.CONTRACT_MONGO_DB_NAME
    || process.env.SNAPSHOT_DB_NAME
    || process.env.SNAPSHOT_MONGO_DB
    || process.env.MONGO_DB_NAME
    || 'salesforceSendBox';
const CONTRACT_MONGO_COLLECTION = process.env.CONTRACT_MONGO_COLLECTION
    || process.env.CONTRACT_MONGO_COLL
    || process.env.SNAPSHOT_COLL
    || process.env.SNAPSHOT_MONGO_COLLECTION
    || process.env.MONGO_COLL
    || 'CurrentContract';
const CONTRACT_MONGO_POOL_SIZE = Number(process.env.CONTRACT_MONGO_POOL_SIZE
    || process.env.CONTRACT_MONGO_POOL
    || process.env.SNAPSHOT_MONGO_POOL_SIZE
    || 5);

let mongoClientPromise = null;

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
    const fixed = dtStr.replace(/([+-]\d{2})(\d{2})$/, '$1:$2');
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

async function getContractsCollection() {
    if (!CONTRACT_MONGO_URI) {
        throw makeError('CONTRACT_MONGO_URI is not configured', 500, 'MONGO_URI_MISSING');
    }

    if (!mongoClientPromise) {
        const client = new MongoClient(CONTRACT_MONGO_URI, {
            maxPoolSize: CONTRACT_MONGO_POOL_SIZE > 0 ? CONTRACT_MONGO_POOL_SIZE : 5,
        });
        mongoClientPromise = client.connect().catch((err) => {
            mongoClientPromise = null;
            throw err;
        });
    }

    const client = await mongoClientPromise;
    return client.db(CONTRACT_MONGO_DB).collection(CONTRACT_MONGO_COLLECTION);
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

function toPlainObject(doc) {
    let plain;
    try {
        plain = JSON.parse(JSON.stringify(doc));
    } catch (err) {
        plain = { ...doc };
    }
    if (plain && plain._id != null) {
        plain._id = String(plain._id);
    }
    return plain;
}

function ensureTotals(contract) {
    const products = Array.isArray(contract.products) ? contract.products : [];
    const promotions = Array.isArray(contract.promotions) ? contract.promotions : [];

    const productsTotal = products.reduce((sum, it) => {
        const qty = Number(it.quantity ?? 0);
        const price = Number(it.totalPrice ?? 0);
        if (!Number.isFinite(qty) || !Number.isFinite(price)) return sum;
        return sum + (price * qty);
    }, 0);

    const promoTotals = promotions.reduce((acc, it) => {
        const amount = Number(it.totalAmount ?? 0);
        if (!Number.isFinite(amount)) return acc;
        if (it._source === 'native') {
            acc.native += amount;
        } else if (it._source === 'product') {
            acc.product += amount;
        } else {
            acc.unknown += amount;
        }
        return acc;
    }, { native: 0, product: 0, unknown: 0 });

    const promotionsFromProductsTotal = Number.isFinite(Number(contract.promotionsFromProductsTotal))
        ? Number(contract.promotionsFromProductsTotal)
        : promoTotals.product;
    const promotionsNativeTotal = Number.isFinite(Number(contract.promotionsNativeTotal))
        ? Number(contract.promotionsNativeTotal)
        : promoTotals.native;
    const promotionsOtherTotal = promoTotals.unknown;

    const purchaseAmount = Number.isFinite(Number(contract.purchaseAmount))
        ? Number(contract.purchaseAmount)
        : productsTotal - promotionsFromProductsTotal;

    const vat = Number.isFinite(Number(contract.vat))
        ? Number(contract.vat)
        : Math.floor(purchaseAmount * 0.1);

    const totalWithVat = Number.isFinite(Number(contract.totalWithVat))
        ? Number(contract.totalWithVat)
        : purchaseAmount + vat;

    const promotionsTotal = promotionsFromProductsTotal + promotionsNativeTotal + promotionsOtherTotal;

    return {
        productsTotal,
        promotionsFromProductsTotal,
        promotionsNativeTotal,
        promotionsTotal,
        totalDiscount: Number.isFinite(Number(contract.totalDiscount))
            ? Number(contract.totalDiscount)
            : promotionsTotal,
        purchaseAmount,
        vat,
        totalWithVat,
    };
}

function ensureLeadInfo(contract) {
    const opportunity = contract.opportunity || {};
    const lead = contract.lead || null;

    const oppCreated = contract.leadTimeSource?.oppCreated || opportunity.createdDate || null;
    const contractStart = contract.leadTimeSource?.contractStart
        || contract.createdDate
        || contract.contractDateStart
        || null;

    const leadTime = contract.leadTime || diffDaysWithReason(oppCreated, contractStart);
    const leadToOpportunity = contract.leadToOpportunity
        || diffDaysWithReason(lead?.createdDate, opportunity.createdDate);

    let normalizedLead = lead;
    if (lead && (lead.utm || lead.utm__c) && (!lead.utmSource && !lead.utmContent && !lead.utmTerm)) {
        const utmRaw = lead.utm || lead.utm__c;
        const parsedUtm = parseUtmParams(utmRaw);
        normalizedLead = {
            ...lead,
            utm: utmRaw,
            utmSource: parsedUtm.utmSource,
            utmContent: parsedUtm.utmContent,
            utmTerm: parsedUtm.utmTerm,
        };
    }

    return {
        lead: normalizedLead,
        leadTime,
        leadTimeSource: {
            oppCreated,
            contractStart,
        },
        leadToOpportunity,
    };
}

function hydrateContract(doc) {
    const contract = toPlainObject(doc);

    const mongoId = contract._id || (doc && doc._id ? String(doc._id) : null);
    contract.id = contract.id || contract.sfId || contract.Id || mongoId || null;
    contract.sfId = contract.sfId || contract.id || contract.Id || null;
    contract.opportunity = contract.opportunity ? { ...contract.opportunity } : {};
    contract.account = contract.account ? { ...contract.account } : undefined;
    contract.products = Array.isArray(contract.products) ? [...contract.products] : [];
    contract.promotions = Array.isArray(contract.promotions) ? [...contract.promotions] : [];

    if (contract.syncedAt) {
        const ts = new Date(contract.syncedAt);
        contract.syncedAt = Number.isNaN(ts.getTime()) ? contract.syncedAt : ts.toISOString();
    }

    const totals = ensureTotals(contract);
    contract.productsTotal = totals.productsTotal;
    contract.promotionsFromProductsTotal = totals.promotionsFromProductsTotal;
    contract.promotionsNativeTotal = totals.promotionsNativeTotal;
    contract.promotionsTotal = totals.promotionsTotal;
    contract.totalDiscount = totals.totalDiscount;
    contract.purchaseAmount = totals.purchaseAmount;
    contract.vat = totals.vat;
    contract.totalWithVat = totals.totalWithVat;

    const leadMeta = ensureLeadInfo(contract);
    contract.lead = leadMeta.lead;
    contract.leadTime = leadMeta.leadTime;
    contract.leadTimeSource = leadMeta.leadTimeSource;
    contract.leadToOpportunity = leadMeta.leadToOpportunity;

    return contract;
}

async function getContracts(params = {}) {
    const collection = await getContractsCollection();

    const { month, start, end, ownerDept } = params;
    const { start: START, end: END } = computeRange({ month, start, end });
    const filters = [];

    const startDateISO = `${START}T00:00:00.000Z`;
    const endDateISO = `${END}T00:00:00.000Z`;
    const startDate = new Date(startDateISO);
    const endDate = new Date(endDateISO);
    const isValidDate = (d) => d instanceof Date && !Number.isNaN(d.getTime());

    const dateFields = [
        'contractDateStart',
        'contractDateStart__c',
        'ContractDateStart__c',
    ];

    const dateClauses = [];
    dateFields.forEach((field) => {
        if (isValidDate(startDate) && isValidDate(endDate)) {
            dateClauses.push({ [field]: { $gte: startDate, $lt: endDate } });
        }
        dateClauses.push({ [field]: { $gte: START, $lt: END } });
    });
    if (dateClauses.length) {
        filters.push({ $or: dateClauses });
    }

    if (CONTRACT_STATUSES.length) {
        const statusFields = [
            'contractStatus',
            'contractStatus__c',
            'ContractStatus__c',
        ];
        const statusClauses = statusFields.map((field) => ({ [field]: { $in: CONTRACT_STATUSES } }));
        filters.push({ $or: statusClauses });
    }

    if (typeof ownerDept === 'string') {
        const trimmed = ownerDept.trim();
        if (trimmed && trimmed !== 'ALL' && trimmed !== '*') {
            const deptFields = [
                'opportunity.ownerDepartment',
                'opportunity.ownerDepartment__c',
                'opportunity.owner_Department__c',
                'opportunity.owner.Department__c',
                'opportunity.ownerDepartmentName',
            ];
            const deptClauses = deptFields.map((field) => ({ [field]: trimmed }));
            filters.push({ $or: deptClauses });
        }
    }

    const query = (() => {
        if (!filters.length) return {};
        if (filters.length === 1) return filters[0];
        return { $and: filters };
    })();

    const sortOptions = {
        contractDateStart: 1,
        ContractDateStart__c: 1,
        createdDate: 1,
    };

    const docs = await collection
        .find(query)
        .sort(sortOptions)
        .toArray();

    return docs.map(hydrateContract);
}

module.exports = {
    getContracts,
};
