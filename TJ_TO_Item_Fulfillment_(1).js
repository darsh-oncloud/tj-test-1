/**
 * @NApiVersion 2.1
 * @NScriptType MapReduceScript
 *
 * ONE script, TWO deployments. No manual TO id / WMS number in code - both come from a saved search.
 *
 *   Deployment 1  MODE = FULFILLMENT
 *       Search returns TOs pending fulfillment. For each row (TO internal id + WMS order number +
 *       document number) it calls Jazz and creates ONE consolidated Item Fulfillment - identical
 *       logic to the original combined script.
 *
 *   Deployment 2  MODE = RECEIPT
 *       Search returns TOs where the "Create Item Receipt" checkbox is ticked.
 *       For each TO it finds EVERY Item Fulfillment that still has outstanding quantity and creates
 *       one Item Receipt per fulfillment. No Jazz call. Checkbox is unticked in summarize.
 *
 *       WHY PER FULFILLMENT: a plain TO -> ItemRecpt transform builds receipt lines per fulfillment.
 *       With 800 fulfillments on one TO that sublist is unusable and the save will time out.
 *       Scoping the transform with defaultValues.itemfulfillment keeps each receipt small, and
 *       because each fulfillment is its own Map/Reduce key the script yields between them.
 *
 * Script parameters (set per deployment):
 *   custscript_jz_mode          Free-Form Text. "FULFILLMENT" or "RECEIPT".
 *   custscript_jz_search_id     Free-Form Text. Saved search script id (customsearch_xxx) or internal id.
 *   custscript_jz_password      Password. Jazz password. FULFILLMENT only.
 *   custscript_jz_clear_field   Free-Form Text. Body checkbox to untick after a clean run. RECEIPT only.
 *
 * Saved search requirements (both):
 *   Type = Transaction. Criteria: Type = Transfer Order AND Main Line = true.
 *   Result columns MUST include the WMS order number field. Document Number is optional (logging only).
 */
define(['N/https', 'N/record', 'N/log', 'N/search', 'N/runtime'],
function (https, record, log, search, runtime) {
    'use strict';

    /* ================= CONFIG ================= */

    var CONFIG = {
        JAZZ_DOMAIN: 'fbflurry-uat01.jazz-oms.com',
        JAZZ_USERNAME: 'dsoni',
        JAZZ_PASSWORD: '',              // leave blank - supply via custscript_jz_password
        JAZZ_TENANT: 'TMJ',
        JAZZ_PAGE_LIMIT: 250,
        JAZZ_MAX_PAGES: 100
    };

    var PARAM = {
        MODE: 'custscript_jz_mode',
        SEARCH_ID: 'custscript_jz_search_id',
        JAZZ_PASSWORD: 'custscript_jz_password',
        CLEAR_FIELD: 'custscript_jz_clear_field'
    };

    var MODE_FULFILLMENT = 'FULFILLMENT';
    var MODE_RECEIPT = 'RECEIPT';

    var NO_IF_PREFIX = 'NOIF-';         // reduce key marker for "TO had nothing to receive"

    var DEBUG_JAZZ_FIELDS = true;

    var LINE_SKU_FIELD = 'custcol_sku_external_id';
    var BODY_WMS_ORDER_NUMBER = 'custbody_wms_order_number';
    var BODY_TRACKING_NUMBER = 'custbody_mtracking';
    var BODY_TOTAL_QTY_SHIPPED = 'custbody_total_qty_shipped';
    var BODY_NO_CARTONS = 'custbody_no_cartons';
    var BODY_JAZZ_SHIPMENT_NUMBER = 'custbody_jazz_shipment_number';
    var MAX_MEMO_LENGTH = 999;

    var _tokenCache = null;

    /* ================= DEPLOYMENT CONFIG ================= */

    function getDeploymentConfig() {
        var script = runtime.getCurrentScript();

        var mode = String(script.getParameter({ name: PARAM.MODE }) || '').trim().toUpperCase();

        if (mode === 'IF' || mode.indexOf('FULFIL') === 0) mode = MODE_FULFILLMENT;
        if (mode === 'IR' || mode.indexOf('RECEI') === 0) mode = MODE_RECEIPT;

        return {
            mode: mode,
            searchId: String(script.getParameter({ name: PARAM.SEARCH_ID }) || '').trim(),
            clearField: String(script.getParameter({ name: PARAM.CLEAR_FIELD }) || '').trim(),
            jazzPassword: String(script.getParameter({ name: PARAM.JAZZ_PASSWORD }) || '').trim()
                || CONFIG.JAZZ_PASSWORD,
            deploymentId: script.deploymentId
        };
    }

    function validateConfig(cfg) {
        if (cfg.mode !== MODE_FULFILLMENT && cfg.mode !== MODE_RECEIPT)
            throw new Error('Parameter ' + PARAM.MODE + ' must be FULFILLMENT or RECEIPT. Got: "' + cfg.mode + '"');

        if (!cfg.searchId)
            throw new Error('Parameter ' + PARAM.SEARCH_ID + ' is empty. Set the saved search id on this deployment.');

        if (cfg.mode === MODE_FULFILLMENT && !cfg.jazzPassword)
            throw new Error('Jazz password missing. Set ' + PARAM.JAZZ_PASSWORD + ' on this deployment.');
    }

    /* ================= GET INPUT DATA ================= */

    function getInputData() {
        var cfg = getDeploymentConfig();
        validateConfig(cfg);

        log.audit('INPUT START', { mode: cfg.mode, searchId: cfg.searchId, deployment: cfg.deploymentId });

        return search.load({ id: cfg.searchId });
    }

    /* ================= MAP ================= */

    function map(context) {
        var cfg = getDeploymentConfig();

        try {
            var res = JSON.parse(context.value);
            var toId = String(res.id || '');

            if (!toId) { log.error('MAP - NO INTERNAL ID ON SEARCH ROW', context.value); return; }

            var wms = extractResultValue(res, BODY_WMS_ORDER_NUMBER);
            var tranid = extractResultValue(res, 'tranid');

            if (cfg.mode === MODE_FULFILLMENT) {
                // one key per TO - duplicate search rows collapse together
                context.write({ key: toId,
                    value: JSON.stringify({ toId: toId, wmsOrderNumber: wms, tranid: tranid }) });
                return;
            }

            // RECEIPT: fan the TO out into one key per outstanding Item Fulfillment
            var fulfillments = findFulfillmentsToReceive(toId);

            log.audit('RECEIPT - FULFILLMENTS FOUND', { toId: toId, tranid: tranid,
                wmsOrderNumber: wms, count: fulfillments.length });

            if (!fulfillments.length) {
                context.write({ key: NO_IF_PREFIX + toId,
                    value: JSON.stringify({ toId: toId, wmsOrderNumber: wms, tranid: tranid }) });
                return;
            }

            for (var i = 0; i < fulfillments.length; i++) {
                context.write({ key: String(fulfillments[i].id), value: JSON.stringify({
                    toId: toId, wmsOrderNumber: wms, tranid: tranid,
                    itemFulfillmentId: fulfillments[i].id, ifTranid: fulfillments[i].tranid }) });
            }

        } catch (e) {
            log.error('MAP ERROR', { mode: cfg.mode, value: context.value,
                message: e.message, stack: e.stack });
        }
    }

    /**
     * Every Item Fulfillment created from this TO that still has stock in transit.
     *
     * NOTE ON THE STATUS FILTER: for Transfer Order fulfillments NetSuite reports "Shipped"
     * (ItemShip:C) while goods are in transit and "Received" once fully received. If your account
     * behaves differently and this returns nothing, delete the status line - the zero-quantity
     * guard in reduce will still stop already-received fulfillments from being processed twice.
     */
    function findFulfillmentsToReceive(toId) {
        var out = [];

        var s = search.create({
            type: 'itemfulfillment',
            filters: [
                ['type', 'anyof', 'ItemShip'], 'AND',
                ['mainline', 'is', 'T'], 'AND',
                ['createdfrom', 'anyof', String(toId)], 'AND',
                ['status', 'anyof', 'ItemShip:C']
            ],
            columns: [
                search.createColumn({ name: 'internalid', sort: search.Sort.ASC }),
                search.createColumn({ name: 'tranid' })
            ]
        });

        var paged = s.runPaged({ pageSize: 1000 });

        paged.pageRanges.forEach(function (range) {
            paged.fetch({ index: range.index }).data.forEach(function (r) {
                out.push({ id: r.getValue({ name: 'internalid' }), tranid: r.getValue({ name: 'tranid' }) });
            });
        });

        return out;
    }

    /** Saved search values arrive as a string or { value, text }. Column keys can carry suffixes. */
    function extractResultValue(res, fieldId) {
        var values = res.values || {};
        var key, raw;

        if (values.hasOwnProperty(fieldId)) raw = values[fieldId];

        if (raw === undefined) {
            for (key in values) {
                if (!values.hasOwnProperty(key)) continue;
                if (String(key).toLowerCase().indexOf(String(fieldId).toLowerCase()) === 0) {
                    raw = values[key];
                    break;
                }
            }
        }

        if (raw === undefined || raw === null) return '';
        if (Array.isArray(raw)) return raw.length ? String(raw[0].text || raw[0].value || '').trim() : '';
        if (typeof raw === 'object') return String(raw.text || raw.value || '').trim();

        return String(raw).trim();
    }

    /* ================= REDUCE ================= */

    function reduce(context) {
        var cfg = getDeploymentConfig();
        var payload = {};

        try { payload = JSON.parse(context.values[0] || '{}'); } catch (e) { payload = {}; }

        try {
            var outcome;

            if (cfg.mode === MODE_FULFILLMENT) {
                outcome = handleFulfillment(cfg, payload);
            } else if (String(context.key).indexOf(NO_IF_PREFIX) === 0) {
                outcome = { status: 'SKIPPED', success: false, mode: MODE_RECEIPT, toId: payload.toId,
                    tranid: payload.tranid, wmsOrderNumber: payload.wmsOrderNumber,
                    message: 'Skipped - no Item Fulfillment on this TO is awaiting receipt.' };
            } else {
                outcome = handleReceipt(payload);
            }

            log.audit(cfg.mode + ' ' + outcome.status, outcome);
            context.write({ key: outcome.status, value: JSON.stringify(outcome) });

        } catch (e) {
            log.error('REDUCE ERROR', { mode: cfg.mode, key: context.key, payload: payload,
                message: e.message, stack: e.stack });
            context.write({ key: 'ERROR', value: JSON.stringify({ mode: cfg.mode, toId: payload.toId,
                tranid: payload.tranid, wmsOrderNumber: payload.wmsOrderNumber,
                itemFulfillmentId: payload.itemFulfillmentId, message: e.message }) });
        }
    }

    /* ================= MODE 1 - FULFILLMENT ================= */

    function handleFulfillment(cfg, payload) {
        var toId = payload.toId;
        var wmsOrderNumber = payload.wmsOrderNumber;

        var outcome = { status: 'SKIPPED', success: false, mode: MODE_FULFILLMENT, toId: toId,
            tranid: payload.tranid, wmsOrderNumber: wmsOrderNumber, message: '' };

        if (!wmsOrderNumber) {
            outcome.message = 'Skipped - WMS order number is blank. Confirm the saved search returns ' +
                BODY_WMS_ORDER_NUMBER + ' as a result column and the TO has it populated.';
            log.error('FULFILLMENT - NO WMS ORDER NUMBER', outcome);
            return outcome;
        }

        var existing = findExistingItemFulfillment(toId, wmsOrderNumber);

        if (existing.found) {
            outcome.status = 'DUPLICATE_SKIPPED';
            outcome.existingItemFulfillmentId = existing.id;
            outcome.existingTranId = existing.tranid;
            outcome.message = 'Skipped - Item Fulfillment already exists for this TO / WMS order.';
            return outcome;
        }

        var row = buildJazzRow(cfg, toId, wmsOrderNumber);

        if (!row.cartons.length) {
            outcome.message = 'Skipped - no eligible (confirmed) cartons found in Jazz for this WMS order.';
            return outcome;
        }

        var result = createConsolidatedFulfillment(row);

        outcome.success = result.success;
        outcome.status = result.success ? 'IF_CREATED' : 'SKIPPED';
        outcome.itemFulfillmentId = result.itemFulfillmentId;
        outcome.fulfilledLineCount = result.fulfilledLineCount;
        outcome.fulfilledQtyTotal = result.fulfilledQtyTotal;
        outcome.packageLinesAdded = result.packageLinesAdded;
        outcome.reconciliation = result.reconciliation;
        outcome.memoTruncated = result.memoTruncated;
        outcome.message = result.message;

        return outcome;
    }

    function buildJazzRow(cfg, toId, wmsOrderNumber) {
        var token = getJazzToken(cfg);

        var shipments = getAllJazzShipments(token, wmsOrderNumber);
        var jazzShip = summariseJazzShipments(shipments);

        var orders = getAllJazzOrders(token, wmsOrderNumber);
        var jazzOrder = summariseJazzOrders(orders);

        var toQtyBySku = getToQtyBySku(toId);

        log.audit('JAZZ SUMMARY', {
            toId: toId, wmsOrderNumber: wmsOrderNumber,
            shipmentsFromJazz: shipments.length, cartonsFound: jazzShip.cartons.length,
            distinctShippedSku: Object.keys(jazzShip.shippedQtyBySku).length,
            totalShippedQty: jazzShip.totalShippedQty, orderRecordsFromJazz: orders.length,
            jazzOrderStatus: jazzOrder.statuses.join(','), totalOrderedQty: jazzOrder.totalOrderedQty,
            totalCancelledQty: jazzOrder.totalCancelledQty, distinctToSku: Object.keys(toQtyBySku).length
        });

        return {
            toId: toId, wmsOrderNumber: wmsOrderNumber,
            cartons: jazzShip.cartons, shippedQtyBySku: jazzShip.shippedQtyBySku,
            totalShippedQty: jazzShip.totalShippedQty,
            cancelledQtyBySku: jazzOrder.cancelledQtyBySku, totalCancelledQty: jazzOrder.totalCancelledQty,
            orderedQtyBySku: jazzOrder.orderedQtyBySku, totalOrderedQty: jazzOrder.totalOrderedQty,
            jazzOrderStatus: jazzOrder.statuses.join(','), toQtyBySku: toQtyBySku,
            primaryTrackingNumber: jazzShip.primaryTrackingNumber,
            primaryShipmentNumber: jazzShip.primaryShipmentNumber,
            earliestShipDate: jazzShip.earliestShipDate
        };
    }

    /* ================= MODE 2 - RECEIPT (one IR per Item Fulfillment) ================= */

    function handleReceipt(payload) {
        var outcome = { status: 'SKIPPED', success: false, mode: MODE_RECEIPT,
            toId: payload.toId, tranid: payload.tranid, wmsOrderNumber: payload.wmsOrderNumber,
            itemFulfillmentId: payload.itemFulfillmentId, ifTranid: payload.ifTranid,
            itemReceiptId: '', lineCount: 0, receivedLineCount: 0, receivedQtyTotal: 0, message: '' };

        var irRec = record.transform({
            fromType: record.Type.TRANSFER_ORDER,
            fromId: payload.toId,
            toType: record.Type.ITEM_RECEIPT,
            isDynamic: false,
            defaultValues: { itemfulfillment: payload.itemFulfillmentId }
        });

        outcome.lineCount = irRec.getLineCount({ sublistId: 'item' });

        if (outcome.lineCount <= 0) {
            outcome.message = 'Skipped - transform returned 0 lines. This fulfillment has already been ' +
                'received or has nothing in transit.';
            return outcome;
        }

        // Count what is actually being received. If everything is zero / unticked, this IF is done.
        for (var line = 0; line < outcome.lineCount; line++) {
            var receiving = irRec.getSublistValue({ sublistId: 'item', fieldId: 'itemreceive', line: line });
            var qty = Number(irRec.getSublistValue({ sublistId: 'item', fieldId: 'quantity', line: line }) || 0);

            if (receiving && qty > 0) { outcome.receivedLineCount++; outcome.receivedQtyTotal += qty; }
        }

        if (outcome.receivedQtyTotal <= 0) {
            outcome.message = 'Skipped - nothing outstanding to receive on this fulfillment.';
            return outcome;
        }

        if (payload.wmsOrderNumber) trySet(irRec, BODY_WMS_ORDER_NUMBER, payload.wmsOrderNumber);

        var memo = getFulfillmentMemo(payload.itemFulfillmentId);
        if (memo) trySet(irRec, 'memo', memo);

        outcome.itemReceiptId = irRec.save({ enableSourcing: false, ignoreMandatoryFields: true });
        outcome.success = true;
        outcome.status = 'IR_CREATED';
        outcome.message = 'Item Receipt created for fulfillment ' + (payload.ifTranid || payload.itemFulfillmentId) + '.';

        return outcome;
    }

    /** Carry the reconciliation memo written on the IF across to its receipt. */
    function getFulfillmentMemo(ifId) {
        try {
            return search.lookupFields({ type: search.Type.ITEM_FULFILLMENT, id: ifId,
                columns: ['memo'] }).memo || '';
        } catch (e) {
            log.debug('memo lookup failed', { ifId: ifId, message: e.message });
            return '';
        }
    }

    /* ================= ITEM FULFILLMENT BUILDER ================= */

    function createConsolidatedFulfillment(row) {
        var result = {
            success: false, toId: row.toId, wmsOrderNumber: row.wmsOrderNumber, itemFulfillmentId: '',
            totalCartons: row.cartons.length, packageLinesAdded: 0, fulfilledLineCount: 0,
            fulfilledQtyTotal: 0, unfulfilledLineCount: 0, memo: '', memoTruncated: false,
            reconciliation: null, message: ''
        };

        var shippedQtyBySku = row.shippedQtyBySku || {};

        if (!Object.keys(shippedQtyBySku).length) {
            result.message = 'Skipped - Jazz reported zero shipped quantity across all cartons.';
            return result;
        }

        var ifRec = record.transform({ fromType: record.Type.TRANSFER_ORDER, fromId: row.toId,
            toType: record.Type.ITEM_FULFILLMENT, isDynamic: false });

        /* header */
        var shipDate = parseJazzDate(row.earliestShipDate);

        if (shipDate) {
            trySet(ifRec, 'trandate', shipDate);
            trySet(ifRec, 'packeddate', shipDate);
            trySet(ifRec, 'shippeddate', shipDate);
            trySet(ifRec, 'pickeddate', shipDate);
        } else {
            log.audit('JAZZ SHIP DATE MISSING/INVALID', {
                wmsOrderNumber: row.wmsOrderNumber, raw: row.earliestShipDate });
        }

        trySet(ifRec, 'shipstatus', 'C');
        trySet(ifRec, BODY_TRACKING_NUMBER, row.primaryTrackingNumber);
        trySet(ifRec, BODY_WMS_ORDER_NUMBER, row.wmsOrderNumber);
        trySet(ifRec, BODY_NO_CARTONS, String(row.cartons.length));
        trySet(ifRec, BODY_JAZZ_SHIPMENT_NUMBER, row.primaryShipmentNumber);

        /* allocate */
        var alloc = allocateQuantities(ifRec, shippedQtyBySku);

        result.fulfilledLineCount = alloc.fulfilledLineCount;
        result.fulfilledQtyTotal = alloc.fulfilledQtyTotal;
        result.unfulfilledLineCount = alloc.unfulfilledLineCount;
        trySet(ifRec, BODY_TOTAL_QTY_SHIPPED, alloc.fulfilledQtyTotal);

        if (alloc.fulfilledLineCount <= 0) {
            result.message = 'Skipped - no TO line matched any Jazz SKU with available quantity.';
            return result;
        }

        /* reconcile + memo */
        var recon = buildReconciliation(row, alloc);
        var memo = buildMemoString(recon);

        result.reconciliation = {
            cancelledCount: recon.cancelled.length, shortCount: recon.short.length,
            extraCount: recon.extra.length, cancelledQty: sumBucket(recon.cancelled),
            shortQty: sumBucket(recon.short), extraQty: sumBucket(recon.extra)
        };
        result.memo = memo.text;
        result.memoTruncated = memo.truncated;

        log.audit('RECONCILIATION DETAIL', {
            wmsOrderNumber: row.wmsOrderNumber, jazzTotalShipped: row.totalShippedQty,
            jazzTotalCancelled: row.totalCancelledQty, jazzTotalOrdered: row.totalOrderedQty,
            netsuiteFulfilled: alloc.fulfilledQtyTotal, cancelledInJazz: recon.cancelled,
            shortNotShipped: recon.short, extraJazzOverTo: recon.extra
        });

        if (memo.text) trySet(ifRec, 'memo', memo.text);

        /* packages - every carton, no exclusions */
        clearPackageLines(ifRec);

        for (var c = 0; c < row.cartons.length; c++) {
            if (addOnePackageLine(ifRec, result.packageLinesAdded, row.cartons[c])) result.packageLinesAdded++;
        }

        log.audit('PACKAGE LINES ADDED', { wmsOrderNumber: row.wmsOrderNumber,
            cartonsFound: row.cartons.length, packageLinesAdded: result.packageLinesAdded });

        result.itemFulfillmentId = ifRec.save({ enableSourcing: false, ignoreMandatoryFields: true });
        result.success = true;
        result.message = 'Consolidated Item Fulfillment created for all cartons.';

        return result;
    }

    /** Uncheck every line, then walk lines in order allocating pooled Jazz qty. */
    function allocateQuantities(ifRec, shippedQtyBySku) {
        var out = {
            fulfilledLineCount: 0, fulfilledQtyTotal: 0, unfulfilledLineCount: 0,
            remainingJazzBySku: copyObj(shippedQtyBySku), fulfilledBySku: {},
            availableBySku: {}, skusOnIf: {}
        };

        var lineCount = ifRec.getLineCount({ sublistId: 'item' });

        for (var i = 0; i < lineCount; i++) setReceive(ifRec, i, false);

        for (var line = 0; line < lineCount; line++) {
            var sku = getLineSku(ifRec, line);
            var available = getLineAvailableQty(ifRec, line);

            if (sku) { out.skusOnIf[sku] = true; addQty(out.availableBySku, sku, available); }

            var jazzLeft = Number(out.remainingJazzBySku[sku] || 0);

            if (!sku || available <= 0 || jazzLeft <= 0) { out.unfulfilledLineCount++; continue; }

            var qty = Math.min(jazzLeft, available);

            setReceive(ifRec, line, true);
            ifRec.setSublistValue({ sublistId: 'item', fieldId: 'quantity', line: line, value: qty });

            out.remainingJazzBySku[sku] = jazzLeft - qty;
            addQty(out.fulfilledBySku, sku, qty);
            out.fulfilledLineCount++;
            out.fulfilledQtyTotal += qty;

            if (qty < available) out.unfulfilledLineCount++; // partially fulfilled
        }

        return out;
    }

    /** cancelled = Jazz qty_canceled | short = on IF but not shipped | extra = Jazz > TO */
    function buildReconciliation(row, alloc) {
        var cancelled = [], short = [], extra = [], sku;
        var cancelledQtyBySku = row.cancelledQtyBySku || {};
        var toQtyBySku = row.toQtyBySku || {};

        for (sku in cancelledQtyBySku) {
            if (!cancelledQtyBySku.hasOwnProperty(sku)) continue;
            var cQty = Number(cancelledQtyBySku[sku] || 0);
            if (cQty > 0) cancelled.push({ sku: sku, qty: cQty,
                orderedInJazz: Number((row.orderedQtyBySku || {})[sku] || 0),
                shippedInJazz: Number((row.shippedQtyBySku || {})[sku] || 0) });
        }

        for (sku in alloc.availableBySku) {
            if (!alloc.availableBySku.hasOwnProperty(sku)) continue;
            var avail = Number(alloc.availableBySku[sku] || 0);
            var done = Number(alloc.fulfilledBySku[sku] || 0);
            if (avail - done > 0) short.push({ sku: sku, qty: avail - done, availableOnIf: avail,
                fulfilled: done, orderedOnTo: Number((toQtyBySku[sku] || {}).qty || 0),
                cancelledInJazz: Number(cancelledQtyBySku[sku] || 0) });
        }

        for (sku in alloc.remainingJazzBySku) {
            if (!alloc.remainingJazzBySku.hasOwnProperty(sku)) continue;
            var left = Number(alloc.remainingJazzBySku[sku] || 0);
            if (left <= 0) continue;
            extra.push({ sku: sku, qty: left, notOnTo: !alloc.skusOnIf[sku],
                jazzShipped: Number((row.shippedQtyBySku || {})[sku] || 0) });
        }

        cancelled.sort(byQtyDesc); short.sort(byQtyDesc); extra.sort(byQtyDesc);

        return { cancelled: cancelled, short: short, extra: extra };
    }

    function buildMemoString(recon) {
        var sections = [];

        if (recon.cancelled.length) sections.push({ label: 'CANCELLED(JAZZ)', rows: recon.cancelled, tagNotOnTo: false });
        if (recon.short.length) sections.push({ label: 'SHORT(NOT SHIPPED)', rows: recon.short, tagNotOnTo: false });
        if (recon.extra.length) sections.push({ label: 'EXTRA(JAZZ>TO)', rows: recon.extra, tagNotOnTo: true });

        if (!sections.length) return { text: '', truncated: false };

        var text = '', truncated = false;

        for (var s = 0; s < sections.length; s++) {
            var sec = sections[s];
            var head = (text ? ' || ' : '') + sec.label + ': ';

            if (text.length + head.length >= MAX_MEMO_LENGTH) { truncated = true; break; }

            text += head;
            var written = 0;

            for (var r = 0; r < sec.rows.length; r++) {
                var item = sec.rows[r];
                var piece = (written ? ', ' : '') + item.sku + ' x' + item.qty +
                    (sec.tagNotOnTo && item.notOnTo ? '[NOT-ON-TO]' : '');
                var overflowNote = ' +' + (sec.rows.length - r) + ' more';

                if (text.length + piece.length + overflowNote.length > MAX_MEMO_LENGTH) {
                    text += overflowNote; truncated = true; break;
                }

                text += piece;
                written++;
            }
        }

        if (text.length > MAX_MEMO_LENGTH) { text = text.substring(0, MAX_MEMO_LENGTH); truncated = true; }

        return { text: text, truncated: truncated };
    }

    /* ================= TO QUANTITY SEARCH ================= */

    function getToQtyBySku(toId) {
        try {
            return runToSearch(toId, true);
        } catch (e) {
            log.audit('TO SEARCH - SKU GROUPING FAILED, FALLING BACK TO ITEM', { toId: toId, message: e.message });
            try {
                return runToSearch(toId, false);
            } catch (e2) {
                log.error('TO SEARCH FAILED', { toId: toId, message: e2.message });
                return {};
            }
        }
    }

    function runToSearch(toId, groupBySku) {
        var columns = [
            search.createColumn({ name: 'item', summary: search.Summary.GROUP }),
            search.createColumn({ name: 'formulanumeric', summary: search.Summary.SUM,
                formula: 'ABS(NVL({quantity},0))' })
        ];

        if (groupBySku) columns.splice(1, 0,
            search.createColumn({ name: LINE_SKU_FIELD, summary: search.Summary.GROUP }));

        var s = search.create({
            type: 'transferorder',
            settings: [{ name: 'consolidationtype', value: 'ACCTTYPE' }],
            filters: [
                ['type', 'anyof', 'TrnfrOrd'], 'AND', ['internalidnumber', 'equalto', String(toId)], 'AND',
                ['mainline', 'is', 'F'], 'AND', ['cogs', 'is', 'F'], 'AND', ['shipping', 'is', 'F'], 'AND',
                ['taxline', 'is', 'F'], 'AND', ['transactionlinetype', 'anyof', 'ITEM']
            ],
            columns: columns
        });

        var out = {};
        var paged = s.runPaged({ pageSize: 1000 });

        paged.pageRanges.forEach(function (range) {
            paged.fetch({ index: range.index }).data.forEach(function (r) {
                var itemName = r.getText({ name: 'item', summary: search.Summary.GROUP }) ||
                               r.getValue({ name: 'item', summary: search.Summary.GROUP });
                var rawSku = groupBySku
                    ? r.getValue({ name: LINE_SKU_FIELD, summary: search.Summary.GROUP })
                    : itemName;
                var sku = normalizeSku(rawSku || itemName);

                if (!sku) return;

                var qty = Number(r.getValue({ name: 'formulanumeric', summary: search.Summary.SUM }) || 0);

                if (!out[sku]) out[sku] = { sku: sku, itemName: itemName, qty: 0 };
                out[sku].qty += qty;
            });
        });

        return out;
    }

    /* ================= JAZZ API ================= */

    function getJazzToken(cfg) {
        if (_tokenCache) return _tokenCache;

        var resp = https.post({
            url: 'https://' + CONFIG.JAZZ_DOMAIN + '/api/token/',
            headers: { 'Content-Type': 'application/json', 'Accept': 'application/json' },
            body: JSON.stringify({ username: CONFIG.JAZZ_USERNAME, password: cfg.jazzPassword })
        });

        if (Number(resp.code) < 200 || Number(resp.code) >= 300)
            throw new Error('Jazz token failed: ' + resp.code + ' - ' + resp.body);

        var body = JSON.parse(resp.body || '{}');
        var token = body.token || body.key || body.access_token || body.auth_token;

        if (!token) throw new Error('Jazz token missing from response.');

        _tokenCache = token;
        return token;
    }

    /** Shared paginated GET. Jazz wraps list endpoints in { count, next, previous, results }. */
    function jazzGetAllPages(token, urlBuilder, label) {
        var all = [], offset = 0, page = 0, nextUrl = urlBuilder(0);

        while (nextUrl && page < CONFIG.JAZZ_MAX_PAGES) {
            page++;

            var resp = https.get({ url: nextUrl, headers: {
                'Accept': 'application/json', 'Content-Type': 'application/json',
                'Tenant': CONFIG.JAZZ_TENANT, 'Authorization': 'Token ' + token } });

            if (Number(resp.code) < 200 || Number(resp.code) >= 300)
                throw new Error('Jazz ' + label + ' failed: ' + resp.code + ' - ' + resp.body);

            var body = JSON.parse(resp.body || '{}');
            var rows = getRowsFromJazzBody(body);

            for (var i = 0; i < rows.length; i++) all.push(rows[i]);

            log.audit('JAZZ PAGE READ - ' + label, { page: page, rowsThisPage: rows.length,
                totalSoFar: all.length, reportedCount: body.count });

            if (!rows.length) nextUrl = '';
            else if (body.next) nextUrl = normalizeJazzNextUrl(body.next);
            else if (body.count && all.length < Number(body.count)) { offset = all.length; nextUrl = urlBuilder(offset); }
            else nextUrl = '';
        }

        if (page >= CONFIG.JAZZ_MAX_PAGES)
            log.error('JAZZ PAGINATION HIT SAFETY LIMIT - ' + label, { totalRows: all.length });

        return all;
    }

    function getAllJazzShipments(token, orderNumber) {
        return jazzGetAllPages(token, function (offset) {
            return 'https://' + CONFIG.JAZZ_DOMAIN + '/api/v1/shipment/status?limit=' + CONFIG.JAZZ_PAGE_LIMIT +
                '&offset=' + Number(offset || 0) + '&order_number=' + encodeURIComponent(orderNumber);
        }, 'SHIPMENT');
    }

    /**
     * detail_set: line_number, sku_code, unit_cost, unit_price, unit_discount, qty_ordered,
     * qty_canceled, qty_allocated, qty_backordered, qty_printed, qty_new, qty_shipped, qty_returned.
     */
    function getAllJazzOrders(token, orderNumber) {
        return jazzGetAllPages(token, function (offset) {
            return 'https://' + CONFIG.JAZZ_DOMAIN + '/api/v1/order/status?limit=' + CONFIG.JAZZ_PAGE_LIMIT +
                '&offset=' + Number(offset || 0) + '&order_number=' + encodeURIComponent(orderNumber);
        }, 'ORDER');
    }

    function summariseJazzOrders(orders) {
        var cancelledQtyBySku = {}, orderedQtyBySku = {}, statusMap = {}, cancelLines = [];
        var totalCancelledQty = 0, totalOrderedQty = 0;

        for (var i = 0; i < orders.length; i++) {
            var order = orders[i];

            if (order.status) statusMap[String(order.status)] = true;

            var details = order.detail_set || order.details || order.detail || [];

            for (var d = 0; d < details.length; d++) {
                var detail = details[d];
                var sku = normalizeSku(getDetailSku(detail));

                if (!sku) continue;

                var ordered = firstNumber([detail.qty_ordered, detail.ordered_qty, detail.quantity_ordered]);

                if (ordered > 0) { addQty(orderedQtyBySku, sku, ordered); totalOrderedQty += ordered; }

                var cancelled = getDetailCancelledQty(detail);

                if (cancelled > 0) {
                    addQty(cancelledQtyBySku, sku, cancelled);
                    totalCancelledQty += cancelled;
                    cancelLines.push({ lineNumber: detail.line_number, sku: sku, qtyCancelled: cancelled,
                        qtyOrdered: ordered, qtyShipped: firstNumber([detail.qty_shipped]) });
                }
            }
        }

        log.audit('JAZZ ORDER CANCELS', { totalCancelledQty: totalCancelledQty,
            distinctCancelledSkus: Object.keys(cancelledQtyBySku).length,
            lines: cancelLines.length ? cancelLines : 'NONE' });

        if (DEBUG_JAZZ_FIELDS && orders.length) {
            var fd = orders[0].detail_set || orders[0].details || orders[0].detail || [];
            log.audit('JAZZ ORDER FIELD DIAGNOSTIC', {
                orderFields: Object.keys(orders[0]).sort(),
                detailFields: fd.length ? Object.keys(fd[0]).sort() : 'NO DETAIL LINES',
                sampleDetail: fd.length ? fd[0] : null, orderStatuses: Object.keys(statusMap) });
        }

        return { cancelledQtyBySku: cancelledQtyBySku, orderedQtyBySku: orderedQtyBySku,
            totalCancelledQty: totalCancelledQty, totalOrderedQty: totalOrderedQty,
            statuses: Object.keys(statusMap) };
    }

    /** Jazz uses qty_canceled (one "l"). Rest are defensive aliases. */
    function getDetailCancelledQty(detail) {
        return firstNumber([detail.qty_canceled, detail.qty_cancelled, detail.canceled_qty,
            detail.cancelled_qty, detail.quantity_canceled, detail.quantity_cancelled, detail.cancel_qty]);
    }

    function normalizeJazzNextUrl(nextValue) {
        if (!nextValue) return '';
        nextValue = String(nextValue);
        if (nextValue.indexOf('http') === 0) return nextValue;
        if (nextValue.indexOf('/') === 0) return 'https://' + CONFIG.JAZZ_DOMAIN + nextValue;
        return 'https://' + CONFIG.JAZZ_DOMAIN + '/' + nextValue;
    }

    function getRowsFromJazzBody(body) {
        if (!body) return [];
        if (Array.isArray(body)) return body;
        if (Array.isArray(body.results)) return body.results;
        if (Array.isArray(body.shipments)) return body.shipments;
        if (Array.isArray(body.data)) return body.data;
        return [];
    }

    /* ================= JAZZ SHIPMENT SUMMARISATION ================= */

    function summariseJazzShipments(shipments) {
        var cartonMap = {}, shippedQtyBySku = {}, totalShippedQty = 0, skippedShipments = 0;
        var primaryTrackingNumber = '', primaryShipmentNumber = '';
        var earliestShipDate = '', earliestShipDateMs = null;

        for (var i = 0; i < shipments.length; i++) {
            var shipment = shipments[i];
            var details = shipment.shipment_detail || shipment.shipment_details || shipment.details || [];

            if (!details.length) continue;
            if (!isEligibleShipment(shipment)) { skippedShipments++; continue; }

            var shipmentNumber = getShipmentNumber(shipment);
            var trackingNumber = getTrackingNumber(shipment);
            var shipmentCartonNo = getCartonNumber(shipment);
            var shipmentDate = getShipmentDate(shipment);
            var weight = getShipmentWeight(shipment);

            if (!primaryTrackingNumber && trackingNumber) primaryTrackingNumber = trackingNumber;
            if (!primaryShipmentNumber && shipmentNumber) primaryShipmentNumber = shipmentNumber;

            for (var d = 0; d < details.length; d++) {
                var detail = details[d];
                var sku = normalizeSku(getDetailSku(detail));

                if (!sku) continue;

                var qty = getDetailShippedQty(detail);

                if (qty <= 0) continue;

                var cartonNumber = String(getCartonNumber(detail) || shipmentCartonNo ||
                    shipmentNumber || trackingNumber || '').trim();
                var finalShipDate = getShipmentDate(detail) || shipmentDate;

                if (!cartonMap[cartonNumber]) {
                    cartonMap[cartonNumber] = { cartonNumber: cartonNumber, trackingNumber: trackingNumber,
                        weight: weight, shipDate: finalShipDate, totalQty: 0 };
                } else {
                    var carton = cartonMap[cartonNumber];
                    if (!carton.trackingNumber && trackingNumber) carton.trackingNumber = trackingNumber;
                    if (!carton.weight && weight) carton.weight = weight;
                    if (!carton.shipDate && finalShipDate) carton.shipDate = finalShipDate;
                }

                cartonMap[cartonNumber].totalQty += qty;
                addQty(shippedQtyBySku, sku, qty);
                totalShippedQty += qty;

                if (finalShipDate) {
                    var parsed = parseJazzDate(finalShipDate);
                    if (parsed) {
                        var ms = parsed.getTime();
                        if (earliestShipDateMs === null || ms < earliestShipDateMs) {
                            earliestShipDateMs = ms; earliestShipDate = finalShipDate;
                        }
                    }
                }
            }
        }

        if (skippedShipments > 0) log.audit('SHIPMENTS SKIPPED - NOT CONFIRMED', { count: skippedShipments });

        var cartons = [];
        for (var key in cartonMap) if (cartonMap.hasOwnProperty(key)) cartons.push(cartonMap[key]);

        return { cartons: cartons, shippedQtyBySku: shippedQtyBySku, totalShippedQty: totalShippedQty,
            primaryTrackingNumber: primaryTrackingNumber, primaryShipmentNumber: primaryShipmentNumber,
            earliestShipDate: earliestShipDate };
    }

    // Docs: shipment status is "confirmed" or "unconfirmed". shipped/closed kept as legacy aliases.
    function isEligibleShipment(shipment) {
        var status = String(shipment.status || shipment.shipment_status || '').toLowerCase();
        return status === 'confirmed' || status === 'shipped' || status === 'closed';
    }

    function getDetailSku(detail) {
        return detail.sku_code || detail.sku || detail.item_sku || detail.item_number || detail.ItemNumber || '';
    }

    function getDetailShippedQty(detail) {
        return firstNumber([detail.qty_shipped, detail.shipped_qty, detail.quantity_shipped, detail.fulfilled_qty]);
    }

    function getShipmentNumber(shipment) {
        return String(shipment.shipment_number || shipment.shipment_no ||
            shipment.shipment_id || shipment.id || '').trim();
    }

    function getTrackingNumber(shipment) {
        return String(shipment.tracking_number || shipment.tracking_no ||
            shipment.pro_number || shipment.master_tracking_number || '').trim();
    }

    function getCartonNumber(obj) {
        if (!obj) return '';
        return String(obj.carton_number || obj.carton_no || obj.carton || obj.carton_id ||
            obj.package_number || obj.package_no || obj.package_id || obj.box_number ||
            obj.box_no || obj.container_number || '').trim();
    }

    function getShipmentWeight(shipment) {
        return firstNumber([shipment.weight, shipment.shipment_weight, shipment.package_weight]);
    }

    function getShipmentDate(obj) {
        if (!obj) return '';
        return obj.ship_date || obj.shipped_date || obj.shipment_date || obj.date_shipped ||
            obj.shipped_at || obj.shipment_date_time || obj.created_at || '';
    }

    function parseJazzDate(value) {
        if (!value) return null;

        value = String(value).trim();

        var m = value.match(/^(\d{4})-(\d{2})-(\d{2})/);
        if (m) return new Date(Number(m[1]), Number(m[2]) - 1, Number(m[3]));

        var d = new Date(value);
        if (isNaN(d.getTime())) { log.debug('Invalid Jazz date', value); return null; }

        return d;
    }

    /* ================= DUPLICATE CHECK ================= */

    function findExistingItemFulfillment(toId, wmsOrderNumber) {
        var out = { found: false, id: '', tranid: '' };

        var filters = [
            ['type', 'anyof', 'ItemShip'], 'AND', ['mainline', 'is', 'T'], 'AND',
            ['createdfrom', 'anyof', String(toId)]
        ];

        if (wmsOrderNumber) filters.push('AND', [BODY_WMS_ORDER_NUMBER, 'is', String(wmsOrderNumber)]);

        search.create({
            type: 'itemfulfillment',
            filters: filters,
            columns: [search.createColumn({ name: 'internalid' }), search.createColumn({ name: 'tranid' })]
        }).run().each(function (r) {
            out.found = true;
            out.id = r.getValue({ name: 'internalid' });
            out.tranid = r.getValue({ name: 'tranid' });
            return false;
        });

        return out;
    }

    /* ================= LINE + PACKAGE HELPERS ================= */

    function getLineSku(ifRec, line) {
        var sku = '';

        try { sku = ifRec.getSublistValue({ sublistId: 'item', fieldId: LINE_SKU_FIELD, line: line }); }
        catch (e) { /* custom column absent */ }

        if (!sku) {
            try { sku = ifRec.getSublistText({ sublistId: 'item', fieldId: 'item', line: line }); }
            catch (e2) { /* ignore */ }
        }

        return normalizeSku(sku);
    }

    function getLineAvailableQty(ifRec, line) {
        return Number(ifRec.getSublistValue({ sublistId: 'item', fieldId: 'quantity', line: line }) || 0);
    }

    function setReceive(ifRec, line, value) {
        try {
            ifRec.setSublistValue({ sublistId: 'item', fieldId: 'itemreceive', line: line, value: value });
        } catch (e) {
            log.debug('itemreceive set failed', { line: line, value: value, error: e.message });
        }
    }

    function clearPackageLines(ifRec) {
        try {
            var count = ifRec.getLineCount({ sublistId: 'package' });
            for (var i = count - 1; i >= 0; i--) ifRec.removeLine({ sublistId: 'package', line: i });
        } catch (e) {
            log.debug('clearPackageLines failed', e.message);
        }
    }

    function addOnePackageLine(ifRec, lineIndex, carton) {
        try {
            ifRec.insertLine({ sublistId: 'package', line: lineIndex });
            trySetSub(ifRec, 'package', 'packagetrackingnumber', lineIndex, carton.trackingNumber);
            trySetSub(ifRec, 'package', 'packagedescr', lineIndex, carton.cartonNumber);
            if (carton.weight) trySetSub(ifRec, 'package', 'packageweight', lineIndex, Number(carton.weight));
            return true;
        } catch (e) {
            log.error('PACKAGE LINE FAILED', { lineIndex: lineIndex, cartonNumber: carton.cartonNumber,
                trackingNumber: carton.trackingNumber, error: e.message });
            return false;
        }
    }

    /* ================= COMMON HELPERS ================= */

    function trySet(rec, fieldId, value) {
        if (!fieldId || value === null || value === undefined || value === '') return;
        try { rec.setValue({ fieldId: fieldId, value: value }); }
        catch (e) { log.debug('header set failed', { fieldId: fieldId, error: e.message }); }
    }

    function trySetSub(rec, sublistId, fieldId, line, value) {
        if (!fieldId || value === null || value === undefined || value === '') return;
        try { rec.setSublistValue({ sublistId: sublistId, fieldId: fieldId, line: line, value: value }); }
        catch (e) { log.debug('sublist set failed', { sublistId: sublistId, fieldId: fieldId,
            line: line, error: e.message }); }
    }

    function normalizeSku(value) {
        value = String(value || '').trim().replace(/\s+/g, '');
        if (value.indexOf(':') !== -1) value = value.replace(/:/g, '_');
        return value.toUpperCase();
    }

    function addQty(map, key, qty) {
        if (!key) return;
        map[key] = Number(map[key] || 0) + Number(qty || 0);
    }

    function firstNumber(candidates) {
        for (var i = 0; i < candidates.length; i++) {
            var v = candidates[i];
            if (v !== null && v !== undefined && v !== '') {
                var n = Number(v);
                if (!isNaN(n)) return n;
            }
        }
        return 0;
    }

    function copyObj(obj) {
        var out = {};
        for (var k in obj) if (obj.hasOwnProperty(k)) out[k] = obj[k];
        return out;
    }

    function sumBucket(rows) {
        var total = 0;
        for (var i = 0; i < rows.length; i++) total += Number(rows[i].qty || 0);
        return total;
    }

    function byQtyDesc(a, b) { return Number(b.qty || 0) - Number(a.qty || 0); }

    /* ================= SUMMARIZE ================= */

    function summarize(summary) {
        var cfg = getDeploymentConfig();
        var counts = { IF_CREATED: 0, IR_CREATED: 0, SKIPPED: 0, DUPLICATE_SKIPPED: 0, ERROR: 0 };
        var toState = {};   // toId -> { ok: bool }

        summary.output.iterator().each(function (key, value) {
            if (counts.hasOwnProperty(key)) counts[key]++;

            log.audit('OUTPUT ' + key, value);

            try {
                var row = JSON.parse(value);
                if (row && row.toId) {
                    if (!toState[row.toId]) toState[row.toId] = { ok: true };
                    if (key === 'ERROR') toState[row.toId].ok = false;
                }
            } catch (e) { /* keep going */ }

            return true;
        });

        // Untick the trigger checkbox, but only for TOs where nothing errored.
        // A TO that half-failed keeps its checkbox so the next run picks up the remainder.
        if (cfg.mode === MODE_RECEIPT && cfg.clearField) {
            var cleared = 0, kept = 0;

            for (var toId in toState) {
                if (!toState.hasOwnProperty(toId)) continue;

                if (!toState[toId].ok) { kept++; continue; }

                try {
                    var values = {};
                    values[cfg.clearField] = false;
                    record.submitFields({ type: record.Type.TRANSFER_ORDER, id: toId, values: values,
                        options: { enableSourcing: false, ignoreMandatoryFields: true } });
                    cleared++;
                } catch (e) {
                    log.error('CLEAR CHECKBOX FAILED', { toId: toId, field: cfg.clearField, message: e.message });
                }
            }

            log.audit('CHECKBOX CLEARED', { cleared: cleared, keptForRetry: kept, field: cfg.clearField });
        }

        log.audit('SCRIPT COMPLETED', { mode: cfg.mode, searchId: cfg.searchId,
            distinctTransferOrders: Object.keys(toState).length, usage: summary.usage,
            concurrency: summary.concurrency, yields: summary.yields, counts: counts });

        if (counts.ERROR > 0)
            log.error('ATTENTION - ERRORS DURING RUN', { mode: cfg.mode, count: counts.ERROR,
                note: 'See REDUCE ERROR entries above. Affected TOs kept their checkbox for retry.' });

        summary.mapSummary.errors.iterator().each(function (key, error) {
            log.error('MAP SUMMARY ERROR ' + key, error);
            return true;
        });

        summary.reduceSummary.errors.iterator().each(function (key, error) {
            log.error('REDUCE SUMMARY ERROR ' + key, error);
            return true;
        });
    }

    return { getInputData: getInputData, map: map, reduce: reduce, summarize: summarize };
});