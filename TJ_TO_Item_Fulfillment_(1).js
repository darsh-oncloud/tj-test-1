/**
 * @NApiVersion 2.1
 * @NScriptType MapReduceScript
 *
 * TO -> 1 consolidated Item Fulfillment + 1 Item Receipt.
 * Jazz shipment/status = what shipped (per carton). Jazz order/status = qty_canceled (one "l").
 * Qty is pooled by SKU across all cartons, then allocated top-down across IF lines.
 * Memo: CANCELLED(JAZZ) | SHORT(NOT SHIPPED) | EXTRA(JAZZ>TO), capped at 999 chars.
 */
define(['N/https', 'N/record', 'N/log', 'N/search'], function (https, record, log, search) {
    'use strict';

    var CONFIG = {
        TO_ID: 142435642, WMS_ORDER_NUMBER: 'TOMFBA143320390',
        JAZZ_DOMAIN: 'fbflurry-uat01.jazz-oms.com', JAZZ_USERNAME: 'dsoni',
        JAZZ_PASSWORD: 'tW4Ffe!EdLWpQfkD', JAZZ_TENANT: 'TMJ',
        JAZZ_PAGE_LIMIT: 250, JAZZ_MAX_PAGES: 100
    };

    var DEBUG_JAZZ_FIELDS = true;

    var LINE_SKU_FIELD = 'custcol_sku_external_id';
    var BODY_WMS_ORDER_NUMBER = 'custbody_wms_order_number';
    var BODY_TRACKING_NUMBER = 'custbody_mtracking';
    var BODY_TOTAL_QTY_SHIPPED = 'custbody_total_qty_shipped';
    var BODY_NO_CARTONS = 'custbody_no_cartons';
    var BODY_JAZZ_SHIPMENT_NUMBER = 'custbody_jazz_shipment_number';
    var MAX_MEMO_LENGTH = 999;

    /* ================= GET INPUT DATA ================= */

    function getInputData() {
        log.audit('INPUT START', { toId: CONFIG.TO_ID, wmsOrderNumber: CONFIG.WMS_ORDER_NUMBER });

        var token = getJazzToken();
        var shipments = getAllJazzShipments(token, CONFIG.WMS_ORDER_NUMBER);
        var jazzShip = summariseJazzShipments(shipments);
        var orders = getAllJazzOrders(token, CONFIG.WMS_ORDER_NUMBER);
        var jazzOrder = summariseJazzOrders(orders);
        var toQtyBySku = getToQtyBySku(CONFIG.TO_ID);

        var row = {
            toId: CONFIG.TO_ID, wmsOrderNumber: CONFIG.WMS_ORDER_NUMBER,
            cartons: jazzShip.cartons, shippedQtyBySku: jazzShip.shippedQtyBySku,
            totalShippedQty: jazzShip.totalShippedQty,
            cancelledQtyBySku: jazzOrder.cancelledQtyBySku, totalCancelledQty: jazzOrder.totalCancelledQty,
            orderedQtyBySku: jazzOrder.orderedQtyBySku, totalOrderedQty: jazzOrder.totalOrderedQty,
            jazzOrderStatus: jazzOrder.statuses.join(','), toQtyBySku: toQtyBySku,
            primaryTrackingNumber: jazzShip.primaryTrackingNumber,
            primaryShipmentNumber: jazzShip.primaryShipmentNumber,
            earliestShipDate: jazzShip.earliestShipDate
        };

        log.audit('INPUT SUMMARY', {
            shipmentsFromJazz: shipments.length, cartonsFound: jazzShip.cartons.length,
            distinctShippedSku: Object.keys(jazzShip.shippedQtyBySku).length,
            totalShippedQty: jazzShip.totalShippedQty, orderRecordsFromJazz: orders.length,
            jazzOrderStatus: row.jazzOrderStatus, totalOrderedQty: jazzOrder.totalOrderedQty,
            distinctCancelSku: Object.keys(jazzOrder.cancelledQtyBySku).length,
            totalCancelledQty: jazzOrder.totalCancelledQty, distinctToSku: Object.keys(toQtyBySku).length
        });

        return [row]; // one row -> one map -> at most one IF
    }

    /* ================= MAP ================= */

    function map(context) {
        var row = JSON.parse(context.value);

        try {
            if (!row.cartons || !row.cartons.length) {
                log.audit('SKIP - NO CARTONS', { wmsOrderNumber: row.wmsOrderNumber });
                return context.write({ key: 'SKIPPED', value: JSON.stringify({
                    reason: 'No eligible cartons found in Jazz for this WMS order.',
                    wmsOrderNumber: row.wmsOrderNumber }) });
            }

            var existing = findExistingItemFulfillment(row.toId, row.wmsOrderNumber);

            if (existing.found) {
                log.audit('SKIP - IF ALREADY EXISTS', existing);
                return context.write({ key: 'DUPLICATE_SKIPPED', value: JSON.stringify({
                    wmsOrderNumber: row.wmsOrderNumber, existingItemFulfillmentId: existing.id,
                    existingTranId: existing.tranid }) });
            }

            var result = createConsolidatedFulfillment(row);
            log.audit('FULFILLMENT RESULT', result);

            if (!result.success) return context.write({ key: 'SKIPPED', value: JSON.stringify(result) });

            var receipt = createConsolidatedItemReceipt(row, result.itemFulfillmentId, result.memo);
            log.audit('ITEM RECEIPT RESULT', receipt);

            result.itemReceiptId = receipt.itemReceiptId;
            result.itemReceiptMessage = receipt.message;

            if (!receipt.success) log.error('ITEM RECEIPT FAILED - IF ALREADY CREATED', {
                wmsOrderNumber: row.wmsOrderNumber, itemFulfillmentId: result.itemFulfillmentId,
                reason: receipt.message });

            context.write({ key: receipt.success ? 'CREATED' : 'CREATED_IF_ONLY_RECEIPT_FAILED',
                value: JSON.stringify(result) });

        } catch (e) {
            log.error('MAP ERROR', { wmsOrderNumber: row.wmsOrderNumber, message: e.message, stack: e.stack });
            context.write({ key: 'ERROR', value: JSON.stringify({
                wmsOrderNumber: row.wmsOrderNumber, message: e.message }) });
        }
    }

    /* ================= ITEM FULFILLMENT ================= */

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

    /* ================= ITEM RECEIPT (TO -> ItemRecpt; IF -> ItemRecpt is unsupported) ================= */

    function createConsolidatedItemReceipt(row, itemFulfillmentId, memo) {
        var result = { success: false, itemReceiptId: '', lineCount: 0, message: '' };

        if (!row || !row.toId) { result.message = 'Skipped - no Transfer Order ID.'; return result; }

        try {
            var irRec = record.transform({ fromType: record.Type.TRANSFER_ORDER, fromId: row.toId,
                toType: record.Type.ITEM_RECEIPT, isDynamic: false });

            result.lineCount = irRec.getLineCount({ sublistId: 'item' });

            if (result.lineCount <= 0) {
                result.message = 'Skipped - transformed Item Receipt has 0 lines. ' +
                    'Nothing outstanding to receive - confirm the IF saved with quantities.';
                log.error('ITEM RECEIPT - NO LINES AFTER TRANSFORM', { toId: row.toId,
                    itemFulfillmentId: itemFulfillmentId, wmsOrderNumber: row.wmsOrderNumber });
                return result;
            }

            trySet(irRec, BODY_WMS_ORDER_NUMBER, row.wmsOrderNumber);
            if (memo) trySet(irRec, 'memo', memo);

            result.itemReceiptId = irRec.save({ enableSourcing: false, ignoreMandatoryFields: true });
            result.success = true;
            result.message = 'Consolidated Item Receipt created from the Transfer Order.';

        } catch (e) {
            log.error('ITEM RECEIPT CREATE FAILED', { toId: row.toId, itemFulfillmentId: itemFulfillmentId,
                wmsOrderNumber: row.wmsOrderNumber, message: e.message, stack: e.stack });
            result.message = 'Error creating Item Receipt: ' + e.message;
        }

        return result;
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

    function getJazzToken() {
        var resp = https.post({
            url: 'https://' + CONFIG.JAZZ_DOMAIN + '/api/token/',
            headers: { 'Content-Type': 'application/json', 'Accept': 'application/json' },
            body: JSON.stringify({ username: CONFIG.JAZZ_USERNAME, password: CONFIG.JAZZ_PASSWORD })
        });

        if (Number(resp.code) < 200 || Number(resp.code) >= 300)
            throw new Error('Jazz token failed: ' + resp.code + ' - ' + resp.body);

        var body = JSON.parse(resp.body || '{}');
        var token = body.token || body.key || body.access_token || body.auth_token;

        if (!token) throw new Error('Jazz token missing from response.');

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

        if (!wmsOrderNumber) return out;

        search.create({
            type: 'itemfulfillment',
            filters: [
                ['type', 'anyof', 'ItemShip'], 'AND', ['mainline', 'is', 'T'], 'AND',
                ['createdfrom', 'anyof', String(toId)], 'AND',
                [BODY_WMS_ORDER_NUMBER, 'is', String(wmsOrderNumber)]
            ],
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
        var counts = { CREATED: 0, CREATED_IF_ONLY_RECEIPT_FAILED: 0, SKIPPED: 0,
            DUPLICATE_SKIPPED: 0, ERROR: 0 };

        summary.output.iterator().each(function (key, value) {
            if (counts.hasOwnProperty(key)) counts[key]++;
            log.audit('OUTPUT ' + key, value);
            return true;
        });

        log.audit('SCRIPT COMPLETED', { usage: summary.usage, concurrency: summary.concurrency,
            yields: summary.yields, counts: counts });

        if (counts.CREATED_IF_ONLY_RECEIPT_FAILED > 0)
            log.error('ATTENTION - IF CREATED WITHOUT A RECEIPT', { count: counts.CREATED_IF_ONLY_RECEIPT_FAILED,
                note: 'See ITEM RECEIPT FAILED entries above. Create the receipt manually.' });

        summary.mapSummary.errors.iterator().each(function (key, error) {
            log.error('MAP SUMMARY ERROR ' + key, error);
            return true;
        });
    }

    return { getInputData: getInputData, map: map, summarize: summarize };
});