/**
 * @NApiVersion 2.1
 * @NScriptType MapReduceScript
 *
 * ONE script, TWO deployments. Nothing hardcoded, no caps - TOs come from a saved search.
 *
 *   Deployment 1  MODE = FULFILLMENT
 *       Search -> every eligible TO. For each TO, Jazz returns EVERY confirmed shipment
 *       carton for its WMS order (all of them - 900+ is fine). Cartons already fulfilled
 *       are skipped. Every remaining carton gets its OWN Item Fulfillment.
 *       NO CONSOLIDATION, NO LIMIT.
 *
 *   Deployment 2  MODE = RECEIPT
 *       Search -> TOs where the "Create Item Receipt" checkbox is ticked. Every Item
 *       Fulfillment on the TO gets its own Item Receipt. No Jazz call. Checkbox is
 *       unticked in summarize once the TO completes cleanly.
 *
 * WHY THERE IS NO CARTON LIMIT
 *   map writes one key per carton. context.write costs no governance, so 900 writes is
 *   free. Each key then becomes its own reduce invocation with its own 5,000 unit
 *   allowance, and one fulfillment costs about 40. NetSuite yields between keys, so the
 *   work spreads across the queue instead of racing a single governance budget.
 *
 * DUPLICATE PROTECTION - three guards, any one is enough:
 *   1. externalid   AUTO_IF_TO_<toId>_CTN_<carton>  on the fulfillment
 *                   AUTO_IR_TO_<toId>_IF_<ifId>     on the receipt
 *   2. carton number on the package line (Contents Description), read back through the
 *      shipmentPackage join on a re-run
 *   3. tracking number on the package line, read back the same way
 *
 *   GUARD 3 IS CONDITIONAL. If Jazz puts the SAME tracking number on many cartons (one
 *   master tracking for the load), matching on tracking would skip every carton after the
 *   first. The script counts tracking numbers per TO and switches guard 3 off automatically
 *   when tracking is not unique per carton. It logs which mode it chose.
 *
 * #############################################################################
 * # DEPLOYMENT: CONCURRENCY LIMIT MUST BE 1                                   #
 * # Every carton transforms the SAME Transfer Order. Two parallel transforms   #
 * # would both read the same open quantity and over-fulfil the TO.             #
 * #############################################################################
 *
 * Script parameters (per deployment):
 *   custscript_jz_mode          Free-Form Text. "FULFILLMENT" or "RECEIPT".
 *   custscript_jz_search_id     Free-Form Text. Saved search script id or internal id.
 *   custscript_jz_password      PASSWORD type. Jazz password. FULFILLMENT only.
 *   custscript_jz_clear_field   Free-Form Text. Checkbox to untick. RECEIPT only.
 *   custscript_jz_receipt_date  Free-Form Text, optional. e.g. 05/11/2026. Blank = today.
 *
 * Saved search: Type = Transaction, Type = Transfer Order, Main Line = true (or grouped).
 * Result columns MUST include Internal ID and the WMS order number field.
 */
define(['N/https', 'N/record', 'N/log', 'N/search', 'N/runtime', 'N/format'],
function (https, record, log, search, runtime, format) {
    'use strict';

    /* ================= CONFIG ================= */

    var CONFIG = {
        JAZZ_DOMAIN: 'fbflurry.jazz-oms.com',
        JAZZ_USERNAME: 'Dsoni',
        JAZZ_PASSWORD: '',              // leave blank - supply via custscript_jz_password
        JAZZ_TENANT: 'TMJ',
        JAZZ_PAGE_LIMIT: 900,
        JAZZ_MAX_PAGES: 200             // 200 x 900 = 180,000 shipment rows before the guard trips
    };

    var PARAM = {
        MODE: 'custscript_jz_mode',
        SEARCH_ID: 'custscript_jz_search_id',
        JAZZ_PASSWORD: 'custscript_jz_password',
        CLEAR_FIELD: 'custscript_jz_clear_field',
        RECEIPT_DATE: 'custscript_jz_receipt_date'
    };

    var MODE_FULFILLMENT = 'FULFILLMENT';
    var MODE_RECEIPT = 'RECEIPT';

    var KEY_IF = 'IF:';     // reduce key - create a fulfillment for this carton
    var KEY_IR = 'IR:';     // reduce key - create a receipt for this fulfillment
    var KEY_NOTE = 'NOTE:'; // reduce key - nothing to do, pass straight to output

    var LINE_SKU_FIELD = 'custcol_sku_external_id';
    var BODY_WMS_ORDER_NUMBER = 'custbody_wms_order_number';
    var BODY_TRACKING_NUMBER = 'custbody_mtracking';
    var BODY_NO_CARTONS = 'custbody_no_cartons';
    var BODY_TOTAL_QTY_SHIPPED = 'custbody_total_qty_shipped';
    var BODY_JAZZ_SHIPMENT_NUMBER = 'custbody_jazz_shipment_number';

    var _token = null;

    /* ================= DEPLOYMENT CONFIG ================= */

    function getConfig() {
        var s = runtime.getCurrentScript();
        var mode = String(s.getParameter({ name: PARAM.MODE }) || '').trim().toUpperCase();

        if (mode === 'IF' || mode.indexOf('FULFIL') === 0) mode = MODE_FULFILLMENT;
        if (mode === 'IR' || mode.indexOf('RECEI') === 0) mode = MODE_RECEIPT;

        return {
            mode: mode,
            searchId: String(s.getParameter({ name: PARAM.SEARCH_ID }) || '').trim(),
            clearField: String(s.getParameter({ name: PARAM.CLEAR_FIELD }) || '').trim(),
            receiptDate: String(s.getParameter({ name: PARAM.RECEIPT_DATE }) || '').trim(),
            jazzPassword: String(s.getParameter({ name: PARAM.JAZZ_PASSWORD }) || '').trim()
                || CONFIG.JAZZ_PASSWORD,
            deploymentId: s.deploymentId
        };
    }

    function validate(cfg) {
        if (cfg.mode !== MODE_FULFILLMENT && cfg.mode !== MODE_RECEIPT)
            throw new Error('Parameter ' + PARAM.MODE + ' must be FULFILLMENT or RECEIPT. Got: "' + cfg.mode + '"');

        if (!cfg.searchId)
            throw new Error('Parameter ' + PARAM.SEARCH_ID + ' is empty.');

        if (cfg.mode === MODE_FULFILLMENT && !cfg.jazzPassword)
            throw new Error('Jazz password missing. Set ' + PARAM.JAZZ_PASSWORD + ' on this deployment.');
    }

    /* ================= GET INPUT DATA ================= */

    function getInputData() {
        var cfg = getConfig();
        validate(cfg);

        log.audit('START', { mode: cfg.mode, searchId: cfg.searchId, deployment: cfg.deploymentId,
            receiptDate: cfg.receiptDate || 'NetSuite default' });

        return search.load({ id: cfg.searchId });
    }

    /* ================= MAP - fan a TO out into cartons or fulfillments ================= */

    function map(context) {
        var cfg = getConfig();
        var toId = '', wms = '';

        try {
            var res = JSON.parse(context.value);

            // Grouped searches put the id only in GROUP(internalid); result.id is a placeholder.
            toId = extract(res, 'internalid') || String(res.id || '');
            if (toId === '0') toId = '';

            if (!toId) {
                log.error('MAP - NO INTERNAL ID', { row: context.value,
                    hint: 'Add Internal ID as a result column on the saved search.' });
                return;
            }

            wms = extract(res, BODY_WMS_ORDER_NUMBER);

            if (cfg.mode === MODE_FULFILLMENT) fanOutCartons(cfg, context, toId, wms);
            else fanOutFulfillments(context, toId, wms);

        } catch (e) {
            log.error('MAP ERROR', { mode: cfg.mode, toId: toId, wms: wms,
                message: e.message, stack: e.stack });
            context.write({ key: KEY_NOTE + 'ERROR:' + toId, value: JSON.stringify({
                status: 'ERROR', toId: toId, wmsOrderNumber: wms, message: e.message }) });
        }
    }

    /* ---- FULFILLMENT: every Jazz carton minus the ones already fulfilled ---- */

    function fanOutCartons(cfg, context, toId, wms) {
        if (!wms) {
            log.error('NO WMS ORDER NUMBER', { toId: toId });
            return context.write({ key: KEY_NOTE + 'NOWMS:' + toId, value: JSON.stringify({
                status: 'SKIPPED', toId: toId,
                message: 'Skipped - WMS order number blank. Check the saved search result columns.' }) });
        }

        var cartons = getJazzCartons(cfg, wms);          // ALL cartons, no cap
        var existing = getExistingCartons(toId);
        var track = trackingUniqueness(cartons);

        log.audit('TRACKING GUARD', { toId: toId, distinctTracking: track.distinct,
            cartonsWithTracking: track.withTracking, maxCartonsPerTracking: track.maxPerValue,
            useTrackingToMatch: track.unique,
            note: track.unique
                ? 'Tracking is unique per carton - used as a second duplicate guard.'
                : 'Tracking is shared across cartons - NOT used for matching, carton number only.' });

        var queued = 0, byCarton = 0, byTracking = 0, blankCarton = 0;
        var queuedQty = 0, sample = [];

        for (var i = 0; i < cartons.length; i++) {
            var c = cartons[i];

            if (!c.cartonNumber) { blankCarton++; continue; }

            if (existing.cartonKeys[normKey(c.cartonNumber)]) { byCarton++; continue; }

            if (track.unique && c.trackingNumber &&
                existing.trackingKeys[normKey(c.trackingNumber)]) { byTracking++; continue; }

            queued++;
            queuedQty += c.totalQty;
            if (sample.length < 25) sample.push(c.cartonNumber + ' (qty ' + c.totalQty + ')');

            context.write({ key: KEY_IF + toId + '|' + c.cartonNumber,
                value: JSON.stringify({ toId: toId, wmsOrderNumber: wms, carton: c }) });
        }

        log.audit('CARTON MATCH', { toId: toId, wmsOrderNumber: wms,
            jazzCartons: cartons.length, existingFulfillments: existing.ifCount,
            cartonNumbersOnExistingIfs: Object.keys(existing.cartonKeys).length,
            trackingNumbersOnExistingIfs: Object.keys(existing.trackingKeys).length,
            skippedByCartonNumber: byCarton, skippedByTrackingNumber: byTracking,
            blankCartonNumber: blankCarton, queuedForCreation: queued, queuedQty: queuedQty,
            sample: sample });

        if (blankCarton > 0) log.error('JAZZ CARTONS WITH NO CARTON NUMBER', { toId: toId,
            count: blankCarton, note: 'These cannot be matched or de-duplicated and were not queued.' });

        if (!queued) context.write({ key: KEY_NOTE + 'NOCARTON:' + toId, value: JSON.stringify({
            status: 'SKIPPED', toId: toId, wmsOrderNumber: wms,
            message: 'Skipped - all ' + cartons.length + ' Jazz cartons already fulfilled.' }) });
    }

    /** Is each tracking number used by exactly one carton? If not, tracking cannot be a guard. */
    function trackingUniqueness(cartons) {
        var counts = {}, withTracking = 0, maxPerValue = 0, distinct = 0, k;

        for (var i = 0; i < cartons.length; i++) {
            var t = normKey(cartons[i].trackingNumber);
            if (!t) continue;
            withTracking++;
            counts[t] = (counts[t] || 0) + 1;
        }

        for (k in counts) {
            if (!counts.hasOwnProperty(k)) continue;
            distinct++;
            if (counts[k] > maxPerValue) maxPerValue = counts[k];
        }

        return { unique: withTracking > 0 && maxPerValue === 1,
            distinct: distinct, withTracking: withTracking, maxPerValue: maxPerValue };
    }

    /**
     * Carton numbers AND tracking numbers already recorded on this TO's fulfillments.
     * The shipmentPackage tracking column name varies by account, so the search is tried
     * with tracking first and retried without it if the column is rejected.
     */
    function getExistingCartons(toId) {
        var attempts = [
            { tracking: 'trackingnumber', label: 'shipmentPackage.trackingnumber' },
            { tracking: 'packagetrackingnumber', label: 'shipmentPackage.packagetrackingnumber' },
            { tracking: '', label: 'carton number only' }
        ];

        for (var a = 0; a < attempts.length; a++) {
            try {
                var out = runExistingSearch(toId, attempts[a].tracking);
                out.trackingColumn = attempts[a].label;

                log.audit('EXISTING CARTONS ON TO', { toId: toId, packageRows: out.rows,
                    fulfillments: out.ifCount, blankDescriptions: out.blank,
                    distinctCartonNumbers: Object.keys(out.cartonKeys).length,
                    distinctTrackingNumbers: Object.keys(out.trackingKeys).length,
                    trackingColumnUsed: out.trackingColumn,
                    sample: Object.keys(out.cartonKeys).slice(0, 10) });

                if (out.blank > 0) log.audit('FULFILLMENTS WITHOUT A CARTON NUMBER', { toId: toId,
                    count: out.blank, note: 'Cannot be matched by carton number. The externalid ' +
                    'guard still prevents duplicates for anything this script created.' });

                return out;

            } catch (e) {
                log.audit('EXISTING CARTON SEARCH RETRY', { toId: toId,
                    tried: attempts[a].label, message: e.message });
            }
        }

        log.error('EXISTING CARTON SEARCH FAILED', { toId: toId,
            note: 'Falling back to externalid guard only. Duplicates are still prevented for ' +
                  'fulfillments this script created, but not for manually created ones.' });

        return { cartonKeys: {}, trackingKeys: {}, ifCount: 0, rows: 0, blank: 0 };
    }

    function runExistingSearch(toId, trackingField) {
        var cartonKeys = {}, trackingKeys = {}, ifs = {}, rows = 0, blank = 0;

        var columns = [
            search.createColumn({ name: 'internalid' }),
            search.createColumn({ name: 'tranid' }),
            search.createColumn({ name: 'contentsdescription', join: 'shipmentPackage' })
        ];

        if (trackingField) columns.push(search.createColumn({ name: trackingField, join: 'shipmentPackage' }));

        var s = search.create({
            type: 'itemfulfillment',
            filters: [['type', 'anyof', 'ItemShip'], 'AND', ['createdfrom', 'anyof', String(toId)]],
            columns: columns
        });

        var paged = s.runPaged({ pageSize: 1000 });

        paged.pageRanges.forEach(function (range) {
            paged.fetch({ index: range.index }).data.forEach(function (r) {
                rows++;

                var tranid = r.getValue({ name: 'tranid' });
                ifs[r.getValue({ name: 'internalid' })] = true;

                var ck = normKey(r.getValue({ name: 'contentsdescription', join: 'shipmentPackage' }));

                if (!ck) blank++;
                else if (!cartonKeys[ck]) cartonKeys[ck] = tranid;

                if (trackingField) {
                    var tk = normKey(r.getValue({ name: trackingField, join: 'shipmentPackage' }));
                    if (tk && !trackingKeys[tk]) trackingKeys[tk] = tranid;
                }
            });
        });

        return { cartonKeys: cartonKeys, trackingKeys: trackingKeys,
            ifCount: Object.keys(ifs).length, rows: rows, blank: blank };
    }

    /* ---- RECEIPT: every fulfillment on the TO ---- */

    function fanOutFulfillments(context, toId, wms) {
        var out = [], sample = [];

        var s = search.create({
            type: search.Type.ITEM_FULFILLMENT,
            filters: [['type', 'anyof', 'ItemShip'], 'AND', ['mainline', 'is', 'T'], 'AND',
                ['createdfrom', 'anyof', String(toId)]],
            columns: [
                search.createColumn({ name: 'internalid', sort: search.Sort.ASC }),
                search.createColumn({ name: 'tranid' })
            ]
        });

        var paged = s.runPaged({ pageSize: 1000 });

        paged.pageRanges.forEach(function (range) {
            paged.fetch({ index: range.index }).data.forEach(function (r) {
                out.push({ ifId: r.getValue({ name: 'internalid' }), tranid: r.getValue({ name: 'tranid' }) });
            });
        });

        for (var i = 0; i < out.length; i++) {
            if (sample.length < 10) sample.push(out[i].tranid);
            context.write({ key: KEY_IR + out[i].ifId, value: JSON.stringify({
                toId: toId, wmsOrderNumber: wms, fulfillment: out[i] }) });
        }

        log.audit('FULFILLMENTS FOUND', { toId: toId, wmsOrderNumber: wms,
            count: out.length, sample: sample });

        if (!out.length) context.write({ key: KEY_NOTE + 'NOIF:' + toId, value: JSON.stringify({
            status: 'SKIPPED', toId: toId, wmsOrderNumber: wms,
            message: 'Skipped - no Item Fulfillment exists on this Transfer Order.' }) });
    }

    /* ================= REDUCE ================= */

    function reduce(context) {
        var cfg = getConfig();
        var key = String(context.key);
        var row;

        try { row = JSON.parse(context.values[0] || '{}'); } catch (e) { row = {}; }

        // Pass-through notes from map. Without this branch these keys would fall into the
        // create paths below and throw on an undefined carton / fulfillment.
        if (key.indexOf(KEY_NOTE) === 0) {
            log.audit('NOTE ' + (row.status || 'SKIPPED'), row);
            return context.write({ key: row.status || 'SKIPPED', value: JSON.stringify(row) });
        }

        try {
            var outcome = (key.indexOf(KEY_IF) === 0)
                ? createFulfillment(row)
                : createReceipt(cfg, row);

            log.audit(outcome.status, outcome);
            context.write({ key: outcome.status, value: JSON.stringify(outcome) });

        } catch (e) {
            var label = (key.indexOf(KEY_IF) === 0)
                ? ((row.carton || {}).cartonNumber || key)
                : ((row.fulfillment || {}).tranid || key);

            log.error('REDUCE ERROR', { mode: cfg.mode, key: key, subject: label,
                toId: row.toId, errorName: e.name, message: e.message, stack: e.stack });

            context.write({ key: 'ERROR', value: JSON.stringify({ status: 'ERROR', mode: cfg.mode,
                toId: row.toId, wmsOrderNumber: row.wmsOrderNumber, subject: label,
                message: e.message }) });
        }
    }

    /* ================= ONE ITEM FULFILLMENT PER CARTON ================= */

    function createFulfillment(row) {
        var carton = row.carton || {};
        var externalId = ifExternalId(row.toId, carton.cartonNumber);

        var out = { status: 'IF_SKIPPED', success: false, toId: row.toId,
            wmsOrderNumber: row.wmsOrderNumber, carton: carton.cartonNumber,
            trackingNumber: carton.trackingNumber, externalId: externalId,
            itemFulfillmentId: '', lines: 0, qty: 0, unplaced: [], message: '' };

        var dup = findByExternalId('itemfulfillment', externalId);

        if (dup) {
            out.status = 'IF_ALREADY_EXISTS';
            out.itemFulfillmentId = dup.id;
            out.message = 'Skipped - carton already fulfilled on ' + dup.tranid + '.';
            return out;
        }

        var ifRec = record.transform({ fromType: record.Type.TRANSFER_ORDER, fromId: Number(row.toId),
            toType: record.Type.ITEM_FULFILLMENT, isDynamic: false });

        var d = parseDate(carton.shipDate);

        if (d) {
            set(ifRec, 'trandate', d); set(ifRec, 'packeddate', d);
            set(ifRec, 'shippeddate', d); set(ifRec, 'pickeddate', d);
        }

        set(ifRec, 'externalid', externalId);
        set(ifRec, 'shipstatus', 'C');
        set(ifRec, BODY_WMS_ORDER_NUMBER, row.wmsOrderNumber);
        set(ifRec, BODY_TRACKING_NUMBER, carton.trackingNumber);
        set(ifRec, BODY_JAZZ_SHIPMENT_NUMBER, carton.shipmentNumber);
        set(ifRec, BODY_NO_CARTONS, '1');

        /* allocate this carton's quantity against the TO's remaining open lines */
        var want = cartonQtyBySku(carton), left = {}, k;
        for (k in want) if (want.hasOwnProperty(k)) left[k] = want[k];

        var count = ifRec.getLineCount({ sublistId: 'item' });

        for (var i = 0; i < count; i++) setReceive(ifRec, i, false);

        for (var line = 0; line < count; line++) {
            var sku = lineSku(ifRec, line);
            var avail = Number(ifRec.getSublistValue({ sublistId: 'item', fieldId: 'quantity', line: line }) || 0);
            var need = Number(left[sku] || 0);

            if (!sku || avail <= 0 || need <= 0) continue;

            var qty = Math.min(need, avail);

            setReceive(ifRec, line, true);
            ifRec.setSublistValue({ sublistId: 'item', fieldId: 'quantity', line: line, value: qty });

            left[sku] = need - qty;
            out.lines++;
            out.qty += qty;
        }

        for (k in left) if (left.hasOwnProperty(k) && Number(left[k]) > 0) out.unplaced.push(k + ' x' + left[k]);

        set(ifRec, BODY_TOTAL_QTY_SHIPPED, out.qty);

        if (!out.lines) {
            out.message = 'Skipped - the TO has no open quantity for this carton\'s SKUs. ' +
                'Either it is already fulfilled elsewhere or the SKU is not on the TO.';
            return out;
        }

        if (out.unplaced.length) log.audit('CARTON QTY NOT PLACED - ' + carton.cartonNumber,
            { toId: row.toId, unplaced: out.unplaced,
              note: 'Jazz shipped more than the TO has open for these SKUs.' });

        /* package line carries carton number + tracking so a re-run matches */
        try {
            var pkg = ifRec.getLineCount({ sublistId: 'package' });
            for (var p = pkg - 1; p >= 0; p--) ifRec.removeLine({ sublistId: 'package', line: p });

            ifRec.insertLine({ sublistId: 'package', line: 0 });
            setPkg(ifRec, 'packagedescr', 0, carton.cartonNumber);
            setPkg(ifRec, 'packagetrackingnumber', 0, carton.trackingNumber);
            if (carton.weight) setPkg(ifRec, 'packageweight', 0, Number(carton.weight));
        } catch (e) {
            log.error('PACKAGE LINE FAILED - ' + carton.cartonNumber, { toId: row.toId, message: e.message });
        }

        out.itemFulfillmentId = ifRec.save({ enableSourcing: false, ignoreMandatoryFields: true });
        out.status = 'IF_CREATED';
        out.success = true;
        out.message = 'Item Fulfillment created for carton ' + carton.cartonNumber + '.';

        return out;
    }

    /* ================= ONE ITEM RECEIPT PER FULFILLMENT ================= */

    function createReceipt(cfg, row) {
        var f = row.fulfillment || {};
        var externalId = 'AUTO_IR_TO_' + row.toId + '_IF_' + f.ifId;

        var out = { status: 'IR_SKIPPED', success: false, toId: row.toId,
            wmsOrderNumber: row.wmsOrderNumber, itemFulfillmentId: f.ifId,
            itemFulfillmentNo: f.tranid, externalId: externalId,
            itemReceiptId: '', lineCount: 0, message: '' };

        var dup = findByExternalId('itemreceipt', externalId);

        if (dup) {
            out.status = 'IR_ALREADY_EXISTS';
            out.itemReceiptId = dup.id;
            out.message = 'Skipped - receipt ' + dup.tranid + ' already exists for this fulfillment.';
            return out;
        }

        /*
         * itemfulfillment tells NetSuite WHICH fulfillment to receive - the same as clicking
         * Receive beside that IF on Receive Orders. NetSuite sources the receivable lines,
         * so quantity, location and item lines are never set by hand.
         */
        var ir = record.transform({
            fromType: record.Type.TRANSFER_ORDER,
            fromId: Number(row.toId),
            toType: record.Type.ITEM_RECEIPT,
            isDynamic: true,
            defaultValues: { itemfulfillment: Number(f.ifId) }
        });

        out.lineCount = ir.getLineCount({ sublistId: 'item' });

        if (out.lineCount <= 0) {
            out.message = 'Skipped - no receivable lines. This fulfillment is already received ' +
                'or is not eligible to receive.';
            return out;
        }

        set(ir, 'externalid', externalId);
        set(ir, 'memo', 'Auto Item Receipt for TO ' + row.toId + ' from Item Fulfillment ' + f.tranid);
        set(ir, BODY_WMS_ORDER_NUMBER, row.wmsOrderNumber);

        if (cfg.receiptDate) {
            try {
                ir.setValue({ fieldId: 'trandate',
                    value: format.parse({ value: cfg.receiptDate, type: format.Type.DATE }) });
            } catch (e) {
                log.error('RECEIPT DATE INVALID', { value: cfg.receiptDate, message: e.message });
            }
        }

        out.itemReceiptId = ir.save({ enableSourcing: true, ignoreMandatoryFields: false });
        out.status = 'IR_CREATED';
        out.success = true;
        out.message = 'Item Receipt created for fulfillment ' + f.tranid + '.';

        return out;
    }

    function ifExternalId(toId, cartonNumber) {
        return 'AUTO_IF_TO_' + toId + '_CTN_' +
            String(cartonNumber || '').trim().replace(/[^A-Za-z0-9]/g, '').toUpperCase();
    }

    function findByExternalId(recType, externalId) {
        var found = null;

        search.create({
            type: recType,
            filters: [['externalid', 'is', externalId], 'AND', ['mainline', 'is', 'T']],
            columns: [search.createColumn({ name: 'internalid' }), search.createColumn({ name: 'tranid' })]
        }).run().each(function (r) {
            found = { id: r.getValue({ name: 'internalid' }), tranid: r.getValue({ name: 'tranid' }) };
            return false;
        });

        return found;
    }

    /* ================= JAZZ ================= */

    function getJazzCartons(cfg, wms) {
        var token = jazzToken(cfg);
        var all = [], page = 0, next = pageUrl(0);

        while (next && page < CONFIG.JAZZ_MAX_PAGES) {
            page++;

            var resp = https.get({ url: next, headers: { 'Accept': 'application/json',
                'Content-Type': 'application/json', 'Tenant': CONFIG.JAZZ_TENANT,
                'Authorization': 'Token ' + token } });

            if (Number(resp.code) < 200 || Number(resp.code) >= 300)
                throw new Error('Jazz shipment failed: ' + resp.code + ' - ' + resp.body);

            var body = JSON.parse(resp.body || '{}');
            var rows = body.results || (Array.isArray(body) ? body : []);

            for (var i = 0; i < rows.length; i++) all.push(rows[i]);

            if (!rows.length) next = '';
            else if (body.next) next = String(body.next).indexOf('http') === 0
                ? body.next : 'https://' + CONFIG.JAZZ_DOMAIN + body.next;
            else if (body.count && all.length < Number(body.count)) next = pageUrl(all.length);
            else next = '';
        }

        if (page >= CONFIG.JAZZ_MAX_PAGES)
            log.error('JAZZ PAGINATION HIT SAFETY LIMIT', { wms: wms, rowsRead: all.length,
                note: 'Raise CONFIG.JAZZ_MAX_PAGES if this order really has more shipments.' });

        var cartons = toCartons(all);

        log.audit('JAZZ READ', { wmsOrderNumber: wms, shipmentRows: all.length,
            pages: page, cartons: cartons.length });

        return cartons;

        function pageUrl(off) {
            return 'https://' + CONFIG.JAZZ_DOMAIN + '/api/v1/shipment/status?limit=' +
                CONFIG.JAZZ_PAGE_LIMIT + '&offset=' + Number(off || 0) +
                '&order_number=' + encodeURIComponent(wms);
        }
    }

    function jazzToken(cfg) {
        if (_token) return _token;

        var resp = https.post({
            url: 'https://' + CONFIG.JAZZ_DOMAIN + '/api/token/',
            headers: { 'Content-Type': 'application/json', 'Accept': 'application/json' },
            body: JSON.stringify({ username: CONFIG.JAZZ_USERNAME, password: cfg.jazzPassword })
        });

        if (Number(resp.code) < 200 || Number(resp.code) >= 300)
            throw new Error('Jazz token failed: ' + resp.code + ' - ' + resp.body);

        var b = JSON.parse(resp.body || '{}');
        var t = b.token || b.key || b.access_token || b.auth_token;

        if (!t) throw new Error('Jazz token missing from response.');

        _token = t;
        return t;
    }

    function toCartons(shipments) {
        var map = {};

        for (var i = 0; i < shipments.length; i++) {
            var s = shipments[i];
            var status = String(s.status || s.shipment_status || '').toLowerCase();

            if (status !== 'confirmed' && status !== 'shipped' && status !== 'closed') continue;

            var details = s.shipment_detail || s.shipment_details || s.details || [];
            var tracking = String(s.tracking_number || s.tracking_no || '').trim();
            var shipNo = String(s.shipment_number || s.shipment_id || s.id || '').trim();

            for (var d = 0; d < details.length; d++) {
                var det = details[d];
                var sku = normSku(det.sku_code || det.sku || det.item_number || '');
                var qty = Number(det.qty_shipped || det.shipped_qty || 0);

                if (!sku || qty <= 0) continue;

                var cn = String(det.carton_number || det.carton_no || s.carton_number ||
                    shipNo || tracking || '').trim();

                // a carton can carry its own tracking on the detail line
                var cartonTracking = String(det.tracking_number || det.tracking_no || tracking || '').trim();

                if (!map[cn]) map[cn] = { cartonNumber: cn, trackingNumber: cartonTracking,
                    shipmentNumber: shipNo, weight: Number(s.weight || 0),
                    shipDate: s.ship_date || s.shipped_date || s.shipment_date || '',
                    totalQty: 0, items: [] };

                if (!map[cn].trackingNumber && cartonTracking) map[cn].trackingNumber = cartonTracking;

                map[cn].items.push({ sku: sku, qty: qty });
                map[cn].totalQty += qty;
            }
        }

        var out = [];
        for (var k in map) if (map.hasOwnProperty(k)) out.push(map[k]);
        return out;
    }

    /* ================= HELPERS ================= */

    /** Saved search values: string or {value,text}. Summary searches wrap keys: GROUP(internalid). */
    function extract(res, fieldId) {
        var values = res.values || {};
        var target = String(fieldId).toLowerCase();
        var key, raw;

        if (values.hasOwnProperty(fieldId)) raw = values[fieldId];

        if (raw === undefined) {
            for (key in values) {
                if (!values.hasOwnProperty(key)) continue;
                if (unwrap(key) === target) { raw = values[key]; break; }
            }
        }

        if (raw === undefined || raw === null) return '';
        if (Array.isArray(raw)) return raw.length ? String(raw[0].value || raw[0].text || '').trim() : '';
        if (typeof raw === 'object') return String(raw.value || raw.text || '').trim();

        return String(raw).trim();
    }

    /** "GROUP(custbody_wms_order_number)" -> "custbody_wms_order_number" */
    function unwrap(key) {
        key = String(key || '').toLowerCase().trim();
        var open = key.indexOf('('), close = key.lastIndexOf(')');
        if (open > -1 && close > open) key = key.substring(open + 1, close);
        return key.trim();
    }

    function cartonQtyBySku(carton) {
        var out = {}, items = carton.items || [];
        for (var i = 0; i < items.length; i++)
            out[items[i].sku] = Number(out[items[i].sku] || 0) + Number(items[i].qty || 0);
        return out;
    }

    function parseDate(v) {
        if (!v) return null;
        var m = String(v).trim().match(/^(\d{4})-(\d{2})-(\d{2})/);
        if (m) return new Date(Number(m[1]), Number(m[2]) - 1, Number(m[3]));
        var d = new Date(v);
        return isNaN(d.getTime()) ? null : d;
    }

    function lineSku(rec, line) {
        var sku = '';
        try { sku = rec.getSublistValue({ sublistId: 'item', fieldId: LINE_SKU_FIELD, line: line }); } catch (e) {}
        if (!sku) { try { sku = rec.getSublistText({ sublistId: 'item', fieldId: 'item', line: line }); } catch (e2) {} }
        return normSku(sku);
    }

    function setReceive(rec, line, v) {
        try { rec.setSublistValue({ sublistId: 'item', fieldId: 'itemreceive', line: line, value: v }); }
        catch (e) { log.debug('itemreceive set failed', { line: line, message: e.message }); }
    }

    function set(rec, field, v) {
        if (!field || v === null || v === undefined || v === '') return;
        try { rec.setValue({ fieldId: field, value: v }); }
        catch (e) { log.debug('set failed', field + ': ' + e.message); }
    }

    function setPkg(rec, field, line, v) {
        if (v === null || v === undefined || v === '') return;
        try { rec.setSublistValue({ sublistId: 'package', fieldId: field, line: line, value: v }); }
        catch (e) { log.debug('package set failed', field + ': ' + e.message); }
    }

    function normSku(v) {
        return String(v || '').trim().replace(/\s+/g, '').replace(/:/g, '_').toUpperCase();
    }

    /** Loose comparison - spaces, dashes, dots and underscores ignored. */
    function normKey(v) {
        return String(v || '').trim().replace(/[\s\-_.]/g, '').toUpperCase();
    }

    /* ================= SUMMARIZE ================= */

    function summarize(summary) {
        var cfg = getConfig();
        var counts = {}, problems = [], toState = {};

        summary.output.iterator().each(function (k, v) {
            counts[k] = (counts[k] || 0) + 1;

            if (k === 'ERROR' || k === 'IF_SKIPPED' || k === 'IR_SKIPPED') problems.push(v);

            try {
                var row = JSON.parse(v);
                if (row && row.toId) {
                    if (!toState[row.toId]) toState[row.toId] = { ok: true };
                    if (k === 'ERROR') toState[row.toId].ok = false;
                }
            } catch (e) { /* keep going */ }

            return true;
        });

        /* untick the trigger checkbox, but only for TOs where nothing errored */
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
                    log.error('CLEAR CHECKBOX FAILED', { toId: toId, field: cfg.clearField,
                        message: e.message });
                }
            }

            log.audit('CHECKBOX CLEARED', { field: cfg.clearField, cleared: cleared, keptForRetry: kept });
        }

        log.audit('COMPLETED', { mode: cfg.mode, searchId: cfg.searchId,
            transferOrdersTouched: Object.keys(toState).length, counts: counts,
            usage: summary.usage, concurrency: summary.concurrency, yields: summary.yields });

        if (summary.concurrency > 1) log.error('CONCURRENCY IS NOT 1', {
            concurrency: summary.concurrency,
            note: 'Parallel transforms of the same Transfer Order can over-fulfil it. ' +
                  'Set the deployment Concurrency Limit to 1.' });

        if (problems.length) log.error('PROBLEMS - REVIEW', problems.slice(0, 50));

        if (summary.inputSummary.error) log.error('INPUT ERROR', summary.inputSummary.error);

        summary.mapSummary.errors.iterator().each(function (k, e) {
            log.error('MAP STAGE ERROR ' + k, e);
            return true;
        });

        summary.reduceSummary.errors.iterator().each(function (k, e) {
            log.error('REDUCE STAGE ERROR ' + k, e);
            return true;
        });
    }

    return { getInputData: getInputData, map: map, reduce: reduce, summarize: summarize };
});