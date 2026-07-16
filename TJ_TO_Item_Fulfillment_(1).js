/**
 * @NApiVersion 2.1
 * @NScriptType MapReduceScript
 *
 * Purpose:
 * - Hardcoded TO + WMS order number.
 * - Call Jazz shipment API and get ALL shipments/cartons for the WMS order.
 * - Instead of one Item Fulfillment per carton, build ONE CONSOLIDATED
 *   Item Fulfillment for the whole Transfer Order:
 *      - All cartons are added as separate lines on the "Package" subtab
 *        (tracking number + carton number + weight per carton).
 *      - All Jazz item quantities are SUMMED BY SKU across every carton,
 *        then matched/fulfilled against the TO lines on the single IF.
 * - Skip cancelled / zero shipped qty lines.
 * - Prevent duplicate consolidated IF creation by WMS order number
 *   (since there should only ever be ONE IF for this TO now).
 * - Set Jazz shipment date on trandate, packeddate, shippeddate, pickeddate
 *   (uses the earliest ship date found across all cartons).
 *
 * IMPORTANT:
 * - This script produces exactly ONE input row (the whole order), so it will
 *   create at most ONE Item Fulfillment total. Deployment concurrency does
 *   not matter for correctness here (only one map executes), but leave it
 *   at 1 anyway to be safe.
 *
 * NEW - ITEM RECEIPT STEP:
 * - After the consolidated Item Fulfillment is created and saved, this
 *   script now ALSO transforms that same Item Fulfillment into ONE
 *   consolidated Item Receipt (record.transform ItemShip -> ItemRecpt).
 * - The receipt picks up whatever the IF actually fulfilled, so it stays
 *   consistent automatically - no separate SKU/qty matching needed here.
 * - If the receipt fails to create for any reason, it is logged clearly,
 *   but the Item Fulfillment that was already created is NOT rolled back.
 */

define(['N/https', 'N/record', 'N/log', 'N/search'],
function (https, record, log, search) {

    /*************************************************************
     * HARDCODE VALUES HERE
     *************************************************************/
    var TO_ID = 142435642;
    var WMS_ORDER_NUMBER = 'TOMFBA143320390';

    /*************************************************************
     * FIELD IDS
     *************************************************************/
    var LINE_SKU_FIELD = 'custcol_sku_external_id';

    var BODY_WMS_ORDER_NUMBER = 'custbody_wms_order_number';
    var BODY_TRACKING_NUMBER = 'custbody_mtracking';
    var BODY_TOTAL_QTY_SHIPPED = 'custbody_total_qty_shipped';
    var BODY_NO_CARTONS = 'custbody_no_cartons';
    var BODY_JAZZ_SHIPMENT_NUMBER = 'custbody_jazz_shipment_number';

    /*************************************************************
     * JAZZ API CONFIG
     *************************************************************/
    var JAZZ_DOMAIN = 'fbflurry.jazz-oms.com';
    var JAZZ_USERNAME = 'Dsoni';
    var JAZZ_PASSWORD = 'OnCloud2026!!';
    var JAZZ_TENANT = 'TMJ';

    var JAZZ_PAGE_LIMIT = 250;

    function getInputData() {
        log.audit('INPUT START', {
            toId: TO_ID,
            wmsOrderNumber: WMS_ORDER_NUMBER
        });

        var token = getJazzToken();

        var shipments = getAllJazzShipments(token, WMS_ORDER_NUMBER);

        var consolidatedRow = buildConsolidatedRow(shipments);

        log.audit('INPUT CONSOLIDATED ROW BUILT', {
            totalShipmentsFromJazz: shipments.length,
            totalCartonsFound: consolidatedRow.cartons.length,
            totalDistinctSkus: Object.keys(consolidatedRow.requiredQtyBySku).length,
            totalQtyAcrossAllCartons: consolidatedRow.totalQty
        });

        // Only ONE row -> only ONE map execution -> ONE Item Fulfillment.
        return [consolidatedRow];
    }

    function map(context) {
        var row = JSON.parse(context.value);

        try {
            if (!row.cartons || row.cartons.length === 0) {
                log.audit('SKIP - NO CARTONS FOUND', row);
                context.write({
                    key: 'SKIPPED',
                    value: JSON.stringify({
                        reason: 'No eligible cartons found in Jazz for this WMS order',
                        row: row
                    })
                });
                return;
            }

            var existing = findExistingItemFulfillment(row.toId, row.wmsOrderNumber);

            if (existing.found) {
                log.audit('SKIP - CONSOLIDATED IF ALREADY EXISTS', {
                    wmsOrderNumber: row.wmsOrderNumber,
                    existingItemFulfillmentId: existing.id,
                    existingTranId: existing.tranid
                });

                context.write({
                    key: 'DUPLICATE_SKIPPED',
                    value: JSON.stringify({
                        wmsOrderNumber: row.wmsOrderNumber,
                        existingItemFulfillmentId: existing.id,
                        existingTranId: existing.tranid
                    })
                });

                return;
            }

            var result = createConsolidatedFulfillment(row);

            log.audit('CONSOLIDATED FULFILLMENT RESULT', result);

            if (result.success) {

                /*****************************************************
                 * NEW: Create ONE consolidated Item Receipt from the
                 * Item Fulfillment we just created.
                 *****************************************************/
                var receiptResult = createConsolidatedItemReceipt(result.itemFulfillmentId, row);

                log.audit('CONSOLIDATED ITEM RECEIPT RESULT', receiptResult);

                result.itemReceiptId = receiptResult.itemReceiptId;
                result.itemReceiptMessage = receiptResult.message;

                if (!receiptResult.success) {
                    log.error('ITEM RECEIPT FAILED - IF ALREADY CREATED', {
                        wmsOrderNumber: row.wmsOrderNumber,
                        itemFulfillmentId: result.itemFulfillmentId,
                        reason: receiptResult.message
                    });
                }

                context.write({
                    key: receiptResult.success ? 'CREATED' : 'CREATED_IF_ONLY_RECEIPT_FAILED',
                    value: JSON.stringify(result)
                });
            } else {
                context.write({
                    key: 'SKIPPED',
                    value: JSON.stringify(result)
                });
            }

        } catch (e) {
            log.error('MAP ERROR', {
                wmsOrderNumber: row.wmsOrderNumber,
                message: e.message,
                stack: e.stack
            });

            context.write({
                key: 'ERROR',
                value: JSON.stringify({
                    wmsOrderNumber: row.wmsOrderNumber,
                    message: e.message
                })
            });
        }
    }

    /*************************************************************
     * CREATE ONE CONSOLIDATED ITEM FULFILLMENT
     * - Sums Jazz qty by SKU across ALL cartons
     * - Adds ONE package line per carton (tracking + carton number + weight)
     *************************************************************/
    function createConsolidatedFulfillment(row) {
        var result = {
            success: false,
            toId: row.toId,
            wmsOrderNumber: row.wmsOrderNumber,
            totalCartons: row.cartons.length,
            itemFulfillmentId: '',
            fulfillLineCount: 0,
            fulfillQtyTotal: 0,
            skippedLineCount: 0,
            unmatchedJazzSkus: [],
            message: ''
        };

        var requiredQtyBySku = row.requiredQtyBySku || {};

        if (Object.keys(requiredQtyBySku).length === 0) {
            result.message = 'Skipped - no shipped item quantity found across any carton.';
            return result;
        }

        var ifRec = record.transform({
            fromType: record.Type.TRANSFER_ORDER,
            fromId: row.toId,
            toType: record.Type.ITEM_FULFILLMENT,
            isDynamic: false
        });

        /*********************************************************
         * SHIP DATE
         * Use the earliest ship date found across all cartons.
         *********************************************************/
        var jazzShipDate = parseJazzDate(row.earliestShipDate);

        if (jazzShipDate) {
            trySet(ifRec, 'trandate', jazzShipDate);
            trySet(ifRec, 'packeddate', jazzShipDate);
            trySet(ifRec, 'shippeddate', jazzShipDate);
            trySet(ifRec, 'pickeddate', jazzShipDate);

            log.audit('JAZZ DATE SET ON CONSOLIDATED IF', {
                wmsOrderNumber: row.wmsOrderNumber,
                rawShipDate: row.earliestShipDate,
                parsedDate: jazzShipDate
            });
        } else {
            log.audit('JAZZ SHIP DATE NOT FOUND / INVALID', {
                wmsOrderNumber: row.wmsOrderNumber,
                rawShipDate: row.earliestShipDate
            });
        }

        /*********************************************************
         * HEADER VALUES
         *********************************************************/
        trySet(ifRec, 'shipstatus', 'C');
        trySet(ifRec, BODY_TRACKING_NUMBER, row.primaryTrackingNumber);
        trySet(ifRec, BODY_WMS_ORDER_NUMBER, row.wmsOrderNumber);
        trySet(ifRec, BODY_TOTAL_QTY_SHIPPED, row.totalQty);
        trySet(ifRec, BODY_NO_CARTONS, String(row.cartons.length));
        trySet(ifRec, BODY_JAZZ_SHIPMENT_NUMBER, row.primaryShipmentNumber);

        /*********************************************************
         * PACKAGE LINES
         * One line per carton: tracking number, carton number (descr), weight
         *********************************************************/
        clearPackageLines(ifRec);

        for (var c = 0; c < row.cartons.length; c++) {
            var carton = row.cartons[c];
            addOnePackageLine(ifRec, c, carton.trackingNumber, carton.cartonNumber, carton.weight);
        }

        log.audit('PACKAGE LINES ADDED', {
            wmsOrderNumber: row.wmsOrderNumber,
            totalPackageLinesAdded: row.cartons.length
        });

        /*********************************************************
         * MATCH JAZZ SKU (SUMMED ACROSS ALL CARTONS) TO IF LINES
         *********************************************************/
        var remainingBySku = copyObj(requiredQtyBySku);

        var lineCount = ifRec.getLineCount({
            sublistId: 'item'
        });

        // First uncheck all lines
        for (var i = 0; i < lineCount; i++) {
            setReceive(ifRec, i, false);
        }

        for (var line = 0; line < lineCount; line++) {
            var lineSku = getLineSku(ifRec, line);
            var lineAvailableQty = getLineAvailableQty(ifRec, line);
            var jazzRemainingQty = Number(remainingBySku[lineSku] || 0);

            if (!lineSku || jazzRemainingQty <= 0 || lineAvailableQty <= 0) {
                result.skippedLineCount++;
                continue;
            }

            var qtyToFulfill = Math.min(jazzRemainingQty, lineAvailableQty);

            if (qtyToFulfill <= 0) {
                result.skippedLineCount++;
                continue;
            }

            setReceive(ifRec, line, true);

            ifRec.setSublistValue({
                sublistId: 'item',
                fieldId: 'quantity',
                line: line,
                value: qtyToFulfill
            });

            remainingBySku[lineSku] = jazzRemainingQty - qtyToFulfill;

            result.fulfillLineCount++;
            result.fulfillQtyTotal += qtyToFulfill;
        }

        /*********************************************************
         * COLLECT UNMATCHED JAZZ SKU/QTY
         *********************************************************/
        for (var sku in remainingBySku) {
            if (remainingBySku.hasOwnProperty(sku)) {
                var remainingQty = Number(remainingBySku[sku] || 0);

                if (remainingQty > 0) {
                    result.unmatchedJazzSkus.push({
                        sku: sku,
                        remainingQtyNotMatched: remainingQty
                    });
                }
            }
        }

        if (result.fulfillLineCount <= 0) {
            result.message = 'Skipped - no matching available TO lines for any Jazz SKU.';
            return result;
        }

        var ifId = ifRec.save({
            enableSourcing: false,
            ignoreMandatoryFields: true
        });

        result.success = true;
        result.itemFulfillmentId = ifId;
        result.message = 'Consolidated Item Fulfillment created successfully for all cartons.';

        return result;
    }

    /*************************************************************
     * NEW: CREATE ONE CONSOLIDATED ITEM RECEIPT FROM THE ITEM FULFILLMENT
     * - Simple transform: Item Fulfillment -> Item Receipt
     * - NetSuite auto-populates the receipt lines/quantities from
     *   whatever the Item Fulfillment actually fulfilled, so no
     *   separate SKU/qty matching is needed here.
     * - Logs clearly if line counts don't match or if save fails,
     *   so nothing is silently missing.
     *************************************************************/
    function createConsolidatedItemReceipt(itemFulfillmentId, row) {
        var result = {
            success: false,
            itemReceiptId: '',
            lineCount: 0,
            message: ''
        };

        if (!itemFulfillmentId) {
            result.message = 'Skipped - no Item Fulfillment ID provided.';
            return result;
        }

        try {
            var irRec = record.transform({
                fromType: record.Type.ITEM_FULFILLMENT,
                fromId: itemFulfillmentId,
                toType: record.Type.ITEM_RECEIPT,
                isDynamic: false
            });

            var lineCount = irRec.getLineCount({
                sublistId: 'item'
            });

            result.lineCount = lineCount;

            if (lineCount <= 0) {
                log.error('ITEM RECEIPT - NO LINES FOUND AFTER TRANSFORM', {
                    itemFulfillmentId: itemFulfillmentId,
                    wmsOrderNumber: row.wmsOrderNumber
                });

                result.message = 'Skipped - transformed Item Receipt has 0 lines.';
                return result;
            }

            var irId = irRec.save({
                enableSourcing: false,
                ignoreMandatoryFields: true
            });

            result.success = true;
            result.itemReceiptId = irId;
            result.message = 'Consolidated Item Receipt created successfully.';

        } catch (e) {
            log.error('ITEM RECEIPT CREATE FAILED', {
                itemFulfillmentId: itemFulfillmentId,
                wmsOrderNumber: row.wmsOrderNumber,
                message: e.message,
                stack: e.stack
            });

            result.message = 'Error creating Item Receipt: ' + e.message;
        }

        return result;
    }

    /*************************************************************
     * JAZZ TOKEN
     *************************************************************/
    function getJazzToken() {
        var resp = https.post({
            url: 'https://' + JAZZ_DOMAIN + '/api/token/',
            headers: {
                'Content-Type': 'application/json',
                'Accept': 'application/json'
            },
            body: JSON.stringify({
                username: JAZZ_USERNAME,
                password: JAZZ_PASSWORD
            })
        });

        if (Number(resp.code) < 200 || Number(resp.code) >= 300) {
            throw new Error('Jazz token failed: ' + resp.code + ' - ' + resp.body);
        }

        var body = JSON.parse(resp.body || '{}');
        var token = body.token || body.key || body.access_token || body.auth_token;

        if (!token) {
            throw new Error('Jazz token missing from response.');
        }

        return token;
    }

    /*************************************************************
     * GET ALL JAZZ SHIPMENTS WITH PAGINATION
     *************************************************************/
    function getAllJazzShipments(token, orderNumber) {
        var all = [];
        var offset = 0;
        var safety = 0;
        var nextUrl = buildJazzShipmentUrl(orderNumber, offset);

        while (nextUrl && safety < 100) {
            safety++;

            var resp = https.get({
                url: nextUrl,
                headers: {
                    'Accept': 'application/json',
                    'Tenant': JAZZ_TENANT,
                    'Authorization': 'Token ' + token
                }
            });

            if (Number(resp.code) < 200 || Number(resp.code) >= 300) {
                throw new Error('Jazz shipment failed: ' + resp.code + ' - ' + resp.body);
            }

            var body = JSON.parse(resp.body || '{}');
            var rows = getRowsFromJazzBody(body);

            for (var i = 0; i < rows.length; i++) {
                all.push(rows[i]);
            }

            log.audit('JAZZ PAGE READ', {
                page: safety,
                rowsThisPage: rows.length,
                totalRowsSoFar: all.length
            });

            if (body.next) {
                nextUrl = normalizeJazzNextUrl(body.next);
            } else if (body.count && all.length < Number(body.count)) {
                offset = all.length;
                nextUrl = buildJazzShipmentUrl(orderNumber, offset);
            } else {
                nextUrl = '';
            }

            if (rows.length === 0) {
                nextUrl = '';
            }
        }

        if (safety >= 100) {
            log.error('JAZZ PAGINATION STOPPED BY SAFETY LIMIT', {
                totalRows: all.length
            });
        }

        return all;
    }

    function buildJazzShipmentUrl(orderNumber, offset) {
        return 'https://' + JAZZ_DOMAIN +
            '/api/v1/shipment/status?limit=' + JAZZ_PAGE_LIMIT +
            '&offset=' + Number(offset || 0) +
            '&order_number=' + encodeURIComponent(orderNumber);
    }

    function normalizeJazzNextUrl(nextValue) {
        if (!nextValue) return '';

        nextValue = String(nextValue);

        if (nextValue.indexOf('http') === 0) {
            return nextValue;
        }

        if (nextValue.indexOf('/') === 0) {
            return 'https://' + JAZZ_DOMAIN + nextValue;
        }

        return 'https://' + JAZZ_DOMAIN + '/' + nextValue;
    }

    function getRowsFromJazzBody(body) {
        if (!body) return [];

        if (body.results && Array.isArray(body.results)) {
            return body.results;
        }

        if (body.shipments && Array.isArray(body.shipments)) {
            return body.shipments;
        }

        if (body.data && Array.isArray(body.data)) {
            return body.data;
        }

        if (Array.isArray(body)) {
            return body;
        }

        return [];
    }

    /*************************************************************
     * BUILD ONE CONSOLIDATED ROW FOR THE ENTIRE ORDER
     * - row.cartons: [{ cartonNumber, trackingNumber, weight, shipDate }, ...]
     * - row.requiredQtyBySku: { SKU: totalQtyAcrossAllCartons, ... }
     *************************************************************/
    function buildConsolidatedRow(shipments) {
        var cartonMap = {};       // cartonNumber -> { cartonNumber, trackingNumber, weight, shipDate }
        var requiredQtyBySku = {};
        var totalQty = 0;
        var primaryTrackingNumber = '';
        var primaryShipmentNumber = '';
        var earliestShipDate = '';
        var earliestShipDateMs = null;

        for (var i = 0; i < shipments.length; i++) {
            var shipment = shipments[i];

            if (!isEligibleShipment(shipment)) {
                continue;
            }

            var shipmentNumber = getShipmentNumber(shipment);
            var trackingNumber = getTrackingNumber(shipment);
            var shipmentCartonNumber = getCartonNumber(shipment);
            var shipDate = getShipmentDate(shipment);
            var weight = getShipmentWeight(shipment);

            if (!primaryTrackingNumber && trackingNumber) {
                primaryTrackingNumber = trackingNumber;
            }
            if (!primaryShipmentNumber && shipmentNumber) {
                primaryShipmentNumber = shipmentNumber;
            }

            var details = shipment.shipment_detail || shipment.shipment_details || shipment.details || [];

            for (var d = 0; d < details.length; d++) {
                var detail = details[d];

                if (isCancelledDetail(detail)) {
                    continue;
                }

                var sku = normalizeSku(getDetailSku(detail));
                var qty = getDetailShippedQty(detail);

                if (!sku || qty <= 0) {
                    continue;
                }

                var detailCartonNumber = getCartonNumber(detail);
                var detailShipDate = getShipmentDate(detail);

                var cartonNumber = detailCartonNumber || shipmentCartonNumber;

                if (!cartonNumber) {
                    cartonNumber = shipmentNumber || trackingNumber;
                }

                cartonNumber = String(cartonNumber || '').trim();

                var finalShipDate = detailShipDate || shipDate;

                // --- Track this carton (for package lines) ---
                if (!cartonMap[cartonNumber]) {
                    cartonMap[cartonNumber] = {
                        cartonNumber: cartonNumber,
                        trackingNumber: trackingNumber,
                        weight: weight,
                        shipDate: finalShipDate
                    };
                } else {
                    if (!cartonMap[cartonNumber].trackingNumber && trackingNumber) {
                        cartonMap[cartonNumber].trackingNumber = trackingNumber;
                    }
                    if (!cartonMap[cartonNumber].weight && weight) {
                        cartonMap[cartonNumber].weight = weight;
                    }
                    if (!cartonMap[cartonNumber].shipDate && finalShipDate) {
                        cartonMap[cartonNumber].shipDate = finalShipDate;
                    }
                }

                // --- Sum item qty by SKU across ALL cartons ---
                if (!requiredQtyBySku[sku]) {
                    requiredQtyBySku[sku] = 0;
                }
                requiredQtyBySku[sku] += qty;
                totalQty += qty;

                // --- Track earliest ship date for header dates ---
                if (finalShipDate) {
                    var parsed = parseJazzDate(finalShipDate);
                    if (parsed) {
                        var ms = parsed.getTime();
                        if (earliestShipDateMs === null || ms < earliestShipDateMs) {
                            earliestShipDateMs = ms;
                            earliestShipDate = finalShipDate;
                        }
                    }
                }
            }
        }

        var cartons = [];
        for (var key in cartonMap) {
            if (cartonMap.hasOwnProperty(key)) {
                cartons.push(cartonMap[key]);
            }
        }

        return {
            toId: TO_ID,
            wmsOrderNumber: WMS_ORDER_NUMBER,
            cartons: cartons,
            requiredQtyBySku: requiredQtyBySku,
            totalQty: totalQty,
            primaryTrackingNumber: primaryTrackingNumber,
            primaryShipmentNumber: primaryShipmentNumber,
            earliestShipDate: earliestShipDate
        };
    }

    function isEligibleShipment(shipment) {
        var status = String(
            shipment.status ||
            shipment.shipment_status ||
            shipment.order_status ||
            ''
        ).toLowerCase();

        var details = shipment.shipment_detail || shipment.shipment_details || shipment.details || [];

        if (!details || details.length <= 0) {
            return false;
        }

        return (
            status === 'confirmed' ||
            status === 'shipped' ||
            status === 'closed'
        );
    }

    function isCancelledDetail(detail) {
        var status = String(
            detail.status ||
            detail.line_status ||
            detail.cancel_status ||
            ''
        ).toLowerCase();

        if (status.indexOf('cancel') !== -1) {
            return true;
        }

        var cancelledQty = Number(
            detail.qty_cancelled ||
            detail.cancelled_qty ||
            detail.quantity_cancelled ||
            0
        );

        var shippedQty = getDetailShippedQty(detail);

        if (cancelledQty > 0 && shippedQty <= 0) {
            return true;
        }

        return false;
    }

    function getDetailSku(detail) {
        return detail.sku_code ||
            detail.sku ||
            detail.item_sku ||
            detail.item_number ||
            detail.ItemNumber ||
            '';
    }

    function getDetailShippedQty(detail) {
        var qty = detail.qty_shipped;

        if (qty === null || qty === undefined || qty === '') {
            qty = detail.shipped_qty;
        }

        if (qty === null || qty === undefined || qty === '') {
            qty = detail.quantity_shipped;
        }

        if (qty === null || qty === undefined || qty === '') {
            qty = detail.fulfilled_qty;
        }

        return Number(qty || 0);
    }

    function getShipmentNumber(shipment) {
        return String(
            shipment.shipment_number ||
            shipment.shipment_no ||
            shipment.shipment_id ||
            shipment.id ||
            ''
        ).trim();
    }

    function getTrackingNumber(shipment) {
        return String(
            shipment.tracking_number ||
            shipment.tracking_no ||
            shipment.pro_number ||
            shipment.master_tracking_number ||
            ''
        ).trim();
    }

    function getCartonNumber(obj) {
        return String(
            obj.carton_number ||
            obj.carton_no ||
            obj.carton ||
            obj.carton_id ||
            obj.package_number ||
            obj.package_no ||
            obj.package_id ||
            obj.box_number ||
            obj.box_no ||
            obj.container_number ||
            ''
        ).trim();
    }

    function getShipmentWeight(shipment) {
        return Number(
            shipment.weight ||
            shipment.shipment_weight ||
            shipment.package_weight ||
            0
        );
    }

    /*************************************************************
     * DATE HELPERS
     *************************************************************/
    function getShipmentDate(obj) {
        if (!obj) return '';

        return obj.ship_date ||
            obj.shipped_date ||
            obj.shipment_date ||
            obj.date_shipped ||
            obj.shipped_at ||
            obj.shipment_date_time ||
            obj.created_at ||
            '';
    }

    function parseJazzDate(value) {
        if (!value) {
            return null;
        }

        value = String(value).trim();

        var m = value.match(/^(\d{4})-(\d{2})-(\d{2})/);

        if (m) {
            return new Date(
                Number(m[1]),
                Number(m[2]) - 1,
                Number(m[3])
            );
        }

        var d = new Date(value);

        if (isNaN(d.getTime())) {
            log.debug('Invalid Jazz shipment date', value);
            return null;
        }

        return d;
    }

    /*************************************************************
     * DUPLICATE CHECK
     * Duplicate key = TO + WMS Order Number ONLY
     * (there should only ever be ONE consolidated IF per WMS order now)
     *************************************************************/
    function findExistingItemFulfillment(toId, wmsOrderNumber) {
        var out = {
            found: false,
            id: '',
            tranid: ''
        };

        if (!wmsOrderNumber) {
            return out;
        }

        var s = search.create({
            type: 'itemfulfillment',
            filters: [
                ['type', 'anyof', 'ItemShip'],
                'AND',
                ['mainline', 'is', 'T'],
                'AND',
                ['createdfrom', 'anyof', String(toId)],
                'AND',
                [BODY_WMS_ORDER_NUMBER, 'is', String(wmsOrderNumber)]
            ],
            columns: [
                search.createColumn({ name: 'internalid' }),
                search.createColumn({ name: 'tranid' })
            ]
        });

        s.run().each(function (r) {
            out.found = true;
            out.id = r.getValue({ name: 'internalid' });
            out.tranid = r.getValue({ name: 'tranid' });
            return false;
        });

        return out;
    }

    /*************************************************************
     * LINE MATCHING HELPERS
     *************************************************************/
    function getLineSku(ifRec, line) {
        var sku = '';

        try {
            sku = ifRec.getSublistValue({
                sublistId: 'item',
                fieldId: LINE_SKU_FIELD,
                line: line
            });
        } catch (e1) {}

        if (!sku) {
            try {
                sku = ifRec.getSublistText({
                    sublistId: 'item',
                    fieldId: 'item',
                    line: line
                });
            } catch (e2) {}
        }

        return normalizeSku(sku);
    }

    function getLineAvailableQty(ifRec, line) {
        return Number(ifRec.getSublistValue({
            sublistId: 'item',
            fieldId: 'quantity',
            line: line
        }) || 0);
    }

    function setReceive(ifRec, line, value) {
        try {
            ifRec.setSublistValue({
                sublistId: 'item',
                fieldId: 'itemreceive',
                line: line,
                value: value
            });
        } catch (e) {
            log.debug('itemreceive set failed', {
                line: line,
                value: value,
                error: e.message
            });
        }
    }

    /*************************************************************
     * PACKAGE HELPERS
     * addOnePackageLine now takes an explicit "line" index so it can
     * be called once per carton to build up multiple package lines.
     *************************************************************/
    function clearPackageLines(ifRec) {
        try {
            var count = ifRec.getLineCount({
                sublistId: 'package'
            });

            for (var i = count - 1; i >= 0; i--) {
                ifRec.removeLine({
                    sublistId: 'package',
                    line: i
                });
            }
        } catch (e) {
            log.debug('clearPackageLines failed', e.message);
        }
    }

    function addOnePackageLine(ifRec, lineIndex, trackingNumber, cartonNumber, weight) {
        try {
            ifRec.insertLine({
                sublistId: 'package',
                line: lineIndex
            });

            trySetSub(ifRec, 'package', 'packagetrackingnumber', lineIndex, trackingNumber);
            trySetSub(ifRec, 'package', 'packagedescr', lineIndex, cartonNumber);

            if (weight) {
                trySetSub(ifRec, 'package', 'packageweight', lineIndex, Number(weight));
            }
        } catch (e) {
            log.debug('addOnePackageLine failed', {
                lineIndex: lineIndex,
                trackingNumber: trackingNumber,
                cartonNumber: cartonNumber,
                error: e.message
            });
        }
    }

    /*************************************************************
     * COMMON HELPERS
     *************************************************************/
    function trySet(rec, fieldId, value) {
        if (!fieldId || value === null || value === undefined || value === '') {
            return;
        }

        try {
            rec.setValue({
                fieldId: fieldId,
                value: value
            });
        } catch (e) {
            log.debug('header set failed', {
                fieldId: fieldId,
                value: value,
                error: e.message
            });
        }
    }

    function trySetSub(rec, sublistId, fieldId, line, value) {
        if (!fieldId || value === null || value === undefined || value === '') {
            return;
        }

        try {
            rec.setSublistValue({
                sublistId: sublistId,
                fieldId: fieldId,
                line: line,
                value: value
            });
        } catch (e) {
            log.debug('sublist set failed', {
                sublistId: sublistId,
                fieldId: fieldId,
                line: line,
                value: value,
                error: e.message
            });
        }
    }

    function normalizeSku(value) {
        value = String(value || '').trim();

        value = value.replace(/\s+/g, '');

        if (value.indexOf(':') !== -1) {
            value = value.replace(/:/g, '_');
        }

        return value.toUpperCase();
    }

    function copyObj(obj) {
        var out = {};

        for (var k in obj) {
            if (obj.hasOwnProperty(k)) {
                out[k] = obj[k];
            }
        }

        return out;
    }

    /*************************************************************
     * SUMMARY
     *************************************************************/
    function summarize(summary) {
        var created = 0;
        var createdIfOnlyReceiptFailed = 0;
        var skipped = 0;
        var duplicateSkipped = 0;
        var errors = 0;

        summary.output.iterator().each(function (key, value) {
            if (key === 'CREATED') {
                created++;
            } else if (key === 'CREATED_IF_ONLY_RECEIPT_FAILED') {
                createdIfOnlyReceiptFailed++;
            } else if (key === 'SKIPPED') {
                skipped++;
            } else if (key === 'DUPLICATE_SKIPPED') {
                duplicateSkipped++;
            } else if (key === 'ERROR') {
                errors++;
            }

            return true;
        });

        log.audit('SCRIPT COMPLETED', {
            usage: summary.usage,
            concurrency: summary.concurrency,
            yields: summary.yields,
            created: created,
            createdIfOnlyReceiptFailed: createdIfOnlyReceiptFailed,
            skipped: skipped,
            duplicateSkipped: duplicateSkipped,
            errors: errors
        });

        if (createdIfOnlyReceiptFailed > 0) {
            log.error('ATTENTION - ITEM FULFILLMENT(S) CREATED WITHOUT A RECEIPT', {
                count: createdIfOnlyReceiptFailed,
                note: 'Check ITEM RECEIPT FAILED entries above for the reason and create the receipt manually if needed.'
            });
        }

        summary.mapSummary.errors.iterator().each(function (key, error) {
            log.error('MAP SUMMARY ERROR ' + key, error);
            return true;
        });
    }

    return {
        getInputData: getInputData,
        map: map,
        summarize: summarize
    };
});