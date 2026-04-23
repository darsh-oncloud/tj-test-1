/**
 * @NApiVersion 2.1
 * @NScriptType MapReduceScript
 *
 * FINAL — Jazz Shipment Status -> NetSuite Item Fulfillment (B2C)
 *
 * What it does:
 * 1) Search B2C Sales Orders that are Pending Fulfillment / Partially Fulfilled (line-level search)
 * 2) For each SO (reduce), call Jazz shipment status endpoint:
 *      /api/v1/shipment/status?limit=10&order_number={order_number}
 * 3) If shipment status is confirmed/shipped:
 *      - Build map sku_code -> qty_shipped
 *      - Transform SO -> Item Fulfillment
 *      - Set itemreceive + quantity by matching SKU (bundle-safe)
 *      - Save IF
 *      - Update SO with shipment/tracking/status fields (optional)
 *
 * IMPORTANT: This script matches by SKU, NOT Shopify line id.
 *
 * ---------------- Deployment Parameters ----------------
 * Jazz:
 * 1) custscript_jazz_domain             Text    (fbflurry-uat01.jazz-oms.com)
 * 2) custscript_jazz_username           Text
 * 3) custscript_jazz_password           Password/Text
 * 4) custscript_jazz_tenant             Text    (TMJ)
 * 5) custscript_jazz_ship_lookup_path   Text    (/api/v1/shipment/status?limit=10&order_number={order_number})
 *
 * B2C Filter:
 * 6) custscript_b2c_filter_type         Text    ("memo" | "sourcecode" | "custbodyfield")
 * 7) custscript_b2c_filter_value        Text    (e.g. "WEB" or "B2C" or "CART-B2C")
 * 8) custscript_b2c_body_fieldid        Text    (only if filter_type=custbodyfield)
 *
 * Order Number formula used to call Jazz:
 * 9) custscript_order_number_formula    Text    default "'TOMB2C'||{internalid}"
 *
 * SKU mapping:
 * 10) custscript_sku_fieldid_so_line    Text    default "custcol_sku_external_id"
 * 11) custscript_sku_transform_colon    Checkbox default T (replace ":" -> "_" to match Jazz sku_code)
 *
 * SO update fields (optional):
 * 12) custscript_jazz_shipstatus_field  Text    (SO fieldId to store ship status - list/text)
 * 13) custscript_jazz_tracking_field    Text    (SO fieldId to store tracking #)
 * 14) custscript_jazz_shipment_field    Text    (SO fieldId to store shipment #)
 * 15) custscript_if_created_field       Text    (SO fieldId to store IF internal id or mark done)
 * 16) custscript_error_field_so         Text    (SO fieldId to store last error)
 *
 * Testing:
 * 17) custscript_test_limit             Integer (optional limit SO lines)
 */

define(['N/search', 'N/runtime', 'N/https', 'N/record', 'N/log', 'N/file'],
function (search, runtime, https, record, log, file) {

  // -------------------------
  // Param helpers
  // -------------------------
  function gp(id, defVal) {
    var v = runtime.getCurrentScript().getParameter({ name: id });
    return (v === null || v === undefined || v === '') ? defVal : v;
  }
  function gb(id) {
    var v = runtime.getCurrentScript().getParameter({ name: id });
    return (v === true || v === 'T' || v === 'true');
  }

  function s(v){ return (v === null || v === undefined) ? '' : String(v); }
  function n(v){ var x = Number(v); return isFinite(x) ? x : 0; }
  function trunc(str, max){
    str = s(str); max = max || 2500;
    return str.length > max ? str.substring(0, max) + '...<truncated>' : str;
  }

  // -------------------------
  // Folder IDs
  // -------------------------
  var PENDING_FOLDER_ID   = 10697;
  var PROCESSED_FOLDER_ID = 10698;
  var ERROR_FOLDER_ID     = 10699;

  // -------------------------
  // Jazz Auth (cached)
  // -------------------------
  var _token = null;
  var _tokenAt = 0;

  function getToken() {
    var now = Date.now();
    if (_token && (now - _tokenAt) < (20 * 60 * 1000)) {
      log.debug('[TOKEN] cached', { ageSec: Math.round((now - _tokenAt)/1000) });
      return _token;
    }

    var domain = gp('custscript_jazz_domain', '');
    var user   = gp('custscript_jazz_username', '');
    var pass   = gp('custscript_jazz_password', '');
    if (!domain || !user || !pass) throw new Error('Missing Jazz params: domain/username/password');

    var url = 'https://' + domain + '/api/token/';
    log.audit('[TOKEN] request', { url: url, username: user });

    var resp = https.post({
      url: url,
      headers: { 'Content-Type': 'application/json', 'Accept': 'application/json' },
      body: JSON.stringify({ username: user, password: pass })
    });

    log.audit('[TOKEN] response', { http: resp.code, body: trunc(resp.body, 1200) });

    if (resp.code < 200 || resp.code >= 300) {
      throw new Error('Token API failed HTTP ' + resp.code + ' :: ' + s(resp.body));
    }

    var obj = JSON.parse(resp.body || '{}');
    var token = obj.token || obj.key || obj.access_token || obj.auth_token;
    if (!token) throw new Error('Token missing in response :: ' + s(resp.body));

    _token = token;
    _tokenAt = now;
    log.audit('[TOKEN] success', { tokenPreview: token.substring(0, 6) + '***' });
    return token;
  }

  // -------------------------
  // Jazz Shipment Status lookup (final endpoint)
  // -------------------------
  function jazzGetShipmentByOrder(orderNumber) {
    var domain = gp('custscript_jazz_domain', '');
    var tenant = gp('custscript_jazz_tenant', 'TMJ');
    var path   = gp('custscript_jazz_ship_lookup_path', '');

    if (!domain || !path) throw new Error('Missing Jazz domain or lookup path param');

    var token = getToken();

    var finalPath = path.replace('{order_number}', encodeURIComponent(orderNumber));
    var url = 'https://' + domain + finalPath;

    log.audit('[JAZZ GET] request', { url: url, tenant: tenant, order_number: orderNumber });

    var resp = https.get({
      url: url,
      headers: {
        'Accept': 'application/json',
        'Tenant': tenant,
        'Authorization': 'Token ' + token
      }
    });

    log.audit('[JAZZ GET] response', { http: resp.code, body: resp });

    if (resp.code === 404) return null;
    if (resp.code < 200 || resp.code >= 300) {
      throw new Error('Jazz GET failed HTTP ' + resp.code + ' :: ' + s(resp.body));
    }

    var obj = JSON.parse(resp.body || '{}');

    var arr = null;
    if (Array.isArray(obj)) arr = obj;
    else if (obj && Array.isArray(obj.results)) arr = obj.results;
    else if (obj && Array.isArray(obj.data)) arr = obj.data;
    else if (obj && Array.isArray(obj.shipments)) arr = obj.shipments;
    else if (obj && obj.result && Array.isArray(obj.result)) arr = obj.result;

    if (!arr || !arr.length) {
      log.audit('[JAZZ GET] no shipments returned', { order_number: orderNumber });
      return null;
    }

    var best = null;
    var bestScore = -1;

    for (var i = 0; i < arr.length; i++) {
      var sh = arr[i] || {};
      var st = s(sh.status).toLowerCase();

      var score = 0;
      if (st === 'confirmed' || st === 'shipped') score += 1000;
      if (sh.tracking_number) score += 50;
      if (sh.shipment_detail && sh.shipment_detail.length) score += 50;

      var dt = Date.parse(sh.created_at || '');
      if (!isNaN(dt)) score += Math.floor(dt / 1000) % 1000;

      if (score > bestScore) {
        bestScore = score;
        best = sh;
      }
    }

    log.audit('[JAZZ GET] selected shipment', {
      order_number: orderNumber,
      returned: arr.length,
      selected_status: best ? best.status : '',
      selected_shipment_number: best ? best.shipment_number : '',
      selected_tracking: best ? (best.tracking_number || '') : ''
    });

    return best;
  }

  // -------------------------
  // Main old search - now driven by internal ids from file lookup
  // -------------------------
  function buildSoSearch(soIds) {
    var filters = [
      ["type", "anyof", "SalesOrd"],
      "AND",
      ["mainline", "is", "T"],
      "AND",
      ["datecreated", "onorafter", "01/01/2026 12:00 am"],
      "AND",
      ["status", "anyof", "SalesOrd:D", "SalesOrd:E", "SalesOrd:B"],
      "AND",
      ["internalid", "anyof", soIds]
    ];

    var COL_WMS_ORDER = search.createColumn({
      name: "custbody_wms_order_number",
      label: "WMS Order Number"
    });

    var COL_SO_INTERNAL = search.createColumn({
      name: "internalid",
      label: "Internal ID"
    });

    var sObj = search.create({
      type: "salesorder",
      settings: [{ "name": "consolidationtype", "value": "ACCTTYPE" }],
      filters: filters,
      columns: [COL_WMS_ORDER, COL_SO_INTERNAL]
    });

    return {
      searchObj: sObj,
      cols: {
        COL_WMS_ORDER: COL_WMS_ORDER,
        COL_SO_INTERNAL: COL_SO_INTERNAL
      }
    };
  }

  function getTextOrValue(res, col) {
    var t = res.getText(col);
    if (t !== null && t !== '' && typeof t !== 'undefined') return t;
    var v = res.getValue(col);
    return (v === null || typeof v === 'undefined') ? '' : v;
  }

  // -------------------------
  // New file helpers
  // -------------------------
  function getPendingFiles() {
    var out = [];
    search.create({
      type: 'file',
      filters: [['folder', 'anyof', PENDING_FOLDER_ID]],
      columns: ['internalid', 'name']
    }).run().each(function (r) {
      out.push({
        id: r.getValue({ name: 'internalid' }),
        name: r.getValue({ name: 'name' })
      });
      return true;
    });
    return out;
  }

  function parseSingleCol(txt) {
    var a = (txt || '').split(/\r\n|\n|\r/), out = [], i, v;
    for (i = 0; i < a.length; i++) {
      v = s(a[i]).replace(/^\uFEFF/, '').trim();
      if (!v) continue;
      if (v.charAt(0) === '"' && v.charAt(v.length - 1) === '"') {
        v = v.substring(1, v.length - 1).replace(/""/g, '"');
      }
      out.push(v);
    }
    return out;
  }

  function searchSingleSo(fieldId, value, eligibleOnly) {
    if (!value) return null;

    var filters = [
      ["type","anyof","SalesOrd"],
      "AND",
      ["mainline","is","T"],
      "AND",
      [fieldId,"is",value]
    ];

    if (eligibleOnly) {
      filters.push("AND");
      filters.push(["status","anyof","SalesOrd:D","SalesOrd:E","SalesOrd:B"]);
    }

    var resObj = null;

    search.create({
      type: "salesorder",
      settings:[{"name":"consolidationtype","value":"ACCTTYPE"}],
      filters: filters,
      columns: [
        search.createColumn({name: "internalid", label: "Internal ID"}),
        search.createColumn({name: "statusref", label: "Status Ref"}),
        search.createColumn({name: "custbody_wms_order_number", label: "WMS Order Number"})
      ]
    }).run().each(function(result){
      resObj = {
        soId: s(result.getValue({ name: 'internalid' })),
        statusRef: s(result.getValue({ name: 'statusref' })),
        wmsOrderNumber: s(result.getValue({ name: 'custbody_wms_order_number' }))
      };
      return false;
    });

    return resObj;
  }

  function findLookupResult(orderNo) {
    var stripped = s(orderNo).replace(/^TOMB2C/i, '');
    var r;

    r = searchSingleSo('custbody_wms_order_number', orderNo, true);
    if (r && r.soId) {
      return {
        ok: true,
        soId: r.soId,
        orderNumber: r.wmsOrderNumber || orderNo
      };
    }

    if (stripped) {
      r = searchSingleSo('custbody_shopify_order_id_2', stripped, true);
      if (r && r.soId) {
        return {
          ok: true,
          soId: r.soId,
          orderNumber: r.wmsOrderNumber || orderNo
        };
      }
    }

    r = searchSingleSo('custbody_wms_order_number', orderNo, false);
    if (r && r.soId) {
      return {
        ok: false,
        reason: 'Sales Order found but status is not eligible: ' + (r.statusRef || 'Unknown')
      };
    }

    if (stripped) {
      r = searchSingleSo('custbody_shopify_order_id_2', stripped, false);
      if (r && r.soId) {
        return {
          ok: false,
          reason: 'Sales Order found but status is not eligible: ' + (r.statusRef || 'Unknown')
        };
      }
    }

    return {
      ok: false,
      reason: 'Sales Order not found in NetSuite.'
    };
  }

  function moveFile(fileId, folderId) {
    var f = file.load({ id: fileId });
    f.folder = folderId;
    f.save();
  }

  function makeErrorFile(fileName, errs) {
    var c = 'Order Number,Error Reason\n', i, d = new Date();
    for (i = 0; i < errs.length; i++) {
      c += '"' + s(errs[i].orderNumber).replace(/"/g, '""') + '","' + s(errs[i].reason).replace(/"/g, '""') + '"\n';
    }
    return file.create({
      name: 'Error_File_' + fileName.replace(/\.[^\.]+$/, '') + '_' +
        d.getFullYear() + ('0' + (d.getMonth() + 1)).slice(-2) + ('0' + d.getDate()).slice(-2) + '_' +
        ('0' + d.getHours()).slice(-2) + ('0' + d.getMinutes()).slice(-2) + ('0' + d.getSeconds()).slice(-2) + '.csv',
      fileType: file.Type.CSV,
      contents: c,
      folder: ERROR_FOLDER_ID
    }).save();
  }

  function simpleErr(e) {
    var m = s(e && e.message ? e.message : e);
    if (!m) return 'Unknown error';
    if (m.indexOf('Missing WMS Order Number') !== -1) return 'WMS Order Number is missing on the Sales Order.';
    if (m.indexOf('No shipment returned from Jazz') !== -1) return 'Shipment not found in Jazz.';
    if (m.indexOf('Shipment not eligible') !== -1) return 'Shipment is not ready for fulfillment.';
    if (m.indexOf('No SKU matched') !== -1) return 'No matching item was found to fulfill.';
    if (m.indexOf('Sales Order not found in NetSuite.') !== -1) return 'Sales Order not found in NetSuite.';
    return m.substring(0, 300);
  }

  // -------------------------
  // getInputData
  // -------------------------
  function getInputData() {
    var files = getPendingFiles();
    var rows = [];
    var limit = parseInt(gp('custscript_test_limit', ''), 10) || 0;

    log.audit('[INPUT] pending files', { count: files.length, limit: limit });

    for (var fi = 0; fi < files.length; fi++) {
      if (limit && rows.length >= limit) break;

      var f = file.load({ id: files[fi].id });
      var vals = parseSingleCol(f.getContents());
      var validIds = [];
      var idMap = {};
      var i, x, built, sObj, C, paged, sampleLogged = false;

      log.audit('[INPUT] file read', {
        fileId: files[fi].id,
        fileName: files[fi].name,
        rowCount: vals.length
      });

      for (i = 0; i < vals.length; i++) {
        if (limit && rows.length >= limit) break;

        x = findLookupResult(vals[i]);

        if (x.ok && x.soId) {
          if (!idMap[x.soId]) {
            idMap[x.soId] = {
              sourceOrderNumber: vals[i],
              fileId: files[fi].id,
              fileName: files[fi].name
            };
            validIds.push(x.soId);
          }
        } else {
          rows.push({
            fileId: files[fi].id,
            fileName: files[fi].name,
            sourceOrderNumber: vals[i],
            soId: '',
            orderNumber: '',
            skipProcess: true,
            errorReason: x.reason || 'Sales Order not found in NetSuite.'
          });
        }
      }

      if (validIds.length) {
        built = buildSoSearch(validIds);
        sObj = built.searchObj;
        C = built.cols;
        paged = sObj.runPaged({ pageSize: 1000 });

        log.audit('[INPUT] main search count', {
          fileName: files[fi].name,
          count: paged.count,
          pages: paged.pageRanges.length
        });

        paged.pageRanges.forEach(function (pr) {
          if (limit && rows.length >= limit) return;

          var page = paged.fetch({ index: pr.index });

          for (var j = 0; j < page.data.length; j++) {
            if (limit && rows.length >= limit) break;

            var r = page.data[j];
            var soId = s(getTextOrValue(r, C.COL_SO_INTERNAL));

            var row = {
              fileId: files[fi].id,
              fileName: files[fi].name,
              sourceOrderNumber: idMap[soId] ? idMap[soId].sourceOrderNumber : '',
              soId: soId,
              orderNumber: s(getTextOrValue(r, C.COL_WMS_ORDER))
            };

            rows.push(row);

            if (!sampleLogged) {
              sampleLogged = true;
              log.audit('[INPUT] sample row', row);
            }
          }
        });
      }
    }

    log.audit('[INPUT] rows built', { rows: rows.length });
    return rows;
  }

  // -------------------------
  // map
  // -------------------------
  function map(context) {
    var row = JSON.parse(context.value);

    log.debug('[MAP] emit', {
      fileId: row.fileId,
      fileName: row.fileName,
      soId: row.soId,
      orderNumber: row.orderNumber,
      skipProcess: row.skipProcess || false
    });

    context.write({
      key: row.fileId + '|' + row.fileName,
      value: row
    });
  }

  // -------------------------
  // reduce
  // -------------------------
  function reduce(context) {
    var keyParts = context.key.split('|');
    var fileId = keyParts[0];
    var fileName = keyParts.slice(1).join('|');
    var rows = [];
    var errs = [];
    var i;

    for (i = 0; i < context.values.length; i++) {
      rows.push(JSON.parse(context.values[i]));
    }
    if (!rows.length) return;

    log.audit('[REDUCE] file start', {
      fileId: fileId,
      fileName: fileName,
      rowCount: rows.length
    });

    for (i = 0; i < rows.length; i++) {
      var row = rows[i];
      var soId = row.soId;
      var orderNumber = row.orderNumber;

      if (row.skipProcess) {
        errs.push({
          orderNumber: row.sourceOrderNumber || '',
          reason: row.errorReason || 'Sales Order not found in NetSuite.'
        });
        continue;
      }

      log.audit('[REDUCE] start', {
        soId: soId,
        orderNumber: orderNumber
      });

      if (!orderNumber) {
        errs.push({
          orderNumber: row.sourceOrderNumber || '',
          reason: 'WMS Order Number is missing on the Sales Order.'
        });
        markFailed(soId, 'Missing WMS Order Number (custbody_wms_order_number) from search result.');
        continue;
      }

      var shipment;
      try {
        shipment = jazzGetShipmentByOrder(orderNumber);
        log.debug('shipment', shipment);
      } catch (e) {
        log.error('[REDUCE] Jazz GET error', e);
        markFailed(soId, 'Jazz GET error :: ' + (e && e.message ? e.message : String(e)));
        errs.push({
          orderNumber: row.sourceOrderNumber || orderNumber,
          reason: simpleErr(e)
        });
        continue;
      }

      if (!shipment) {
        log.audit('[REDUCE] No shipment found in Jazz', { soId: soId, orderNumber: orderNumber });
        errs.push({
          orderNumber: row.sourceOrderNumber || orderNumber,
          reason: 'Shipment not found in Jazz.'
        });
        continue;
      }

      var st = s(shipment.status).toLowerCase();
      if (st !== 'confirmed' && st !== 'shipped') {
        log.audit('[REDUCE] Shipment not confirmed/shipped', {
          soId: soId,
          orderNumber: orderNumber,
          status: shipment.status
        });
        updateSoTrackingFields(soId, shipment, false, null);
        errs.push({
          orderNumber: row.sourceOrderNumber || orderNumber,
          reason: 'Shipment is not ready for fulfillment.'
        });
        continue;
      }

      var shipMap = {};
      var details = shipment.shipment_detail || [];
      for (var d = 0; d < details.length; d++) {
        var sku = s(details[d].sku_code);
        var q   = n(details[d].qty_shipped);
        if (!sku) continue;
        shipMap[sku] = (shipMap[sku] || 0) + q;
      }

      log.audit('[REDUCE] Jazz shipment parsed', {
        soId: soId,
        orderNumber: orderNumber,
        status: shipment.status,
        shipment_number: shipment.shipment_number || '',
        tracking_number: shipment.tracking_number || '',
        detailLines: details.length,
        uniqueSkus: Object.keys(shipMap).length
      });

      var ifId;
      try {
        ifId = createItemFulfillmentFromShipment(soId, shipment, shipMap);
      } catch (e2) {
        log.error('[REDUCE] IF create error', e2);
        markFailed(soId, 'IF create error :: ' + (e2 && e2.message ? e2.message : String(e2)));
        errs.push({
          orderNumber: row.sourceOrderNumber || orderNumber,
          reason: simpleErr(e2)
        });
        continue;
      }

      log.audit('[REDUCE] IF created', { soId: soId, ifId: ifId, orderNumber: orderNumber });
      updateSoTrackingFields(soId, shipment, true, ifId);
      markSuccess(soId, ifId);
    }

    if (errs.length) {
      makeErrorFile(fileName, errs);
    }

    moveFile(fileId, PROCESSED_FOLDER_ID);

    log.audit('[REDUCE] file end', {
      fileId: fileId,
      fileName: fileName,
      errorCount: errs.length
    });
  }

  // -------------------------
  // Create IF from shipment (SKU match, bundle-safe)
  // -------------------------
  /**
 * Create Item Fulfillment from Jazz shipment payload (bundle-safe SKU match)
 * - Matches Jazz shipment_detail[].sku_code (underscore) to NS item display (colon) or custcol_sku_external_id
 * - Sets header fields + package tracking (best effort)
 * - LOUD logs everywhere
 */
function createItemFulfillmentFromShipment(soId, shipment, shipMap) {
  var skuFieldId = gp('custscript_sku_fieldid_so_line', 'custcol_sku_external_id');
  var doColon = true;
  var debugSkus = Object.keys(shipMap || {});
  var shipCode = s(shipment && shipment.ship_code);

  log.audit('[IF] start', {
    soId: soId,
    order_number: s(shipment && shipment.order_number),
    shipment_number: s(shipment && shipment.shipment_number),
    status: s(shipment && shipment.status),
    ship_date: s(shipment && shipment.ship_date),
    tracking_number: s(shipment && shipment.tracking_number),
    ship_code: shipCode,
    uniqueSkusFromJazz: debugSkus.length,
    sampleSkusFromJazz: debugSkus.slice(0, 5)
  });

  var ifRec = record.transform({
    fromType: record.Type.SALES_ORDER,
    fromId: soId,
    toType: record.Type.ITEM_FULFILLMENT,
    isDynamic: false
  });

  log.audit('[IF] transformed', {
    soId: soId,
    ifLineCount: ifRec.getLineCount({ sublistId: 'item' }),
    skuFieldId: skuFieldId,
    colonToUnderscore: doColon
  });

  try { ifRec.setValue({ fieldId: 'shipstatus', value: 'C' }); } catch (eShip) {
    log.debug('[IF][HDR] shipstatus not set (ok)', eShip);
  }

  trySet(ifRec, 'custbody_mtracking', shipment.tracking_number || shipment.carton_number || '');
  trySet(ifRec, 'custbody_wms_order_number', shipment.order_number || '');
  trySet(ifRec, 'custbody_total_weight', shipment.weight || '');
  trySet(ifRec, 'custbody_no_cartons', '1');

  var totalQty = 0;
  var det = shipment.shipment_detail || [];
  for (var i = 0; i < det.length; i++) totalQty += n(det[i].qty_shipped);
  trySet(ifRec, 'custbody_total_qty_shipped', String(totalQty));

  trySet(ifRec, 'custbody_shopify_order_id', shipment.po_number || '');

  var shipmethodId = getShipMethodInternalIdFromShipCode(shipCode);
  log.debug('shipmethodId', shipmethodId);
  if (shipmethodId) {
    try {
      ifRec.setValue({ fieldId: 'shipmethod', value: shipmethodId.shipmethodId });
      ifRec.setValue({ fieldId: 'custbody_scac_routing_code', value: shipmethodId.scac });

      log.audit('[IF][HDR] shipmethod set', { ship_code: shipCode, shipmethodId: shipmethodId });
    } catch (eSM) {
      log.error('[IF][HDR] shipmethod set failed', { ship_code: shipCode, shipmethodId: shipmethodId, err: eSM });
    }
  } else {
    log.audit('[IF][HDR] shipmethod NOT set (no mapping)', { ship_code: shipCode });
  }

  try {
    if (shipment.ship_date) {
      var d0 = new Date(shipment.ship_date + 'T00:00:00Z');
      trySet(ifRec, 'trandate', d0);
      trySet(ifRec, 'pickeddate', d0);
      trySet(ifRec, 'packeddate', d0);
      trySet(ifRec, 'shippeddate', d0);
      log.audit('[IF][HDR] dates set', { ship_date: shipment.ship_date });
    }
  } catch (eDate) {
    log.debug('[IF][HDR] date set failed (ok)', eDate);
  }

  var lineCount = ifRec.getLineCount({ sublistId: 'item' });
  var fulfilledAny = false;
  var matchedLines = 0;

  for (var ln = 0; ln < lineCount; ln++) {
    var lineSkuRaw = getIfLineSkuRaw(ifRec, ln, skuFieldId);
    var lineSkuNorm = lineSkuRaw;
    if (doColon && lineSkuNorm) lineSkuNorm = lineSkuNorm.split(':').join('_');

    var shippedQty = shipMap[lineSkuNorm] || 0;

    log.debug('[IF][LINE] sku read', {
      line: ln,
      skuFieldId: skuFieldId,
      skuRaw: lineSkuRaw,
      skuNorm: lineSkuNorm,
      shippedQty: shippedQty,
      existsInJazzMap: shippedQty > 0
    });

    if (shippedQty > 0) {
      fulfilledAny = true;
      matchedLines++;

      try {
        ifRec.setSublistValue({ sublistId: 'item', fieldId: 'itemreceive', line: ln, value: true });
      } catch (eRec) {
        log.debug('[IF][LINE] itemreceive not set (ok)', { line: ln, err: eRec });
      }

      try {
        ifRec.setSublistValue({ sublistId: 'item', fieldId: 'quantity', line: ln, value: shippedQty });
      } catch (eQty) {
        log.error('[IF][LINE] quantity set failed', { line: ln, shippedQty: shippedQty, err: eQty });
        throw eQty;
      }

      try {
        var shopLine = findShopifyLineIdForSku(shipment, lineSkuNorm);
        if (shopLine) {
          ifRec.setSublistValue({
            sublistId: 'item',
            fieldId: 'custcol_shopify_line_item_id',
            line: ln,
            value: String(shopLine)
          });
          log.debug('[IF][LINE] shopify line id set', { line: ln, shopLine: shopLine });
        }
      } catch (eShop) {
        log.debug('[IF][LINE] shopify line id not set (ok)', { line: ln, err: eShop });
      }

    } else {
      try { ifRec.setSublistValue({ sublistId: 'item', fieldId: 'itemreceive', line: ln, value: false }); } catch (eNo) {}
    }
  }

  log.audit('[IF] line match summary', {
    soId: soId,
    lineCount: lineCount,
    matchedLines: matchedLines,
    fulfilledAny: fulfilledAny,
    jazzSkus: debugSkus.length
  });

  if (!fulfilledAny) {
    var firstIfSkus = [];
    for (var k = 0; k < Math.min(lineCount, 10); k++) {
      var r = getIfLineSkuRaw(ifRec, k, skuFieldId);
      var rn = r ? (doColon ? r.split(':').join('_') : r) : '';
      firstIfSkus.push(rn);
    }

    log.error('[IF] NO SKU MATCH - diagnostics', {
      soId: soId,
      skuFieldId: skuFieldId,
      doColon: doColon,
      jazzSkusSample: debugSkus.slice(0, 15),
      ifSkusSample: firstIfSkus
    });

    throw new Error('Shipment confirmed/shipped but NO SKU matched any IF line. Check sku_fieldid + colon transform.');
  }

  try {
    var tracking = shipment.tracking_number || shipment.carton_number || '';
    if (tracking) {
      var pkgCount = ifRec.getLineCount({ sublistId: 'package' });
      ifRec.insertLine({ sublistId: 'package', line: pkgCount });

      trySetSub(ifRec, 'package', 'packagedescr', pkgCount, tracking);
      trySetSub(ifRec, 'package', 'packagetrackingnumber', pkgCount, tracking);
      if (shipment.weight) trySetSub(ifRec, 'package', 'packageweight', pkgCount, n(shipment.weight));

      log.audit('[IF] package added', { tracking: tracking, weight: shipment.weight || '' });
    } else {
      log.audit('[IF] package skipped (no tracking/carton)', { shipment_number: shipment.shipment_number || '' });
    }
  } catch (ePkg) {
    log.debug('[IF] package set failed (ok)', ePkg);
  }

  log.audit('[IF] saving...', { soId: soId });
  var ifId = ifRec.save({ enableSourcing: false, ignoreMandatoryFields: true });
  log.audit('[IF] saved', { soId: soId, ifId: ifId });

  return ifId;
}

function getIfLineSkuRaw(ifRec, line, skuFieldId) {
  var v = '';

  try {
    v = s(ifRec.getSublistValue({ sublistId: 'item', fieldId: skuFieldId, line: line }));
    if (v) return v;
  } catch (e1) {}

  try {
    v = s(ifRec.getSublistText({ sublistId: 'item', fieldId: 'item', line: line }));
    if (v) return v;
  } catch (e2) {}

  return '';
}

function findShopifyLineIdForSku(shipment, lineSkuNorm) {
  try {
    var details = shipment.shipment_detail || [];
    for (var i = 0; i < details.length; i++) {
      var sku = s(details[i].sku_code);
      if (sku === lineSkuNorm) {
        var da = details[i].detail_attributes || {};
        return da.line_number || '';
      }
    }
  } catch (e) {}
  return '';
}

function findShopifyLineIdForSku(shipment, lineSkuNorm) {
  try {
    var details = shipment.shipment_detail || [];
    for (var i = 0; i < details.length; i++) {
      var sku = s(details[i].sku_code);
      if (sku === lineSkuNorm) {
        var da = details[i].detail_attributes || {};
        return da.line_number || '';
      }
    }
  } catch (e) {}
  return '';
}

  function trySet(rec, fieldId, val) {
    if (!fieldId || val === null || val === undefined || val === '') return;
    try { rec.setValue({ fieldId: fieldId, value: val }); } catch (e) { log.debug('[SET] failed ' + fieldId, e); }
  }
  function trySetSub(rec, sublistId, fieldId, line, val) {
    if (!fieldId || val === null || val === undefined || val === '') return;
    try { rec.setSublistValue({ sublistId: sublistId, fieldId: fieldId, line: line, value: val }); } catch (e) {}
  }

  // -------------------------
  // Update SO fields (optional)
  // -------------------------
  function updateSoTrackingFields(soId, shipment, shipped, ifId) {
    var fldShipStatus = gp('custscript_jazz_shipstatus_field', '');
    var fldTracking   = gp('custscript_jazz_tracking_field', '');
    var fldShipmentNo = gp('custscript_jazz_shipment_field', '');
    var fldIfCreated  = gp('custscript_if_created_field', '');

    var vals = {};

    if (fldShipStatus) vals[fldShipStatus] = shipped ? 3 : 2;

    if (fldTracking && shipment.tracking_number) vals[fldTracking] = shipment.tracking_number;
    if (fldShipmentNo && shipment.shipment_number) vals[fldShipmentNo] = shipment.shipment_number;

    if (fldIfCreated && ifId) vals[fldIfCreated] = String(ifId);

    if (Object.keys(vals).length === 0) return;

    log.audit('[SO UPDATE] submitFields', { soId: soId, values: vals });

    record.submitFields({
      type: record.Type.SALES_ORDER,
      id: soId,
      values: vals,
      options: { enableSourcing: false, ignoreMandatoryFields: true }
    });
  }

function getShipMethodInternalIdFromShipCode(shipCode) {
  var raw = gp('custscript_ship_code_mapping', '');
  if (!raw) return { shipmethodId: '', scac: '' };

  try {
    var map = JSON.parse(raw);
    var entry = map ? map[String(shipCode)] : null;

    if (!entry) return { shipmethodId: '', scac: '' };

    if (Array.isArray(entry) && entry.length === 2 && !Array.isArray(entry[0])) {
      return { shipmethodId: s(entry[0]), scac: s(entry[1]) };
    }

    if (Array.isArray(entry) && entry.length && Array.isArray(entry[0])) {
      for (var i = 0; i < entry.length; i++) {
        var pair = entry[i];
        if (Array.isArray(pair) && pair.length >= 2) {
          var sm = s(pair[0]);
          var sc = s(pair[1]);
          if (sm || sc) return { shipmethodId: sm, scac: sc };
        }
      }
      return { shipmethodId: '', scac: '' };
    }

    return { shipmethodId: '', scac: '' };

  } catch (e) {
    log.error('[SHIPCODE MAP] invalid JSON', { raw: trunc(raw, 800), err: e });
    return { shipmethodId: '', scac: '' };
  }
}

  function markSuccess(soId, ifId) {
    log.audit('[SUCCESS]', { soId: soId, ifId: ifId });
  }

  function markFailed(soId, msg) {
    log.error('[FAILED]', { soId: soId, msg: trunc(msg, 2000) });

    var errFld = gp('custscript_error_field_so', '');
    if (!errFld) return;

    try {
      var o = {}; o[errFld] = s(msg).substring(0, 999);
      record.submitFields({
        type: record.Type.SALES_ORDER,
        id: soId,
        values: o,
        options: { enableSourcing: false, ignoreMandatoryFields: true }
      });
    } catch (e) {
      log.error('[FAILED] error field update failed', e);
    }
  }

  // -------------------------
  // summarize
  // -------------------------
  function summarize(summary) {
    log.audit('[SUMMARY] done', { usage: summary.usage, concurrency: summary.concurrency, yields: summary.yields });

    if (summary.inputSummary && summary.inputSummary.error) {
      log.error('[SUMMARY] input error', summary.inputSummary.error);
    }

    summary.mapSummary.errors.iterator().each(function (k, e) {
      log.error('[SUMMARY] map error ' + k, e);
      return true;
    });

    summary.reduceSummary.errors.iterator().each(function (k, e) {
      log.error('[SUMMARY] reduce error ' + k, e);
      return true;
    });
  }

  return {
    getInputData: getInputData,
    map: map,
    reduce: reduce,
    summarize: summarize
  };

});