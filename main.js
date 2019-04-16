var appConfig = require('./settings.json');
var appQueue = require('./faQueue.js');
var mibConfig = require('./mib.json');

var snmp = require('snmp-native'), log4js = require('log4js'), moment = require('moment'), amqp = require('amqplib');

var genLogger = log4js.getLogger('General'), amqpLogger = log4js.getLogger('amqp'), snmpLogger = log4js.getLogger('snmp'), parserLogger = log4js.getLogger('parser');

log4js.configure(appConfig.logger);

let queueSettings = {
    url: 'amqp://' + appConfig.amqp.user + ':' + appConfig.amqp.password + '@' + appConfig.amqp.server,
    consumeQueueName: appConfig.amqp.consumeQueueName,
    isWorkerEnabled: true
};
appQueue.init(log4js, queueSettings, snmpHandler);

genLogger.info('Starting...');


function snmpHandler(msg) {
    let targetHost = JSON.parse(msg);
    let nowTime = moment().unix();
//    if ((nowTime >= targetHost.stime) && (nowTime <= targetHost.etime)) {
    if (nowTime <= targetHost.etime) {
        genLogger.debug('Target: "' + targetHost.target + '" Service: "' + targetHost.service + '" Date: "' + targetHost.rdate + '"');
        routeSNMPRequest(targetHost);
    } else {
        genLogger.warn('Too late to process. Target "' + targetHost.target + '" mib "' + targetHost.service + '" rdate -> "' + targetHost.rdate + '"');
    }
}


function singlePublish(queuename, payload) {
    amqpLogger.debug('Sending payload to the queue broker, service: ' + JSON.stringify(payload.service_name) + ' host: ' + JSON.stringify(payload.host) + ' data objects: ' + Object.keys(payload.data).length);
    appQueue.publishMsg("", queuename, JSON.stringify(payload));
}


function batchPublish(queuename, payloads) {
    payloads.forEach(function (payload) {
        amqpLogger.debug('Sending payload to the queue broker, service: ' + JSON.stringify(payload.service_name) + ' host: ' + JSON.stringify(payload.host) + ' data objects: ' + Object.keys(payload.data).length);
        appQueue.publishMsg("", queuename, JSON.stringify(payload));
    });
}

function bail(err) {
    genLogger.error(err);
    process.exit(1);
}

//function publishAMQP(queuename, msg) {
//    amqp.connect('amqp://' + appConfig.amqp.user + ':' + appConfig.amqp.password + '@' + appConfig.amqp.server).then(function (conn) {
//        return conn.createChannel().then(function (ch) {
//            var ok = ch.assertQueue(queuename, {durable: true});
//            return ok.then(function (_qok) {
//
//                ch.sendToQueue(queuename, new Buffer(JSON.stringify(msg)));
//                amqpLogger.debug("Sent " + JSON.stringify(msg));
//                return ch.close();
//            });
//        }).finally(function () {
//            conn.close();
//        });
//    }).catch(amqpLogger.warn);
//}


function cleanValue(value, type) {
    switch (type) {
        case 6:
            return value.oid.toString().replace(/,/g, '.');
            //      break;
        default:
            return value;
    }
}

function processSNMPGetData(agents, rawSNMPData) {
    rawSNMPData.forEach(function (vb) {
//        console.log(vb);
//        console.log(vb.oid);
//        console.log(typeof vb.oid);
//        let tt = vb.oid.toString();
        //var o = vb.oid.split('.');
        var tt = vb.oid.toString().replace(/,/g, '.');
//        console.log('tt ' + tt);
        let o = tt.split('.');
//      console.log('o ' + o);

//        console.log('idx ' + idx);
        let oid = o.join('.');

        let idx = o.pop();
        //  console.log('oid ' + oid);        
        if (typeof (agents['data'][idx]) === 'undefined')
            agents['data'][idx] = {};
//        console.log('oid ' + oid);
        //    console.log(mibConfig[agents.service_name].oid2mib[oid]);
        agents['data'][idx][mibConfig[agents.service_name].oid2mib[oid]] = vb.value.toString();

    });
    parserLogger.debug(agents);
}

/**
 * Process raw data for Mikrotik AP Clients
 * @param {array} agents
 * @param {array} rawSNMPData
 * @returns {undefined}
 */
function processRawData_MAPC(agents, rawSNMPData) {
    rawSNMPData.forEach(function (vb) {
        let ap_id = vb.oid.pop();
        agents['ap_id'] = ap_id;
        let mac6 = vb.oid.pop();
        let mac5 = vb.oid.pop();
        let mac4 = vb.oid.pop();
        let mac3 = vb.oid.pop();
        let mac2 = vb.oid.pop();
        let mac1 = vb.oid.pop();
        let mac_add_idx = mac1 + '.' + mac2 + '.' + mac3 + '.' + mac4 + '.' + mac5 + '.' + mac6;
        let mac_add = mac1.toString(16) + ':' + mac2.toString(16) + ':' + mac3.toString(16) + ':' + mac4.toString(16) + ':' + mac5.toString(16) + ':' + mac6.toString(16);
        let idx = mac_add_idx;
        let tst = vb.oid.toString().replace(/,/g, '.');
        let o = tst.split('.');
        let oid = o.join('.');
        let matches = tst.match(/(1\.3\.6\.1\.4\.1\.14988\.1\.1\.1\.2\.1.[0-9])/);
        if (typeof (agents['data'][idx]) === 'undefined')
            agents['data'][idx] = {};
        if ((vb.type === 4) && ((oid === '1.3.6.1.4.1.14988.1.1.1.2.1.1') || (oid === '1.3.6.1.4.1.14988.1.1.1.2.1.10'))) {
            agents['data'][idx][mibConfig[agents.service_name].oid2mib[tst]] = vb.valueHex.toString('hex');
        } else {
            agents['data'][idx][mibConfig[agents.service_name].oid2mib[tst]] = vb.value.toString();
        }

    });
//    parserLogger.debug(agents);
    singlePublish(appConfig.amqp.publishQueueName, agents);
}

function processRawData_ifmib(agents, rawSNMPData) {
    rawSNMPData.forEach(function (vb) {
        let idx = vb.oid.pop();
        let tst = vb.oid.toString().replace(/,/g, '.');
        let o = tst.split('.');
        let oid = o.join('.');
        if (typeof (agents['data'][idx]) === 'undefined')
            agents['data'][idx] = {};
//        if ((vb.type === 4) && (oid === '1.3.6.1.2.1.2.2.1.6')) {
        if (oid === '1.3.6.1.2.1.2.2.1.6') {
            ///        console.log('vb.valueHex.toString(hex) ' + vb.valueHex.toString('hex'));
            agents['data'][idx][mibConfig[agents.service_name].oid2mib[tst]] = vb.valueHex.toString('hex');
        } else {

            agents['data'][idx][mibConfig[agents.service_name].oid2mib[tst]] = vb.value.toString();
        }
    });
    //   parserLogger.debug(agents);
    singlePublish(appConfig.amqp.publishQueueName, agents);
}

function processSNMPWalkData(agents, rawSNMPData) {
    parserLogger.debug(agents.service_name + ' detected');
    switch (agents.service_name) {
        case 'mikrotik_ap_client':
            processRawData_MAPC(agents, rawSNMPData);
            break;
        case 'ifmib_interfaces':
            processRawData_ifmib(agents, rawSNMPData);
            break;
        default:
            parserLogger.debug('Using generic parser');
            rawSNMPData.forEach(function (vb) {
                let idx = vb.oid.pop();
                let tst = vb.oid.toString().replace(/,/g, '.');
                let o = tst.split('.');
                //   let oid = o.join('.');
                //   console.log(oid);
                if (typeof (agents['data'][idx]) === 'undefined')
                    agents['data'][idx] = {};
//                if (agents.service_name == 'host_resources_processor') {
//                    console.log(vb)
//                }
                agents['data'][idx][mibConfig[agents.service_name].oid2mib[tst]] = vb.value.toString();
            });
//            if (agents.service_name == 'host_resources_processor') {
//                console.log(agents);
//            }
            //           parserLogger.debug(agents.data);
            singlePublish(appConfig.amqp.publishQueueName, agents);
    }

}


function getSNMP(targetData) {
    let agents = {
        host: targetData.target,
        rdate: targetData.rdate,
        service_name: targetData.service,
        interval: targetData.interval,
        status: 'success',
        data: {}
    };
    let session = new snmp.Session({host: targetData.target, port: targetData.port, community: targetData.name, timeouts: [10000, 10000, 10000, 10000]});
    session.get({oid: mibConfig[targetData.service].oid}, function (error, varbinds) {
        if (error) {
            snmpLogger.error(error);
        } else {
            processSNMPGetData(agents, varbinds);
        }
        session.close();
    });
}

function walkSNMP(targetData) {
    let agents = {
        host: targetData.target,
        rdate: targetData.rdate,
        service_name: targetData.service,
        interval: targetData.interval,
        status: 'success',
        data: {}
    };
    let session = new snmp.Session({host: targetData.target, port: targetData.port, community: targetData.name, timeouts: [10000, 10000, 10000, 10000]});
    session.getSubtree({oid: mibConfig[targetData.service].oid}, function (error, varbinds) {
        if (error) {
            snmpLogger.error(error);
        } else {
            processSNMPWalkData(agents, varbinds);
        }
        session.close();
    });
}


function getServiceType(serviceName) {
    if (mibConfig[serviceName])
        return mibConfig[serviceName]['type'];
    return '';
}


function routeSNMPRequest(targetData) {
    switch (getServiceType(targetData.service)) {
        case 'get':
            getSNMP(targetData);
            break;
        case 'walk':
            walkSNMP(targetData);
            break;
        default:
            genLogger.warn('Unknown type');
    }




// determine what sub service type     





//    let agents = {
//        host: targetData.target,
//        rdate: targetData.rdate,
//        service_name: targetData.service,
//        data: {}
//    };
//    var session = new snmp.Session({host: targetData.target, port: targetData.port, community: targetData.name, timeouts: [10000, 10000, 10000, 10000]});
//    session.getSubtree({oid: mibConfig[targetData.service].oid}, function (error, varbinds) {
//        if (error) {
//            snmpLogger.error(error);
//        } else {
//            var i = 1;
//            snmpLogger.debug('Processing return data for ' + agents['host'] + ' rdate ' + agents['rdate']);
//            varbinds.forEach(function (vb) {
//                if (targetData.service === 'mikrotik_ap_client') {
//                    var ap_id = vb.oid.pop();
//                    agents['ap_id'] = ap_id;
//                    var mac6 = vb.oid.pop();
//                    var mac5 = vb.oid.pop();
//                    var mac4 = vb.oid.pop();
//                    var mac3 = vb.oid.pop();
//                    var mac2 = vb.oid.pop();
//                    var mac1 = vb.oid.pop();
//                    var mac_add_idx = mac1 + '.' + mac2 + '.' + mac3 + '.' + mac4 + '.' + mac5 + '.' + mac6;
//                    var mac_add = mac1.toString(16) + ':' + mac2.toString(16) + ':' + mac3.toString(16) + ':' + mac4.toString(16) + ':' + mac5.toString(16) + ':' + mac6.toString(16);
//                    var idx = mac_add_idx;
//                    var tst = vb.oid.toString().replace(/,/g, '.');
//                    var o = tst.split('.');
//                    var oid = o.join('.');
//                    var matches = tst.match(/(1\.3\.6\.1\.4\.1\.14988\.1\.1\.1\.2\.1.[0-9])/);
//                    if (typeof (agents['data'][idx]) === 'undefined')
//                        agents['data'][idx] = {};
//                    if ((vb.type === 4) && ((oid === '1.3.6.1.4.1.14988.1.1.1.2.1.1') || (oid === '1.3.6.1.4.1.14988.1.1.1.2.1.10'))) {
//                        agents['data'][idx][mibConfig[targetData.service].oid2mib[tst]] = vb.valueHex.toString('hex');
//                    } else {
//                        agents['data'][idx][mibConfig[targetData.service].oid2mib[tst]] = vb.value.toString();
//                    }
//                    i++;
//                } else {
//                    var idx = vb.oid.pop();
//                    var tst = vb.oid.toString().replace(/,/g, '.');
//                    var o = tst.split('.');
//                    var oid = o.join('.');
//
//                    if (typeof (agents['data'][idx]) === 'undefined')
//                        agents['data'][idx] = {};
//
//                    if ((vb.type === 4) && (oid === '1.3.6.1.2.1.2.2.1.6')) {
//                        agents['data'][idx][mibConfig[targetData.service].oid2mib[tst]] = vb.valueHex.toString('hex');
//                    } else {
//                        agents['data'][idx][mibConfig[targetData.service].oid2mib[tst]] = vb.value.toString();
//                    }
//                    i++;
//                }
//            });
//            //        singlePublish(appConfig.amqp.publishQueueName, agents);
//        }
//        session.close();
//    });
}

// Consumer
//open.then(function (conn) {
//    return conn.createChannel();
//}).then(function (ch) {
//    return ch.assertQueue(appConfig.amqp.queuenamecron).then(function (ok) {
//        ch.prefetch(1);
//        return ch.consume(appConfig.amqp.queuenamecron, function (msg) {
//            if (msg !== null) {
//                let strMsg = msg.content.toString();
//                let targetHost = JSON.parse(strMsg);
//                let nowTime = moment().unix();
//                if ((nowTime >= targetHost.stime) && (nowTime <= targetHost.etime)) {
//                    amqpLogger.debug('Target: "' + targetHost.target + '" Service: "' + targetHost.service + '" Date: "' + targetHost.rdate + '"');
//                    routeSNMPRequest(targetHost);
//                } else {
//                    amqpLogger.warn('Too late to process. Target "' + targetHost.target + '" mib "' + targetHost.service + '" rdate -> "' + targetHost.rdate + '"');
//                }
//                ch.ack(msg);
//            }
//        });
//    });
//}).catch(amqpLogger.warn);