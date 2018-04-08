var appConfig = require('./settings.json');
var mibConfig = require('./mib.json');

var snmp = require('snmp-native'), log4js = require('log4js'), moment = require('moment'), amqp = require('amqplib');

var genLogger = log4js.getLogger('netstatsNodeSNMPPoller');
var amqpLogger = log4js.getLogger('amqp');
var snmpLogger = log4js.getLogger('snmp');

log4js.configure(appConfig.logger);

genLogger.info('Starting...');


//process.on('exit', function () {
//    console.log(agents);
//});

//function processSNMPData(res) {
//    for (var i = 0; i < res.length; i++) {
//        var o = res[i].oid.split('.');
//        var idx = o.pop();
//        var oid = o.join('.');
//        if (typeof (agents['data'][idx]) === 'undefined')
//            agents['data'][idx] = {};
//        if ((res[i].type === 'buffer') && (oid === '1.3.6.1.2.1.2.2.1.6')) {
//            agents['data'][idx][mibConfig.mikrotikMIB.interfaceMIB.oid2mib[oid]] = res[i].value.toString('hex');
//        } else {
//            agents['data'][idx][mibConfig.mikrotikMIB.interfaceMIB.oid2mib[oid]] = res[i].value.toString();
//        }
//    }
//}

//function m(target, oid, mgr) {
//    mgr.get(target, oid, function (e, r) {
//        if (e)
//            return logger.error(e);
//        for (var k in mibConfig.mikrotikMIB.interfaceMIB.oid2mib) {
//            mgr.bulkGet(target, k, function (e, r) {
//                if (e)
//                    return console.error(e);
//                processSNMPData(r);
//            });
//        }
//    }, {retries: 3});
//    console.log(agents);
//}

//function snmpMain(target, oid, service_name) {
//    agents['host'] = target;
//    agents['service_name'] = service_name;
//    agents['data'] = {};
//    logger.info('Running snmp mgr');
//    mgr = snmp.createManager({community: 'asuran', version: 2, retries: 10});
//    m(target, oid, mgr);
//}

function bail(err) {
    genLogger.error(err);
    process.exit(1);
}

var open = require('amqplib').connect('amqp://' + appConfig.amqp.user + ':' + appConfig.amqp.password + '@' + appConfig.amqp.server);


//function publishAMQP(queuename, msg) {
//    open.then(function (conn) {
//        return conn.createChannel();
//    }).then(function (ch) {
//        return ch.assertQueue(queuename).then(function (ok) {
//            return ch.sendToQueue(queuename, new Buffer(JSON.stringify(msg)));
//            try {
//                return ch.close();
//            } catch (alreadyClosed) {
//                logger.error(alreadyClosed.stackAtStateChange);
//            }
//        });
////    }).finally(function () {
////        ch.close();
//    }).catch(logger.warn);
//}

//function publishAMQP(queuename, msg) {
//    open.then(function (conn) {
//        return conn.createChannel();
//    }).then(function (ch) {
//        return ch.assertQueue(queuename).then(function (ok) {
//            return ch.sendToQueue(queuename, new Buffer(JSON.stringify(msg)));
//            try {
//                return ch.close();
//            } catch (alreadyClosed) {
//                logger.error(alreadyClosed.stackAtStateChange);
//            }
//        });
//    }).catch(logger.warn);
//}

function publishAMQP(queuename, msg) {
    amqp.connect('amqp://' + appConfig.amqp.user + ':' + appConfig.amqp.password + '@' + appConfig.amqp.server).then(function (conn) {
        return conn.createChannel().then(function (ch) {
            var ok = ch.assertQueue(queuename, {durable: true});
            return ok.then(function (_qok) {
                // NB: `sentToQueue` and `publish` both return a boolean
                // indicating whether it's OK to send again straight away, or
                // (when `false`) that you should wait for the event `'drain'`
                // to fire before writing again. We're just doing the one write,
                // so we'll ignore it.
                ch.sendToQueue(queuename, new Buffer(JSON.stringify(msg)));
                amqpLogger.debug("Sent " + JSON.stringify(msg));
                return ch.close();
            });
        }).finally(function () {
            conn.close();
        });
    }).catch(amqpLogger.warn);
}


function cleanValue(value, type) {
    switch (type) {
        case 6:
            return value.oid.toString().replace(/,/g, '.');
            //      break;
        default:
            return value;
    }
}

function processSNMPGetData(agents, res) {
    res.forEach(function (vb) {
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
    console.log(agents);
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
//    snmpLogger.warn(targetData.target);
//    snmpLogger.warn(targetData.port);
//    snmpLogger.warn(targetData.name);
    //snmpLogger.warn();
//    snmpLogger.warn(mibConfig[targetData.service].oid);

    let session = new snmp.Session({host: targetData.target, port: targetData.port, community: targetData.name, timeouts: [10000, 10000, 10000, 10000]});
    session.get({oid: mibConfig[targetData.service].oid}, function (error, varbinds) {
        if (error) {
            snmpLogger.error(error);
        } else {
            //   snmpLogger.warn(varbinds);
            agents.rawdata = varbinds;
            //       console.log(agents);
            processSNMPGetData(agents, varbinds);
//            return agents;
        }
        session.close();
    });
}

function walkSNMP(targetData) {
    let agents = {
        host: targetData.target,
        rdate: targetData.rdate,
        service_name: targetData.service,
        status: 'failed',
        data: {}
    };
    snmpLogger.info('1');
    let session = new snmp.Session({host: targetData.target, port: targetData.port, community: targetData.name, timeouts: [10000, 10000, 10000, 10000]});
    session.getSubtree({oid: mibConfig[targetData.service].oid}, function (error, varbinds) {
        snmpLogger.info('2');
        if (error) {
            snmpLogger.error(error);
        } else {
            agents.status = 'success';
            snmpLogger.info(varbinds.length);
            agents.data = varbinds;
        }
        session.close();
    });
    snmpLogger.info('3');
    return agents;
}


function getServiceType(serviceName) {
    if (mibConfig[serviceName])
        return mibConfig[serviceName]['type'];
    return '';
}


function pollTarget(targetData) {


//    console.log(targetData.service);

    //  let stype = getServiceType(targetData.service);

    switch (getServiceType(targetData.service)) {
        case 'get':
//            console.log('GET');
            getSNMP(targetData);
            break;
        case 'walk':
            console.log('WALK');
            break;
        default:
            console.log('Unknown type');
    }
    //  console.log(data);


    //  console.log(mibConfig);



// service: 'testing', 



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
//            //        publishAMQP(appConfig.amqp.queuenamedata, agents);
//        }
//        session.close();
//    });
}

// Consumer
open.then(function (conn) {
    return conn.createChannel();
}).then(function (ch) {
    return ch.assertQueue(appConfig.amqp.queuenamecron).then(function (ok) {
        ch.prefetch(1);
        return ch.consume(appConfig.amqp.queuenamecron, function (msg) {
            if (msg !== null) {
                let strMsg = msg.content.toString();
                let targetHost = JSON.parse(strMsg);
                let nowTime = moment().unix();
                if ((nowTime >= targetHost.stime) && (nowTime <= targetHost.etime)) {
                    amqpLogger.debug('Target: "' + targetHost.target + '" Service: "' + targetHost.service + '" Date: "' + targetHost.rdate + '"');
                    pollTarget(targetHost);
                } else {
                    amqpLogger.warn('Too late to process. Target "' + targetHost.target + '" mib "' + targetHost.service + '" rdate -> "' + targetHost.rdate + '"');
                }
                ch.ack(msg);
            }
        });
    });
}).catch(amqpLogger.warn);