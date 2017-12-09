var snmp = require('snmp-native');
var appConfig = require('./settings.json');
var mibConfig = require('./mib.json');
var log4js = require('log4js');
var moment = require('moment');
var cron5 = require('node-cron');

var amqp = require('amqplib');

//var agents = {};


initLogger();
var logger = log4js.getLogger('wugmsNodeSNMPPoller');
logger.info('Starting... ');


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
    logger.error(err);
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
                logger.debug("Sent " + JSON.stringify(msg));
                return ch.close();
            });
        }).finally(function () {
            conn.close();
        });
    }).catch(logger.warn);
}

function initLogger() {

    log4js.configure({
        appenders: {
            out: {type: 'console'},
            task: {
                type: 'file',
                filename: 'logs/wugmsNodeSNMPPoller.log',
                maxLogSize: 1048576,
                backups: 10
            }
        },
        categories: {
            default: {appenders: ['out', 'task'], level: 'debug'},
            task: {appenders: ['task'], level: 'error'}
        }
    });
}

function cleanValue(value, type) {
    switch (type) {
        case 6:
            return value.oid.toString().replace(/,/g, '.');
            break;
        default:
            return value;
    }
}

function getSNMP(target, service_name, stime) {
    var agents = {};
    agents['host'] = target;
    agents['rdate'] = stime;
    agents['service_name'] = service_name;
    agents['data'] = {};

    var session = new snmp.Session({host: target, port: appConfig.snmp.port, community: appConfig.snmp.community, timeouts: [10000, 10000, 10000, 10000]});
    session.getSubtree({oid: mibConfig[service_name].oid}, function (error, varbinds) {
        if (error) {
            logger.error(error);
        } else {
            var i = 1;
            logger.debug('Processing return data for ' + agents['host'] + ' rdate ' + agents['rdate']);
            varbinds.forEach(function (vb) {
                if (service_name === 'mikrotik_ap_client') {
                    var ap_id = vb.oid.pop();
                    agents['ap_id'] = ap_id;
                    var mac6 = vb.oid.pop();
                    var mac5 = vb.oid.pop();
                    var mac4 = vb.oid.pop();
                    var mac3 = vb.oid.pop();
                    var mac2 = vb.oid.pop();
                    var mac1 = vb.oid.pop();
                    var mac_add_idx = mac1 + '.' + mac2 + '.' + mac3 + '.' + mac4 + '.' + mac5 + '.' + mac6;
                    var mac_add = mac1.toString(16) + ':' + mac2.toString(16) + ':' + mac3.toString(16) + ':' + mac4.toString(16) + ':' + mac5.toString(16) + ':' + mac6.toString(16);
                    var idx = mac_add_idx;
                    var tst = vb.oid.toString().replace(/,/g, '.');
                    var o = tst.split('.');
                    var oid = o.join('.');
                    var matches = tst.match(/(1\.3\.6\.1\.4\.1\.14988\.1\.1\.1\.2\.1.[0-9])/);
                    if (typeof (agents['data'][idx]) === 'undefined')
                        agents['data'][idx] = {};
                    if ((vb.type === 4) && ((oid === '1.3.6.1.4.1.14988.1.1.1.2.1.1') || (oid === '1.3.6.1.4.1.14988.1.1.1.2.1.10'))) {
                        agents['data'][idx][mibConfig[service_name].oid2mib[tst]] = vb.valueHex.toString('hex');
                    } else {
                        agents['data'][idx][mibConfig[service_name].oid2mib[tst]] = vb.value.toString();
                    }
                    i++;
                } else {
                    var idx = vb.oid.pop();
                    var tst = vb.oid.toString().replace(/,/g, '.');
                    var o = tst.split('.');
                    var oid = o.join('.');

                    if (typeof (agents['data'][idx]) === 'undefined')
                        agents['data'][idx] = {};

                    if ((vb.type === 4) && (oid === '1.3.6.1.2.1.2.2.1.6')) {
                        agents['data'][idx][mibConfig[service_name].oid2mib[tst]] = vb.valueHex.toString('hex');
                    } else {
                        agents['data'][idx][mibConfig[service_name].oid2mib[tst]] = vb.value.toString();
                    }
                    i++;
                }
            });
            publishAMQP(appConfig.amqp.queuenamedata, agents);
        }
        session.close();
    });
}

// Consumer
open.then(function (conn) {
    return conn.createChannel();
}).then(function (ch) {
    return ch.assertQueue(appConfig.amqp.queuenamecron).then(function (ok) {
        ch.prefetch(1);
        return ch.consume(appConfig.amqp.queuenamecron, function (msg) {
            if (msg !== null) {
                strMsg = msg.content.toString();
                JSONPayload = JSON.parse(strMsg);
                nowTime = moment().unix();
                if ((nowTime >= JSONPayload.stime) && (nowTime <= JSONPayload.etime)) {
                    logger.debug('Working on  target ' + JSONPayload.target + ' mib -> ' + JSONPayload.mib + ' rdate -> ' + JSONPayload.rdate);
                    getSNMP(JSONPayload.target, JSONPayload.mib, JSONPayload.rdate);
                } else {
                    logger.debug('Too late start ' + JSONPayload.stime + ' end ' + JSONPayload.etime + ' mib ' + JSONPayload.mib + ' target ' + JSONPayload.target);
                }
                ch.ack(msg);
            }
        });
    });
}).catch(logger.warn);