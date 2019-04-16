/*
 * version : 0.1.1
 */
var amqp = require('amqplib/callback_api');
var pluginLogger;
var settings = {};
var workerHandover;
var isWorkerEnabled;


function queueAMQP() {
    this.init = function (logger, extsettings, extworkerHandover) {
        pluginLogger = logger.getLogger(module.commandName);
        settings = extsettings;
        if (extworkerHandover) {
            workerHandover = extworkerHandover;
        }
        isWorkerEnabled = settings.isWorkerEnabled || process.env.isWorkerEnabled || false;
        consumeQueueName = settings.consumeQueueName || process.env.consumeQueueName || 'consumeQueueName';
        start();
    };

    this.publishMsg = function (exchange, routingKey, content) {
        publish(exchange, routingKey, new Buffer.from(content));
    };

// if the connection is closed or fails to be established at all, we will reconnect
    var amqpConn = null;
    function start() {
        amqp.connect(settings.url + "?heartbeat=60", function (err, conn) {
            if (err) {
                pluginLogger.error('Connecting error: ' + err.message);
                return setTimeout(start, 1000);
            }
            conn.on("error", function (err) {
                if (err.message !== "Connection closing") {
                    pluginLogger.error("Conn error", err.message);
                }
            });
            conn.on("close", function () {
                pluginLogger.error("Reconnecting to broker");
                return setTimeout(start, 1000);
            });
            pluginLogger.info("Connected to broker");
            amqpConn = conn;
            whenConnected();
        });
    }

    function whenConnected() {
        startPublisher();
        if (isWorkerEnabled) {
            startWorker();
        }
    }

    var pubChannel = null;
    var offlinePubQueue = [];
    function startPublisher() {
        amqpConn.createConfirmChannel(function (err, ch) {
            if (closeOnErr(err))
                return;
            ch.on("error", function (err) {
                pluginLogger.error("Publisher channel error", err.message);
            });
            ch.on("close", function () {
                pluginLogger.info("Publisher channel closed");
            });
            pubChannel = ch;
            while (true) {
                var m = offlinePubQueue.shift();
                if (!m)
                    break;
                publish(m[0], m[1], m[2]);
            }
        });
    }

// method to publish a message, will queue messages internally if the connection is down and resend later
    function publish(exchange, routingKey, content) {
        try {
            pubChannel.publish(exchange, routingKey, content, {persistent: true},
                    function (err, ok) {
                        if (err) {
                            pluginLogger.error("Publish", err);
                            offlinePubQueue.push([exchange, routingKey, content]);
                            pubChannel.connection.close();
                        }
                    });
        } catch (e) {
            pluginLogger.error("Publishing error: ", e.message);
            offlinePubQueue.push([exchange, routingKey, content]);
        }
    }

// A worker that acks messages only if processed succesfully
    function startWorker() {
        amqpConn.createChannel(function (err, ch) {
            if (closeOnErr(err))
                return;
            ch.on("error", function (err) {
                pluginLogger.error("Worker channel -> ", err.message);
            });
            ch.on("close", function () {
                pluginLogger.info("Worker channel closed");
            });
            ch.prefetch(1);
            ch.assertQueue(consumeQueueName, {durable: true}, function (err, _ok) {
                if (closeOnErr(err))
                    return;
                ch.consume(consumeQueueName, processMsg, {noAck: false});
                pluginLogger.info("Worker are started");
            });
            function processMsg(msg) {
                work(msg, function (ok) {
                    try {
                        if (ok)
                            ch.ack(msg);
                        else
                            ch.reject(msg, true);
                    } catch (e) {
                        closeOnErr(e);
                    }
                });
            }
        });
    }

    function work(msg, cb) {
        workerHandover(msg.content.toString());
        pluginLogger.debug("Handing msg over to external handler for processing");
        cb(true);
    }

    function closeOnErr(err) {
        if (!err)
            return false;
        pluginLogger.error("Closing error ", err);
        amqpConn.close();
        return true;
    }

}

module.commandName = 'faqueue';
module.exports = new queueAMQP();
module.helpText = 'amqp interface';