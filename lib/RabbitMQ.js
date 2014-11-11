var
    Events = require('events'),
    Util = require('util');

// Dependencies: NPM modules
var
    Q = require('q'), // promises lib
    Async = require('async'); // async
    AMQP = require('amqp'); // amqp/rabbitmq client

//var Logger = require('logger');

function cloneObject(o)
{
    return JSON.parse(JSON.stringify(o));
}

/**
 * Null logger
 *
 * @constructor
 */
function NullLogger()
{
    this.debug = function() {};
    this.info = function() {};
    this.warn = function() {};
    this.error = function() {};
    this.contextualize = function() { return this; }
}

/**
 * Bind all functions to a special object via a proxy
 *
 * @param {object} object
 * @param {object} [boundThis]
 * @returns {object}
 */
function bindFunctionsViaProxy(object, boundThis)
{
    if (boundThis == null) boundThis = object;

    var old = {};

    // wrap all methods in proxies that call them with the
    // correct "this"
    for (var fn in object)
    {
        if (typeof object[fn] == 'function')
        {
            old[fn] = object[fn];
            (function (fn)
            {
                var orig = object[fn];
                var proxy = function()
                {
                    return orig.apply(boundThis, arguments);
                };
                object[fn] = proxy;
            })(fn);

            //$this[fn.substring(2)] = $this[fn].bind($this, $this);
        }
    }
    return old;
}

/**
 * Automatic RabbitMQ configuration helper
 *
 * @param {object} options
 * @constructor
 */
function RabbitMQ(options)
{
    var $this = this;

    if (typeof options != 'object') options = {};

    Events.EventEmitter.call($this); // call inherited constructor of EventEmitter

    $this.logger = (options.logger == null) ? new NullLogger() : (options.logger || new NullLogger());

    // import roles
    var roles = [];
    if (options.roles instanceof Array) roles = roles.concat(options.roles);
    if (typeof options.role == 'string') roles.push(options.role);

    // translate roles array into hash for faster lookup
    $this.roles = {};
    roles.forEach(function(role)
    {
        $this.roles[role] = true;
    });

    $this.logPrefix = options.logPrefix || "";
    $this.connection = null;
    $this.exchanges = {};
    $this.queues = {};
    $this.exchangePrefix = options.exchangePrefix || options.prefix || "";
    $this.exchangeSuffix = options.exchangeSuffix || options.suffix || "";
    $this.queuePrefix = options.queuePrefix || options.prefix || "";
    $this.queueSuffix = options.queueSuffix || options.suffix || "";

    bindFunctionsViaProxy($this, $this);
}

Util.inherits(RabbitMQ, Events.EventEmitter);

/**
 * Check if our roles match something in a supplied list of roles
 *
 * @param roles {Array} List of roles to test for (any)
 * @returns {boolean}
 * @private
 */
RabbitMQ.prototype._checkRole = function (roles)
{
    var me = this;

    if (roles == null) return true; // null/undefined = all

    for (var i = 0; roles.length > i; i++)
        if (me.roles[roles[i]] === true) return true;

    return false; // not in array, or array was blank = disabled
};

/**
 * Handle error callback
 *
 * @param {function} callback
 * @param {Error|string} error
 * @returns {Error}
 * @private
 */
RabbitMQ.prototype._error = function (callback, error)
{
    var me = this;

    if (typeof error == 'string')
        error = new Error(error);

    if (typeof callback == 'function')
        callback(error);

    return error;
};

/**
 * Handle success/ok callback
 *
 * @param {function} callback
 * @param {Array} [args]
 * @returns {undefined}
 * @private
 */
RabbitMQ.prototype._ok = function (callback, args)
{
    var me = this;

    if (typeof callback == 'function')
    {
        if (args != null && args instanceof Array)
        {
            var stuff = [null].concat(args);
            callback.apply(me, stuff);
        }
        else
        {
            callback(null);
        }
    }
    return undefined;
};

/**
 * Publish a message to some exchange
 *
 * @param {string} exchangeName - the name of the exchange
 * @param {string} routingKey - routing key for exchange
 * @param {object} message - the message
 * @param {object} [options] - Options
 * @param {function} [confirmCallback]
 */
RabbitMQ.prototype.publish = function (exchangeName, routingKey, message, options, confirmCallback)
{
    var me = this;

    if (typeof options == 'function')
    {
        confirmCallback = options;
        options = {};
    }
    me.exchanges[me.expandExchangeName(exchangeName)].exchange.publish(routingKey, message, options, confirmCallback);
};

/**
 * Connect to RabbitMQ
 *
 * @param {object} config
 * @param {function} callback
 */
RabbitMQ.prototype.connect = function(config, callback)
{
    var me = this;

    var log = (me.logger.contextualize) ? me.logger.contextualize("connect") : me.logger;
    if (typeof callback != 'function') callback = null;

    var onError = function (err)
    {
        log.error("Error while connecting:", err.message, "\n", err.stack);
    };

    // connect
    log.info("Connecting..");
    var c = me.connection = AMQP.createConnection(config);

    // 2 error event handlers are set up: one is just to handle during initial connect
    c.on('error', function (err)
    {
       log.error('Error event: ', err);
    });

    // set up handlers for events
    c.on('error', onError);

    c.once('ready', function ()
    {
        log.info("Connected");
        c.removeListener('error', onError);
        return me._ok(callback);
    });
};

/**
 * Declare an exchange
 *
 * @param {string} exchangeName
 * @param {object} exchangeConfig
 * @param {function} callback
 * @returns {*}
 */
RabbitMQ.prototype.declareExchange = function(exchangeName, exchangeConfig, callback)
{
    var me = this;

    try
    {
        var log = (me.logger.contextualize) ? me.logger.contextualize("declareExchange") : me.logger;

        exchangeName = me.expandExchangeName(exchangeName);

        // allow silent re-declaring of exchanges
        if (me.exchanges[exchangeName] != null)
            return me._ok(callback);

        // prepare
        var roles = exchangeConfig.roles;
        var options = cloneObject(exchangeConfig.options);
        me.exchanges[exchangeName] = { name: exchangeName, options: options, roles: roles, exchange: null };

        if (!me._checkRole(roles))
            return me._ok(callback);

        log.info("Declaring exchange '" + exchangeName + "'..");
        var ex = me.connection.exchange(exchangeName, options, function(exchange)
        {
            if (exchange == null)
            {
                log.error("Failed to declare exchange '" + exchangeName + "' (unknown reason)");
                return me._error(callback, "Failed to declare exchange: " + exchangeName);
            }
            else
            {
                log.info("Exchange '" + exchangeName + "' declared");
                return me._ok(callback);
            }
        });
        me.exchanges[exchangeName].exchange = ex;
    }
    catch (err)
    {
        return me._error(callback, err);
    }
}

/**
 * Expand an exchange name using the configured prefix and suffix
 *
 * @param {string} exchangeName
 * @returns {string}
 */
RabbitMQ.prototype.expandExchangeName = function(exchangeName)
{
    var me = this;

    return me.exchangePrefix + exchangeName + me.exchangeSuffix;
};

/**
 * Expand a queue name using the configured prefix and suffix
 * @param queueName
 * @returns {*}
 */
RabbitMQ.prototype.expandQueueName = function(queueName)
{
    var me = this;

    if (queueName != '' && queueName != null)
        queueName = me.queuePrefix + queueName + me.queueSuffix;
    return queueName;
};

/**
 * Bind a queue to an exchange
 *
 * @param {string} queueName - The queue name
 * @param {function} callback
 * @returns {*}
 */
RabbitMQ.prototype.bindQueue = function (queueName, callback)
{
    var me = this;

    var log = (me.logger.contextualize) ? me.logger.contextualize("bindQueues") : me.logger;
    if (typeof callback != 'function') callback = undefined;

    var origQueueName = queueName;
    queueName = me.expandQueueName(queueName);

    var queueInfo = me.queues[queueName];
    if (queueInfo == null)
        return me._error(callback, "Can't process bindings for undeclared queue '" + queueName + "'");

    var roles = queueInfo.roles;
    var bindings = queueInfo.bindings;

    // bind the queue?
    if (bindings != null && bindings instanceof Array && bindings.length > 0 && me._checkRole(bindings[0].roles))
    {
        // XXX NOTE: THIS ONLY SUPPORTS ONE BINDING PER QUEUE!!
        var binding = bindings[0];

        // if this binding has already been processed, we're done.
        if (binding.processed === true)
        {
            // TODO XXX CHECK SUBS
            return me._ok(callback);
        }

        var exchangeName = me.expandExchangeName(binding.exchange);
        log.info("Binding queue '" + queueName + "' to exchange '" + exchangeName + "'..");
        queueInfo.queue.bind(exchangeName, binding.topic, function (queue)
        {
            if (queue)
            {
                binding.processed = true;

                log.info("Bound queue '" + queueName + "' to exchange '" + exchangeName + "'");

                // check for subscriptions
                var sub = (binding.subscriptions && binding.subscriptions.length > 0) ? binding.subscriptions[0] : null;
                if (sub && me._checkRole(sub.roles) && sub.processed !== true)
                {
                    sub.processed = true;

                    // XXX NOTE: THIS ONLY SUPPORTS FOR SUBSCRIPTION PER BINDING!!

                    // queue the subscription for later
                    me.subs.push({ queue: queueInfo.queue, queueName: queueName, sub: sub });
                }
                return me._ok(callback);
            }
            else
                return me._error(callback, "Error binding queue '" + queueName + "' to exchange '" + binding.exchange + "'");
        });
    }
    else
    {
        // no bindings, we're done
        return me._ok(callback);
    }
};

/**
 * Declare a queue
 *
 * @param {string} queueName
 * @param {object} queueConfig
 * @param {function} callback
 * @returns {*}
 */
RabbitMQ.prototype.declareQueue = function (queueName, queueConfig, callback)
{
    var me = this;

    var log = (me.logger.contextualize) ? me.logger.contextualize("declareQueue") : me.logger;
    if (typeof callback != 'function') callback = undefined;

    var origQueueName = queueName;
    queueName = me.expandQueueName(queueName);

    // if the queue has already been declared, skip that part and jump straight
    // to bindings
    if (me.queues[queueName])
    {
        return me.bindQueue(origQueueName, callback);
    }

    // prepare
    var declareQueueName = queueName;
    var roles = cloneObject(queueConfig.roles);
    var bindings = cloneObject(queueConfig.bindings);
    var sub = cloneObject(queueConfig.subscription);
    var options = cloneObject(queueConfig.options || {});

    // make a copy of the queue configuration and just keep what can be passed to the AMQP lib methods
    queueConfig = cloneObject(queueConfig);

    if (queueConfig.name !== undefined)
        declareQueueName = queueConfig.name || '';

    // save the queue

    me.queues[queueName] = {
        name: queueName,
        declaredName: declareQueueName,
        roles: roles,
        options: options,
        bindings: bindings,
        queue: null,
        subscription: sub
    };

    if (!me._checkRole(roles))
        return me._ok(callback);

    // declare the queue
    if (declareQueueName != queueName)
        log.info("Declaring anonymous queue '" + queueName + "'..");
    else
        log.info("Declaring queue '" + queueName + "'..");

    var q = me.queues[queueName].queue = me.connection.queue(declareQueueName, options, function (queue, dunno1, dunno2) // OR: q.on('queueDeclareOk', function(queue)
    {
        if (queue == null)
            return me._error(callback, "Failed to declare queue: '" + queueName + "'");

        me.queues[queueName].actualName = queue.name;
        if (declareQueueName != queueName)
        {
            log.info("Queue '" + queueName + "' declared as '" + queue.name + "'");
        }
        else
        {
            log.info("Queue '" + queueName + "' declared");
        }

        // process bindings
        me.bindQueue(origQueueName, callback);
    });
};

/**
 * Declare exchanges and queues
 *
 * @param {object} exchanges
 * @param {object} queues
 * @param {function} callback
 */
RabbitMQ.prototype.declare = function(exchanges, queues, callback)
{
    var me = this;

    var chain = [];

    for (var name in exchanges)
    {
        var config = exchanges[name];
        (function (name, config)
        {
            chain.push(function() { return Q.nfcall(me.declareExchange, name, config) });
        })(name, config);
    }

    for (var name in queues)
    {
        var config = queues[name];
        (function (name, config)
        {
            chain.push(function () { return Q.nfcall(me.declareQueue, name, config) });
        })(name, config);
    }

    chain.reduce(Q.when, Q()).then(
        function (statuses)
        {
            return me._ok(callback);
        },
        function (err)
        {
            return me._error(callback, err);
        }
    );
};


RabbitMQ.prototype.subscribeMultiple = function(queuesAndCallbacks, configuredCallback)
{
    var me = this;

    // translate to array so we can work through it easier
    var subs = Object.keys(queuesAndCallbacks).map(function(queueName)
    {
        return function(cb)
        {
            me.subscribe(queueName, queuesAndCallbacks[queueName], cb);
        }
    });

    // this could have been done in parallel, but i don't really think the
    // basicQosOk event emitted by the amqp lib handles many simultaneous
    // ongoing subs..?
    Async.series(subs, function(err, results)
    {
        return configuredCallback(err, results);
    })
};

/**
 * Subscribe to a queue
 *
 * @param {string} queueName
 * @param {function} subscribeCallback
 * @param {function} configuredCallback
 */
RabbitMQ.prototype.subscribe = function (queueName, subscribeCallback, configuredCallback)
{
    var me = this;

    if (typeof queueName == 'object' && typeof subscribeCallback == 'function' && configuredCallback == null)
        return me.subscribeMultiple(queueName, subscribeCallback);

    var log = (me.logger.contextualize) ? me.logger.contextualize("subscribe:" + queueName) : me.logger;

    var expandedQueueName = me.expandQueueName(queueName);
    var q = me.queues[expandedQueueName];
    var prefix = "(" + queueName + ") ";
    var options = q.subscription || {};

    log.info(prefix + "Setting up subscription..");
    q.queue.subscribe(options, function (message, headers, deliveryInfo, messageObject)
    {
        // convert the binary tag to a number
        var tag = deliveryInfo.deliveryTag;
        if (tag instanceof Buffer && tag.length == 8)
        {
            var hi = tag.readUInt32BE(0);
            var lo = tag.readUInt32BE(4);
            tag = (hi << 32) || lo;
        }

        // handle acknowledging
        var acked = false;
        var ack = function ()
        {
            if (!acked && options.ack)
            {
                log.debug(prefix + "ACKing received message (" + tag + ")");
                messageObject.acknowledge(); //q.shift();  <-- only works if prefetchCount = 1 !!! DUH!
                acked = true;
            }
            else
            {
                log.debug("Nothing to ACK (" + tag + ")");
            }
        };

        // dispatch!
        log.debug(prefix + "Got message (" + tag + "), dispatching..");
        try
        {
            subscribeCallback(message, function (err)
            {
                if (err) log.error(prefix + "Function failed processing message (" + tag + "): " + err);
                else log.debug(prefix + "Function finished processing message (" + tag + ")");
                ack();
            });
        }
        catch (err)
        {
            log.error(prefix + "Exception during function dispatch (" + tag + "): " + err.toString() + ": " + err.stack);
            ack();
        }
    });

    // TODO FIXME  If no ack, then this will never be called:

    q.queue.once('basicQosOk', function (a, b, c, d)
    {
        log.info(prefix + "Subscribed", arguments);
        configuredCallback(null);
    });
};

module.exports = RabbitMQ;
