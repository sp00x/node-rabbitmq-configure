var
    Events = require('events'),
    Util = require('util');

// Dependencies: NPM modules
var
    Q = require('q'), // promises lib
    AMQP = require('amqp'); // amqp/rabbitmq client

//var Logger = require('logger');

function cloneObject(o)
{
    return JSON.parse(JSON.stringify(o));
}

function NullLogger()
{
    this.trace = function(){};
    this.debug = function(){};
    this.info = function(){};
    this.warning = function(){};
    this.error = function(){}
    this.contextualize = function() { return this; }
}

function RabbitMQ(options)
{
    if (typeof options != 'object') options = {};
    Events.EventEmitter.call(this); // call inherited constructor of EventEmitter
    this.logger = (options.logger === null) ? new NullLogger() : (options.logger || new NullLogger());
    this.role = options.role || null;
    this.logPrefix = options.logPrefix || "";
    this.connection = null;
    this.subscriptionFunctions = {};
    this.exchanges = {};
    this.queues = {};
    this.exchangePrefix = options.exchangePrefix || options.prefix || "";
    this.exchangeSuffix = options.exchangeSuffix || options.suffix || "";
    this.queuePrefix = options.queuePrefix || options.prefix || "";
    this.queueSuffix = options.queueSuffix || options.suffix || "";
    
    this.subs = [];

    // bind all __ prefixed functions to this(this,..)
    for (var fn in this)
    {
        if (typeof this[fn] == 'function' && fn.match(/^__/))
            this[fn.substring(2)] = this[fn].bind(this, this);
    }
}

Util.inherits(RabbitMQ, Events.EventEmitter);

RabbitMQ.prototype.__checkRole = function (me, roles)
{
    if (roles == null) return true; // null/undefined = all
    if (typeof roles == 'string') return roles == me.role; // '' = false, unless someone has '' as role..
    for (var i = 0; roles.length > i; i++)
        if (roles[i] == me.role) return true;
    return false; // not in array, or array was blank = disabled
}

RabbitMQ.prototype.__error = function (me, callback, error)
{
    if (typeof error == 'string')
        error = new Error(error);

    if (typeof callback == 'function')
        callback(error);

    return error;
}

RabbitMQ.prototype.__ok = function (me, callback, args)
{
    if (typeof callback == 'function')
    {
        callback(null); // TODO add $args here
    }
    return undefined;
}

RabbitMQ.prototype.__publish = function (me, exchangeName, topic, message, options, confirmCallback)
{
    if (typeof options == 'function')
    {
        confirmCallback = options;
        options = {};
    }
    me.exchanges[me.expandExchangeName(exchangeName)].exchange.publish(topic, message, options, confirmCallback);
}

RabbitMQ.prototype.__connect = function(me, config, callback)
{
    var log = (me.logger.contextualize) ? me.logger.contextualize("connect") : me.logger;
    if (typeof callback != 'function') callback = null;

    var onError = function (err)
    {
        log.error("Error while connecting:", err.message, "\n", err.stack);
    }

    // connect
    log.info("Connecting..");
    var c = me.connection = AMQP.createConnection(config);
    c.on('error', function (err)
    {
        me.logger.error('Error event: ', err);
    });

    // set up handlers for events
    c.on('error', onError);
    c.once('ready', function ()
    {
        log.info("Connected");
        c.removeListener('error', onError);
        return me.ok(callback);
    });
}

RabbitMQ.prototype.__declareExchange = function(me, exchangeName, exchangeConfig, callback)
{
    try
    {
        var log = (me.logger.contextualize) ? me.logger.contextualize("declareExchange") : me.logger;

        exchangeName = me.expandExchangeName(exchangeName);

        // prepare
        var roles = exchangeConfig.roles;
        exchangeConfig = cloneObject(exchangeConfig);
        delete exchangeConfig.name;
        delete exchangeConfig.roles;
        me.exchanges[exchangeName] = { name: exchangeName, config: exchangeConfig, roles: roles, exchange: null };

        if (!me.checkRole(roles))
            return me.ok(callback);

        log.info("Declaring exchange '" + exchangeName + "'..");
        var ex = me.connection.exchange(exchangeName, exchangeConfig, function(exchange)
        {
            if (exchange == null)
            {
                log.error("Failed to declare exchange '" + exchangeName + "' (unknown reason)");
                return me.error(callback, "Failed to declare exchange: " + exchangeName);
            }
            else
            {
                log.info("Exchange '" + exchangeName + "' declared");
                return me.ok(callback);
            }
        });
        me.exchanges[exchangeName].exchange = ex;
    }
    catch (err)
    {
        return me.error(callback, err);
    }
}

RabbitMQ.prototype.__expandExchangeName = function(me, exchangeName)
{
    return me.exchangePrefix + exchangeName + me.exchangeSuffix;
}

RabbitMQ.prototype.__expandQueueName = function(me, queueName)
{
    if (queueName != '' && queueName != null)
        queueName = me.queuePrefix + queueName + me.queueSuffix;
    return queueName;
}

RabbitMQ.prototype.__declareQueue = function (me, queueName, queueConfig, callback)
{
    var log = (me.logger.contextualize) ? me.logger.contextualize("declareQueue") : me.logger;
    if (typeof callback != 'function') callback = undefined;

    queueName = me.expandQueueName(queueName);

    // prepare
    var declareQueueName = queueName;
    var roles = queueConfig.roles;
    var bindings = queueConfig.bindings;
    queueConfig = cloneObject(queueConfig);
    if (queueConfig.name !== undefined)
    {
        declareQueueName = queueConfig.name || '';
    }
    delete queueConfig.name;
    delete queueConfig.isServer;
    delete queueConfig.bindings;
    delete queueConfig.roles;
    me.queues[queueName] = { name: queueName, roles: roles, bindings: bindings, config: queueConfig, queue: null };

    if (!me.checkRole(roles))
        return me.ok(callback);

    // declare the queue
    if (declareQueueName != queueName)
        log.info("Declaring anonymous queue '" + queueName + "'..");
    else
        log.info("Declaring queue '" + queueName + "'..");

    var q = me.connection.queue(declareQueueName, queueConfig, function (queue, dunno1, dunno2) // OR: q.on('queueDeclareOk', function(queue)
    {
        if (queue == null)
            return me.error(callback, "Failed to declare queue: '" + queueName + "'");

        me.queues[queueName].actualName = queue.name;
        if (declareQueueName != queueName)
        {
            log.info("Queue '" + queueName + "' declared as '" + queue.name + "'");
        }
        else
        {
            log.info("Queue '" + queueName + "' declared");
        }

        // bind the queue?
        if (bindings != null && bindings instanceof Array && bindings.length > 0 && me.checkRole(bindings[0].roles))
        {
            var binding = bindings[0];

            var exchangeName = me.expandExchangeName(binding.exchange);
            log.info("Binding queue '" + queueName + "' to exchange '" + exchangeName + "'..");
            q.bind(exchangeName, binding.topic, function (queue)
            {
                if (queue)
                {
                    log.info("Bound queue '" + queueName + "' to exchange '" + exchangeName + "'");
                    var sub = (binding.subscriptions && binding.subscriptions.length > 0) ? binding.subscriptions[0] : null;
                    if (sub && me.checkRole(sub.roles))
                    {
                        me.subs.push({ queue: q, queueName: queueName, sub: sub });
                        me.ok(callback);
                    }
                    else
                        return me.ok(callback);
                }
                else
                    return me.error(callback, "Error binding queue '" + queueName + "' to exchange '" + binding.exchange + "'");
            });
        }
        else
        {
            // no bindings, we're done
            return me.ok(callback);
        }
    });
    me.queues[queueName].queue = q;

}

RabbitMQ.prototype.__registerSubscriptionCallback = function(me, name, func)
{
    me.subscriptionFunctions[name] = func;
}

RabbitMQ.prototype.__declare = function(me, exchanges, queues, callback)
{
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
            return me.ok(callback);
        },
        function (err)
        {
            return me.error(callback, err);
        }
    );
}

RabbitMQ.prototype.__subscribe = function (me, callback)
{
    var log = (me.logger.contextualize) ? me.logger.contextualize("subscribe") : me.logger;
    
    log.info("Setting up subscriptions..");

    function next()
    {
        var j = me.subs.shift();
        if (j == null)
        {
            log.info("All subscriptions set up");
            return callback(null);
        }
        
        var q = j.queue;
        var queueName = j.queueName;
        var sub = j.sub;

        log.info("Subscribing to queue '" + queueName + "'..");
        q.subscribe(sub.options, function (message, headers, deliveryInfo, messageObject)
        {
            var acked = false;
            var ack = function ()
            {
                if (!acked && sub.options && sub.options.ack)
                {
                    log.debug("ACKing received message");
                    q.shift();
                    acked = true;
                }
                else
                {
                    log.debug("Nothing to ack");
                }
            }
            
            var fn = me.subscriptionFunctions[sub.function];
            if (typeof fn == 'function')
            {
                log.debug("Got message, dispatching to function '" + sub.function + "'..");
                try
                {
                    fn(message, function (err)
                    {
                        if (err) log.error("Function failed processing message: " + err);
                        else log.info("Function finished processing message");
                        ack();
                    });
                }
                catch (err)
                {
                    log.error("Error in function '" + sub.function + "'", err);
                    ack();
                }
            }
            else
            {
                log.warning("Got message, but no function to dispatch to??");
                ack();
            }
        });
        
        q.once('basicQosOk', function (a, b, c, d)
        {
            log.info("basicQosOk", arguments);
            setImmediate(next);
        });
    }
    
    next();
     
    return deferred.promise;
}

module.exports = RabbitMQ;

