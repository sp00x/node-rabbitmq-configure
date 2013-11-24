# RabbitMQ Configure (rabbitmq-configure)

Automatically configure RabbitMQ exchanges, queues, bindings and other stuff based on common JSON config file on both consumer and producer ends, with role conditionals.

I'm lazy. Wrapper around the AMQP library.

## Usage

TBD

## API

### Constructor

#### ..([options])

### Methods

#### publish() 

#### connect(serverConfig, [callback])

#### declareExchange(exchangeKey, exchangeConfig, [callback])

#### declareQueue(queueKey, queueConfig, [callback])

#### declare(exchangesConfig, queuesConfig, [callback])

#### registerSubscriptionCallback(name, function)

## Configuration file format

TBD

## Requirements

* Node.js
* node-class-util
* node-logger

## License

TBD.