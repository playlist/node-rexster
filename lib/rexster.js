var _ = require('lodash');
var util = require('util');
var EventEmitter = require('events').EventEmitter;
var Promise = require('bluebird');
var Message = require('./message');
var Commander = require('./commander');
var utils = require('./utils');
var eventHandler = require('./rexster/event_handler');
var debug = require('debug')('node-rexster:rexster');
var Connector = require('./connectors/connector');

function Rexster() {
  if (!(this instanceof Rexster)) {
    return new Rexster(arguments[0], arguments[1], arguments[2]);
  }

  EventEmitter.call(this);
  Commander.call(this);

  this.parseOptions(arguments[0], arguments[1], arguments[2]);

  this.messageQueue = [];
  this.offlineQueue = [];

  this.retryAttempts = 0;

  // end(or wait) -> connecting -> connect -> ready -> end
  if (this.options.lazyConnect) {
    this.setStatus('wait');
  } else {
    this.connect().catch(function() {});
  }
}

util.inherits(Rexster, EventEmitter);
_.extend(Rexster.prototype, Commander.prototype);

Rexster.defaultOptions = {
  // Connection
  port: 8182,
  host: 'localhost',
  family: 4,
  connectTimeout: 3000,
  retryStrategy: function(times) {
    return Math.min(times * 2, 1000);
  },
  enableOfflineQueue: true,
  enableReadyCheck: true,
  autoResubscribe: true,
  autoResendUnfulfilledCommands: true,
  lazyConnect: false
};

Rexster.prototype.parseOptions = function() {
  this.options = {};
  for (var i = 0; i < arguments.length; i++) {
    var arg = arguments[i];
    if (arg === null || typeof arg === 'undefined') {
      continue;
    }
    if (typeof arg === 'object') {
      _.defaults(this.options, arg);
    } else if (typeof arg === 'string') {
      _.defaults(this.options, utils.parseURL(arg));
    } else if (typeof arg === 'number') {
      this.options.port = arg;
    } else {
      throw new Error('Invalid argument' + arg);
    }
  }
  _.defaults(this.options, Rexster.defaultOptions);

  if (typeof this.options.port === 'string') {
    this.options.port = parseInt(this.options.port, 10);
  }
};

Rexster.prototype.setStatus = function(status, arg) {
  debug('status[%s:%s]: %s -> %s', this.options.host, this.options.port, this.status || '[empty]', status);
  this.status = status;
  process.nextTick(this.emit.bind(this, status, arg));
};

Rexster.prototype.connect = function(callback) {
  return new Promise(function(resolve, reject) {
    if (this.status === 'connecting' || this.status === 'connect' || this.status === 'ready') {
      reject(new Error('Rexster is already connecting / connected'));
      return;
    }
    this.setStatus('connecting');

    var self = this;
    this.connector.conect(function(err, stream) {
      if (err) {
        self.flushQueue(err);
        reject(err);
        return;
      }

      self.stream = stream;

      stream.once('connect', eventHandler.connectHandler(self));
      stream.once('error', eventHandler.errorHandler(self));
      stream.once('close', eventHandler.closeHandler(self));
      stream.on('data', eventHandler.dataHandler(self));

      if (self.options.connectTimeout) {
        stream.setTimeout(self.options.connectTimeout, function() {
          stream.setTimeout(0);
          stream.destroy();
        });
        stream.once('connect', function() {
          stream.setTimeout(0);
        });
      }

      var connectionConnectHandler = function() {
        self.removeListener('close', connectionCloseHandler);
        resolve();
      };
      var connectionCloseHandler = function(err) {
        self.removeListener('connect', connectionConnectHandler);
        reject(err);
      };
      self.once('connect', connectionConnectHandler);
      self.once('close', connectionCloseHandler);
    });
  }.bind(this)).nodeify(callback);
};

Rexster.prototype.disconnect = function(reconnect) {
  if (!reconnect) {
    this.manuallyClosing = true;
  }
  this.connector.disconnect();
};

Rexster.prototype.duplicate = function(override) {
  return new Rexster(_.defaults(override || {}, this.options));
};

Rexster.prototype.flushQueue = function(error) {
  var item;
  while (this.offlineQueue.length > 0) {
    item = this.offlineQueue.shift();
    item.command.reject(error);
  }

  var message;
  while (this.messageQueue.length > 0) {
    message = this.messageQueue.shift();
    message.reject(error);
  }
};

Rexster.prototype._readyCheck = function(callback) {
  // do stuff here to check the connection
  callback(null);
};

Rexster.prototype.silentEmit = function(eventName) {
  if (this.listeners(eventName).length > 0) {
    return this.emit.apply(this, arguments);
  }
  return false;
};

Rexster.prototype.sendMessage = function(message, stream) {
  if (this.status === 'wait') {
    this.connect().catch(function() {});
  }
  if (this.status === 'end') {
    message.reject(new Error('Connection is closed.'));
    return message.promise;
  }

  var writable = (this.status === 'ready');
  if (!this.stream) {
    writable = false;
  } else if (!this.stream.writable) {
    writable = false;
  } else if (this.stream._writableState && this.stream._writableState.ended) {
    // https://github.com/iojs/io.js/pull/1217
    writable = false;
  }

  if (!writable && !this.options.enableOfflineQueue) {
    message.reject(new Error('Stream is not writable and enableOfflineQueue options is false'));
    return message.promise;
  }

  if (writable) {
    debug('write message -> %s(%s)', message.name, message.args);
    (stream || this.stream).write(message.toWritable());

    this.messageQueue.push(message);
  } else if (this.options.enableOfflineQueue) {
    debug('queue message -> %s(%s)', message.name, message.args);
    this.offlineQueue.push({
      message: message,
      stream: stream
    });
  }

  return message.promise;
};

_.assign(Rexster.prototype, require('./rexster/prototype/parser'));

module.exports = Rexster;
