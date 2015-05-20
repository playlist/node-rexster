var _ = require('lodash');
var msgpack = require('msgpack5');
var util = require('util');
var uuid = require('node-uuid');

var MessageTypes = {
  ERROR: 0,
  SESSION_REQUEST: 1,
  SESSION_RESPONSE: 2,
  SCRIPT_REQUEST: 3,
  SCRIPT_RESPONSE: 5
};

var Language = {
  GROOVY: 'groovy',
  SCALA: 'scala',
  JAVA: 'java'
};

var Message = function() {
  this.MESSAGE_TYPE = null;
};

Message.prototype.getMeta = function() {
  return {};
};

Message.prototype.getMessageList = function() {
  var uuidBytes = new Buffer();
  uuid.v1({}, uuidBytes);

  return [
    this.session,
    uuidBytes,
    this.getMeta()
  ];
};

Message.prototype.serialize = function() {
  var msg = this.getMessageList();
  var bytes = msgpack.encode(msg);

  // protocol version
  var message = new Buffer([1]);

  // serializer type (msgpack)
  message.concat(new Buffer([0]));

  // padding
  message.concat(new Buffer([0, 0, 0, 0]));

  // message type
  message.concat(new Buffer([this.MESSAGE_TYPE]));

  // message length
  message.concat(new Buffer([bytes.length]));

  message.concat(bytes);

  return message;
};

Message.deserialize = function() {
  throw new Error('Not implemented');
};

var ErrorResponseCodes = {
  INVALID_MESSAGE_ERROR: 0,
  INVALID_SESSION_ERROR: 1,
  SCRIPT_FAILURE_ERROR: 2,
  AUTH_FAILURE_ERROR: 3,
  GRAPH_CONFIG_ERROR: 4,
  CHANNEL_CONFIG_ERROR: 5,
  RESULT_SERIALIZATION_ERROR: 6
};

var ErrorResponse = function(meta, message, data) {
  Message.call(this);

  this.meta = meta;
  this.metaOrig = meta;
  if (_.isObject(this.meta)) {
    this.meta = this.meta.flag;
  }

  this.message = message;
  this.data = data;
};

util.inherits(ErrorResponse, Message);

ErrorResponse.prototype.raiseException = function() {
  switch (this.meta) {
    case ErrorResponseCodes.INVALID_MESSAGE_ERROR:
      throw new Error('Invalid message');
    case ErrorResponseCodes.INVALID_SESSION_ERROR:
      throw new Error('Invalid session');
    case ErrorResponseCodes.SCRIPT_FAILURE_ERROR:
      throw new Error('Script failure');
    case ErrorResponseCodes.AUTH_FAILURE_ERROR:
      throw new Error('Auth failure');
    case ErrorResponseCodes.GRAPH_CONFIG_ERROR:
      throw new Error('Graph config error');
    case ErrorResponseCodes.CHANNEL_CONFIG_ERROR:
      throw new Error('Channel config error');
    case ErrorResponseCodes.RESULT_SERIALIZATION_ERROR:
      throw new Error('Could not serialize result');
    default:
      throw new Error('Generic Rexster error');
  }
};

ErrorResponse.deserialize = function(data, cb) {
  var message = msgpack.decode(data);

  // var sessionKey = message[0];
  // var request = message[1];
  var meta = message[2];
  var msg = message[3];

  return cb({
    message: msg,
    meta: meta,
    data: data
  });
};

var SessionRequest = function(options) {
  Message.call(this);

  this.MESSAGE_TYPE = MessageTypes.SESSION_REQUEST;

  this.options = options;

  _.defaults(this.options, {
    graphName: null,
    graphObjName: null,
    username: '',
    password: '',
    sessionKey: null,
    killSession: false
  });
};

util.inherits(SessionRequest, Message);

SessionRequest.prototype.getMeta = function() {
  if (this.options.killSession) {
    return { killSession: true };
  }

  var meta = this.super_.prototype.getMeta.call(this);
  if (this.options.graphName) {
    meta.graphName = this.options.graphName;
    if (this.options.graphObjName) {
      meta.graphObjName = this.options.graphObjName;
    }
  }
  return meta;
};

SessionRequest.prototype.getMessageList = function() {
  var list = this.super_.prototype.getMessageList.call(this);
  list.push(this.username);
  list.push(this.password);
  if (!this.options.sessionKey) {
    list[0] = '\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00';
  }
  return list;
};

var SessionResponse = function(sessionKey, meta, languages) {
  Message.call(this);
  this.sessionKey = sessionKey;
  this.meta = meta;
  this.languages = languages;
};

util.inherits(SessionResponse, Message);

SessionResponse.deserialize = function(data, cb) {
  var message = msgpack.decode(data);

  var sessionKey = message[0];
  // var request = message[1];
  var meta = message[2];
  var languages = message[3];

  return cb({
    sessionKey: sessionKey,
    meta: meta, // bytearray_to_text
    languages: languages
  });
};

var ScriptRequest = function(script, options) {
  Message.call(this);

  this.MESSAGE_TYPE = MessageTypes.SCRIPT_REQUEST;

  this.script = script;

  this.options = options;
  _.defaults(options, {
    params: null,
    sessionKey: null,
    graphName: null,
    graphObjName: null,
    inSession: true,
    isolate: true,
    inTransaction: true,
    language: Language.GROOVY
  });
};

util.inherits(ScriptRequest, Message);

ScriptRequest.prototype.getMeta = function() {
  var meta = this.super_.prototype.getMeta.call(this);

  if (this.options.graphName) {
    meta.graphName = this.options.graphName;
    if (this.options.graphObjName) {
      meta.graphObjName = this.options.graphObjName;
    }
  }

  // defaults to false
  if (this.options.inSession) {
    meta.inSession = true;
  }

  // defaults to true
  if (!this.options.isolate) {
    meta.isolate = false;
  }

  // defaults to true
  if (!this.options.inTransaction) {
    meta.transaction = false;
  }

  return meta;
};

ScriptRequest.prototype._validateParams = function() {
  _.each(this.params, function(v, k) {
    if (k.match(/^[0-9]/)) {
      throw new Error('Parameter names cannot begin with a number');
    }

    if (k.match(/[\s.]/)) {
      throw new Error('Parameter name contains invalid characters');
    }
  });
};

ScriptRequest.prototype.serializeParameters = function() {
  var data = new Buffer();
  _.each(this.params, function(v, k) {
    var key = k;
    var val = JSON.stringify(v);
    data.append(new Buffer(key.length));
    data.append(new Buffer(key));
    data.append(new Buffer(val.length));
    data.append(new Buffer(val));
  });

  return data.toString();
};

ScriptRequest.prototype.getMessageList = function() {
  var list = this.super_.prototype.getMessageList.call(this);
  list.push(this.language);
  list.push(this.script);
  list.push(this.params);
  return list;
};

var ScriptResponse = function(results, bindings) {
  Message.call(this);
  this.results = results;
  this.bindings = bindings;
};

util.inherits(ScriptResponse, Message);

ScriptResponse.deserialize = function(data, cb) {
  var message = msgpack.decode(data);

  // var sessionKey = message[0];
  // var request = message[1];
  // var meta = message[2];
  var results = message[3];
  var bindings = message[4];

  return cb({
    results: results, // bytearray_to_text
    bindings: bindings // bytearray_to_text
  });
};

module.exports = {
  MessageTypes: MessageTypes,
  Language: Language,
  Message: Message,
  ErrorResponseCodes: ErrorResponseCodes,
  ErrorResponse: ErrorResponse,
  SessionRequest: SessionRequest,
  SessionResponse: SessionResponse,
  ScriptRequest: ScriptRequest,
  ScriptResponse: ScriptResponse
};
