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

var ErrorResponse = function(meta, message) {
  Message.call(this);
  this.meta = meta;
  this.message = message;
};

util.inherits(ErrorResponse, Message);

ErrorResponse.deserialize = function(data, cb) {
  var message = msgpack.decode(data);
  return cb(message, this.meta);
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

  var meta = {};
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
    meta: meta,
    languages: languages
  });
};

var ScriptRequest = function(script, options) {
  Message.call(this);

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
  var meta = {};

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

module.exports = {
  MessageTypes: MessageTypes,
  Language: Language,
  Message: Message,
  ErrorResponse: ErrorResponse,
  SessionRequest: SessionRequest,
  SessionResponse: SessionResponse,
  ScriptRequest: ScriptRequest,
  ScriptResponse: ScriptResponse
};




































//
