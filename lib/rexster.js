var _ = require('lodash');
var Queue = require('./queue');
var Socket = require('./socket');

var messages = require('./messages');

var ConnectionPool = function(options) {
  this.options = options;
  _.defaults(options, {
    graphObjName: 'g',
    username: '',
    password: '',
    timeout: null,
    poolSize: 10,
    withSession: false
  });

  if (!this.options.host) {
    throw new Error('Missing host');
  }

  if (!this.options.port) {
    throw new Error('Missing port');
  }

  if (!this.options.graphName) {
    throw new Error('Missing graphName');
  }

  this.host = this.options.host;
  this.port = this.options.port;
  this.graphName = this.options.graphName;
  this.graphObjName = this.options.graphObjName;
  this.username = this.options.username;
  this.password = this.options.password;
  this.timeout = this.options.timeout;

  this.poolSize = this.options.poolSize;
  this.pool = new Queue();
  this.size = 0;
  this.sessionKey = null;

  if (this.options.withSession) {
    // do something
  }
};

ConnectionPool.prototype.get = function() {
  var pool = this.pool;
  if (this.size >= this.poolSize || pool.qsize()) {
    return pool.get();
  }

  this.size += 1;
  var newItem;
  try {
    newItem = this._create_connection.apply(this, arguments);
  } catch (e) {
    this.size -= 1;
    throw e;
  }
  return newItem;
};

ConnectionPool.prototype.put = function(conn) {
  this.pool.put(conn);
};

ConnectionPool.prototype.closeAll = function(forceCommit) {
  while (!this.pool.empty()) {
    var conn = this.pool.getNowait();
    try {
      if (forceCommit) {
        conn.execute({
          script: 'g.stopTransaction(SUCCESS)',
          isolate: false,
          transaction: false
        });
      }
      conn.close();
    } catch (e) {
      // do nothing
    }
  }
};

ConnectionPool.prototype.connection = function() {
  var args = Array.prototype.slice(arguments);
  var transaction = args.shift();
  var callback = args.shift();

  if (transaction === undefined) {
    transaction = true;
  }

  var conn = this.createConnection.apply(this, args);
  if (!conn) {
    throw new Error('Cannot commit because connection was closed');
  }

  try {
    if (transaction) {
      return callback(conn.transaction());
    }
    return callback(conn);
  } finally {
    // close the connection
  }
};

ConnectionPool.prototype._createConnection = function(options) {
  _.defaults(options, {
    host: null,
    port: null,
    graphName: null,
    graphObjName: null,
    username: null,
    password: null,
    timeout: null,
    sessionKey: null
  });

  return new Connection(options);
};

ConnectionPool.prototype.createConnection = function() {
  var conn = this.get.apply(this, arguments);
  // if opened, soft open, else hard open
  conn.open(conn._opened);
  return conn;
};

ConnectionPool.prototype.closeConnection = function(conn, soft) {
  if (conn._opened) {
    conn.close(soft);
  }
  this.put(conn);
};

var Connection = function(options) {
  this.options = options;
  _.defaults(this.options, {
    graphObjName: 'g',
    username: '',
    password: '',
    timeout: null,
    sessionKey: null,
    poolSession: null
  });

  if (!this.options.host) {
    throw new Error('Missing host');
  }

  if (!this.options.port) {
    throw new Error('Missing port');
  }

  if (!this.options.graphName) {
    throw new Error('Missing graphName');
  }

  this.host = this.options.host;
  this.port = this.options.port;
  this.graphName = this.options.graphName;
  this.graphObjName = this.options.graphObjName;
  this.username = this.options.username;
  this.password = this.options.password;
  this.timeout = this.options.timeout;
  this._sessionKey = this.options.sessionKey;
  this.poolSession = this.options.poolSession;

  this.graphFeatures = null;
  this._conn = null;
  this._inTransaction = false;
  this._opened = false;

  this.open();
};

Connection.prototype._select = function(rlist, wlist, xlist, timeout) {
  throw new Error('Not implemented');
};

Connection.prototype._openSession = function() {
  this._conn.sendMessage(messages.SessionRequest({
    username: this.username,
    password: this.password,
    graphName: this.graphName
  }));
  var response = this._conn.getResponse();
  if (response instanceof messages.ErrorResponse) {
    response.raiseException();
  }
  this._sessionKey = response.sessionKey;

  this.graphFeatures = this.execute('g.getFeatures().toMap()');
};

Connection.prototype.openTransaction = function() {
  if (this._in_transaction) {
    throw new Error('Transaction is already open');
  }

  this.execute({
    script: 'g.stopTransaction(FAILURE)',
    isolate: false,
    transaction: false
  });
  this._inTransaction = true;
};

Connection.prototype.closeTransaction = function(success) {
  if (success === undefined) {
    success = true;
  }

  var arg;
  if (success) {
    arg = 'SUCCESS';
  } else {
    arg = 'FAILURE';
  }

  if (!this._inTransaction) {
    throw new Error('Transaction is not open');
  }

  this.execute({
    script: 'g.stopTransaction(' + arg + ')',
    isolate: false,
    transaction: false
  });
  this._inTransaction = false;
};

Connection.prototype.close = function(soft) {
  if (!this.poolSession) {
    this._conn.sendMessage(messages.SessionRequest({
      sessionKey: this._sessionKey,
      graphName: this.graphName,
      killSession: true
    }));
    var response = this._conn.getResponse();
    this._sessionKey = null;

    if (response instanceof messages.ErrorResponse) {
      response.raiseException();
    }
  }

  if (!soft) {
    this._opened = false;
  }

  this._inTransaction = false;
};

Connection.prototype.open = function(soft) {
  if (!soft || !this._opened) {
    this._conn = new Socket();
    this._conn.setTimeout(this.timeout);
    try {
      this._conn.connect(this.host, this.port);
    } catch (e) {
      throw new Error('Could not connect to database');
    }
  }

  this._inTransaction = false;

  this._opened = true;
  if (!this._sessionKey) {
    this._openSession();
  }
};

Connection.prototype.testConnection = function() {
  var self = this;
  var res = this._select([this._conn], [this._conn], [], 1);
  var readable = res[0];
  var writable = res[1];
  if (!readable && !writable) {
    var timeouts = [2, 4, 8];

    for (var i = 0; i < timeouts.length; i++) {
      var timeout = timeouts[i];
      try {
        self._conn.shutdown();
        self._conn.close();
        self._conn.connect(self.host, self.port);
        res = this._select([this._conn], [this._conn], [], timeout);
        readable = res[0];
        writable = res[1];
        if (!readable && !writable) {
          continue;
        }
        self._conn.setTimeout(self.timeout);
        self._inTransaction = false;

        if (self.poolSession) {
          self._sessionKey = self.poolSession;
        } else {
          self._sessionKey = null;
          self._openSession();
        }
        return null;
      } catch (e) {
        // ignore this
      }
    }

    throw new Error('Could not reconnect to database');
  }
};

Connection.prototype.transaction = function(cb) {
  this.testConnection();
  this.openTransaction();
  try {
    return cb(this);
  } catch (e) {
    try {
      this.closeTransaction(false);
    } catch (e) {
      this.closeTransaction(true);
    }
    throw e;
  }
};

Connection.prototype.execute = function(options) {
  _.defaults(options, {
    params: null,
    isolate: true,
    transaction: true,
    language: messages.Languages.GROOVY
  });

  if (!options.script) {
    throw new Error('Script not specified');
  }

  if (this._inTransaction) {
    options.transaction = false;
  }

  this._conn.sendMessage(messages.ScriptRequest({
    script: options.script,
    params: options.params || {},
    sessionKey: this._sessionKey,
    isolate: options.isolate,
    inTransaction: options.transaction,
    language: options.language
  }));
  var response = this._conn.getResponse();

  if (response instanceof messages.ErrorResponse) {
    response.raiseException();
  }

  return response.results;
};

































module.exports = {
  ConnectionPool: ConnectionPool
};
