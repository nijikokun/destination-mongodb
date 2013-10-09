/**
 * Destination Database Adapter - Built with you in mind.
 * for MongoDB
 * 
 * @author Nijiko Yonskai
 * @copyright 2013
 * @license MIT
 */
var Mongode = require('mongode');

/**
 * Adapter Constructor
 *
 * Does adapter setup, checks, and creates a connection to the mongodb database.
 * 
 * @param  {Object} settings    Database settings
 * @param  {Object} objective   Current Destination
 * @param  {Object} destination Destination Framework
 * @return {Object}
 */
var Database = module.exports = function (settings, objective, framework) {
  if (!Mongode) framework.log.database.fatal('Missing node_module:', 'Mongode');
  var uri;

  this.framework = framework;
  this.log = framework.log.database;
  this.BSON = Mongode.BSON;
  this.collections = {};

  this.identifier = function (id) {
    return (typeof id !== 'string') ? id : Mongode.ObjectID(id);
  };
  
  if (settings.urls) {
    uri = settings.urls;
    if (framework.log.level === 'debug') this.log.debug('Opening Connections To:', uri);
    else this.log.info('Opening Connections...');
  } else {
    uri = 'mongodb://' +  (settings.username ? settings.username + ':' + settings.password + '@' : '') + settings.host + (settings.port ? ':' + settings.port : '') + '/' + (settings.database || '_test');
    if (framework.log.level === 'debug') this.log.debug('Opening Connection To:', uri);
    else this.log.info('Opening Connection...');
  }

  this.connection = Mongode.connect(uri, settings.options || {});

  return this;
};

Database.prototype.parse = function (filter) {
  if (filter.id)
    return { _id: (typeof filter.id === 'string') ? new this.ObjectID(filter.id) : filter.id };

  var query = {};
  for (var type in filter) {
    if (typeof filter[type] === 'object')
      query[type] = filter[type];
    else if (filter[type] === null)
      query[type] = { $type: 10 };
    else 
      query[type] = filter[type];
  }

  return query;
};

Database.prototype.define = function (name, object) {
  this.log.info('Storing Collection: ' + name);
  this.log.debug('Collection Model:', object);
  this.collections[name] = this.connection.collection(name);
  this.collections[name].model = object;
};

Database.prototype.create = function (collection, data, callback) {
  if (data.id === null) {
    delete data.id;
  }

  if (data.id) {
    data._id = data.id;
    delete data.id;
  }

  this.collections[collection].insert(data, {}, function (err, m) {
    callback(err, err ? null : m[0]._id);
  });
};

Database.prototype.update = function (collection, data, callback) {
  var query = {};

  if (data.where) {
    query = this.parse(data.where);
    delete data.where;
  } else query._id = (typeof data.id === 'string') ? new this.ObjectID(data.id) : data.id;

  this.collections[collection].update(query, data, function (err) {
    callback(err);
  });
};

Database.prototype.exists = function (collection, query, callback) {
  query = this.parse(query);

  this.collections[collection].findOne(query, { _id: 1 }, function (err, data) {
    callback(err, !!(data && data._id));
  });
};

Database.prototype.find = function (collection, query, projection, callback) {
  if (query.id) {
    query._id = (typeof query.id === 'string') ? new this.ObjectID(query.id) : query.id;
    delete query.id;
  } else query = this.parse(query);

  if (typeof projection === 'function')
    callback = projection, projection = {};
  else if (!projection)
    projection = {};

  this.collections[collection].findOne(query, projection, function (err, data) {
    callback(err, data);
  });
};

Database.prototype.upsert = function (model, data, callback) {
  var query = {};

  if (data.where) {
    query = this.parse(data.where);
    delete data.where;
  } else {
    if (data.id)
      query._id = (typeof data.id === 'string') ? new this.ObjectID(data.id) : data.id;
    else 
      query._id = data._id ? data._id : new this.ObjectID();

    data.id = query._id;

    if (data._id)
      delete data._id;
  }

  this.collections[collection].update(query, { $set: data }, { upsert: true, multi: false }, function (error, rowsAffected) {
    callback(error, data);
  });
};

Database.prototype.remove = function (collection, data, callback) {
  var query = {};

  if (data.where) {
    query = this.parse(data.where);
    delete data.where;
  } else 
    query._id = (typeof data.id === 'string') ? new this.ObjectID(data.id) : data.id;

  this.collections[collection].remove(query, callback);
};

Database.prototype.all = function (collection, filter, callback) {
  (!filter) && (filter = {});
  var query = filter.where ? this.parse(filter.where) : {};
  var projection = filter.projection;
  var cursor = this.collections[collection].find(query, projection);

  if (filter.order) {
    if (typeof filter.order === 'string') {
      var order = filter.order.split(",");
      filter.order = {};

      for (var index = 0; index < order.length; index++) {
        var set = order[index].split(" ").filter(function (n) { 
          return n; 
        });

        if (!(set[0] && set[1])) throw new Error(
          "Invalid ordering set."
        );

        filter.order[set[0]] = filter.order[set[1]];
      }
    }

    for (var key in filter.order) {
      if (!filter.order.hasOwnProperty(key)) return;
      var value = filter.order[key];
      var match = value.match(/\s+(A|DE)SC$/);

      filter.order[key] = (match && match[1] === 'DE') ? -1 : 1;
    }

    cursor.sort(filter.order);
  }

  if (filter.limit)
    cursor.limit(filter.limit);

  if (filter.skip)
    cursor.skip(filter.skip);
  else if (filter.offset)
    cursor.skip(filter.offset);

  cursor.toArray(function (error, data) {
    if (error) return callback(error);

    var output = data.map(function (o) { 
      o.id = o._id;
      return o; 
    });

    if (filter && filter.include) {
      var keys = Object.keys(filter.include);
      for (var index = 0; index < keys.length; index++) {
        var includeCollection = keys[index];
        var includeFilter = filter.include[includeCollection];
        var last = index === keys.length - 1;

        this.all(includeCollection, includeFilter, function (error, data) {
          output[includeCollection] = data;

          if (last) callback(null, output);
        });
      }
    } else callback(null, output);
  });
};

Database.prototype.empty = function (collection, callback) {
  this.collections[collection].remove({}, callback);
};

Database.prototype.count = function (collection, where, callback) {
  if (typeof where === 'function')
    callback = where, where = undefined;

  this.collections[collection].count(where, function (error, count) {
    callback(error, count);
  });
};

Database.prototype.updateAttributes = function (model, query, data, cb) {
  var query = this.parse(query);

  this.collection(model).findAndModify(query, [['_id','asc']], { $set: data }, {}, function (error, object) {
    cb(error, object);
  });
};

Database.prototype.close = function () {
  this.client.close();
};