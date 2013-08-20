var modeler = require('modeler')
  , hydration = require('hydration')

module.exports = function (_opts) {
  var api = modeler(_opts);

  if (!api.options.client) throw new Error('must pass a node_redis client with options.client');
  var client = api.options.client;
  var prefix = api.options.prefix + api.options.name + ':';

  function continuable (offset, limit, reverse, cb) {
    var stop = limit ? offset + limit - 1 : -1;
    (function next () {
      client[reverse ? 'ZREVRANGE' : 'ZRANGE'](prefix, offset, limit ? offset + limit - 1 : -1, function (err, chunk) {
        if (err) return cb(err);
        offset += chunk.length;
        cb(null, chunk, next);
      });
    })();
  }

  api._head = function (offset, limit, cb) {
    continuable(offset, limit, false, cb);
  };
  api._tail = function (offset, limit, cb) {
    continuable(offset, limit, true, cb);
  };
  api._save = function (entity, cb) {
    try {
      var data = hydration.dehydrate(entity);
      data = JSON.stringify(data);
    }
    catch (e) {
      return cb(e);
    }

    function add (idx) {
      client.MULTI()
        .SET(prefix + entity.id, data)
        .ZADD(prefix, idx, entity.id)
        .EXEC(function (err) {
          cb(err);
        });
    }

    if (entity.rev > 1) client.SET(prefix + entity.id, data, function (err) {
      cb(err);
    });
    else if (typeof entity.__idx === 'number') add(entity.__idx);
    else client.INCR(prefix + '_idx', function (err, idx) {
      if (err) return cb(err);
      add(idx);
    });
  };
  api._load = function (id, cb) {
    client.GET(prefix + id, function (err, data) {
      if (err) return cb(err);
      if (!data) return cb(null, null);
      try {
        var entity = JSON.parse(data);
        entity = hydration.hydrate(entity);
      }
      catch (e) {
        return cb(e);
      }
      cb(null, entity);
    });
  };
  api._destroy = function (id, cb) {
    client.MULTI()
      .DEL(prefix + id)
      .ZREM(prefix, id)
      .EXEC(cb);
  };

  return api;
};
