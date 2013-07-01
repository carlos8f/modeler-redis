var modeler = require('modeler')
  , hydration = require('hydration')

module.exports = function (_opts) {
  var api = modeler(_opts);

  if (!api.options.client) throw new Error('must pass a node_redis client with options.client');
  var client = api.options.client;
  var prefix = api.options.prefix + api.options.name + ':';

  api._tail = function (limit, cb) {
    if (!limit) limit = -1;
    else limit--;
    client.ZREVRANGE(prefix, 0, limit, cb);
  };
  api._save = function (entity, cb) {
    try {
      var data = hydration.dehydrate(entity);
      data = JSON.stringify(data);
    }
    catch (e) {
      return cb(e);
    }

    if (entity.rev > 1) client.SET(prefix + entity.id, data, cb);
    else {
      client.INCR(prefix + '_idx', function (err, idx) {
        if (err) return cb(err);
        client.MULTI()
          .SET(prefix + entity.id, data)
          .ZADD(prefix, idx, entity.id)
          .EXEC(cb);
      });
    }
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
