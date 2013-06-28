var modeler = require('modeler')
  , hydration = require('hydration')

module.exports = function (_opts) {
  var api = modeler(_opts);

  if (!api.options.client) throw new Error('must pass a node_redis client with options.client');
  var client = api.options.client;
  var prefix = api.options.prefix + api.options.name + ':';
  api.options.scoreFn || (api.options.scoreFn = function (entity) {
    return entity.created.getTime();
  });

  api._list = function (options, cb) {
    var method = options.reverse ? 'ZREVRANGE' : 'ZRANGE';
    var start = options.start || 0;
    var stop = typeof options.stop === 'undefined' ? -1 : options.stop - 1;
    client[method](prefix, start, stop, cb);
  };
  api._save = function (entity, cb) {
    try {
      var data = hydration.dehydrate(entity);
      data = JSON.stringify(data);
    }
    catch (e) {
      return cb(e);
    }
    client.MULTI()
      .SET(prefix + entity.id, data)
      .ZADD(prefix, api.options.scoreFn(entity), entity.id)
      .EXEC(function (err) {
        if (err) return cb(err);
        cb();
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
