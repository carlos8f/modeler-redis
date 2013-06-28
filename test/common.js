assert = require('assert');
util = require('util');
modeler = require('../');
idgen = require('idgen');

extraOptions = {
  client: require('redis').createClient(),
  prefix: idgen() + ':'
};

tearDown = function (done) {
  extraOptions.client.KEYS(extraOptions.prefix + '*', function (err, keys) {
    assert.ifError(err);
    extraOptions.client.DEL.apply(extraOptions.client, keys.concat(done));
  });
};
