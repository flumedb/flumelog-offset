
var create = require('../')
require('test-flumelog')(function () {

  return create('/tmp/test_flumelog-offset_'+Date.now(), 1024, {
  encode: function (v) {
    return new Buffer(JSON.stringify(v))
  },
  decode: function (v) {
    return JSON.parse(v)
  },
  buffer: false
})

}, function () {
  console.log('done')
})
