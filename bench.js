var FlumeLog = require('./')
var codec = require('flumecodec')

require('../bench-flumelog/appendy')(function () {
  return FlumeLog('/tmp/bench-flumelog-offset'+Date.now(), 1024*4, codec.json)
})

