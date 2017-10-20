var FlumeLog = require('./')
var codec = require('flumecodec')

require('bench-flumelog')(function () {
  return FlumeLog('/tmp/bench-flumelog-offset' + Date.now(), {
    blockSize: 1024*4,
    codec: codec.json
  })
})

