var Blocks = require('aligned-block-file')
var createFrame = require('./frame/basic')
var Cache = require('hashlru')
var inject = require('./inject')

module.exports = function (file, block_size, codec) {
  if('function' == typeof block_size)
    codec = block_size, block_size = 1024*16
  var blocks = Blocks(file, block_size,'a+', Cache(1024))
  return inject(
    blocks,
    createFrame(blocks, codec),
    codec,
    file
  )
}




