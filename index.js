var Blocks = require('aligned-block-file')
var createFrame = require('./frame/recoverable')
var filesizeCodecs = require('./frame/filesizecodecs.js')
var Cache = require('hashlru')
var inject = require('./inject')
function id (e) { return e }
function isNumber(n) { return 'number' == typeof n && !isNaN(n) }

module.exports = function (file, opts) {
  if (!opts) opts = {}
  //file, blocks, frame, codec
  if (typeof opts !== 'object') legacy.apply(null, arguments)

  var blockSize = opts.blockSize || 1024*16
  var codec = opts.codec || {encode: id, decode: id}
  var flags = opts.flags || 'a+'
  var cache = opts.cache || Cache(1024)
  var filesizeCodec = opts.filesizeCodec || filesizeCodecs.UInt32BE

  var blocks = Blocks(file, blockSize, flags, cache)
  var frame = createFrame(blocks, blockSize, filesizeCodec)
  return inject(blocks, frame, codec, file)
}

var warned = false
var msg = 'flumelog-offset: blockSize and codec params moved into an object. https://github.com/flumedb/flumelog-offset'
function legacy (blockSize, codec) {
  if (!warned) warned = true, console.warn(msg)
  if (!isNumber(blockSize)) codec = blockSize, blockSize = undefined
  return {blockSize: blockSize, codec: codec}
}

