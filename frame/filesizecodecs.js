'use strict'
var uint48be = require('uint48be')

module.exports = {
  UInt32BE: {
    byteWidth: 4,
    encode: function(buf, value, offset) {
      buf.writeUInt32BE(value, offset)
    },
    decode: function(blocks, offset, cb) {
      return blocks.readUInt32BE(offset, cb) 
    }
  },
  UInt48BE: {
    byteWidth: 6,
    encode: function(buf, value, offset) {
      uint48be.encode(value, buf, offset)
    },
    decode: function(blocks, offset, cb) {
      return blocks.readUInt48BE(offset, cb) 
    }
  }
}
