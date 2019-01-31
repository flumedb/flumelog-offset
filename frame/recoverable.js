'use strict'
var Looper = require('looper')
var offsetCodecs = require('./offset-codecs')

module.exports = function (blocks, blockSize, offsetCodec) {
  if (typeof offsetCodec === 'number') {
    offsetCodec = offsetCodecs[offsetCodec]
    if (!offsetCodec) throw Error('Invalid number of bits to encode file offsets. Must be one of ' + Object.keys(offsetCodecs).join(' '))
  }
  offsetCodec = offsetCodec || offsetCodecs[32]
  var fsw = offsetCodec.byteWidth

  function frame(data, start) {
    var _start = start
    var length = data.reduce(function (total, value) { return total + value.length }, 0)
    var b = Buffer.alloc(length + data.length * (8+fsw))
    var offset = 0
    for(var i = 0; i < data.length; i++) {
      var buf = data[i]
      b.writeUInt32BE(buf.length, 0 + offset) //start
      buf.copy(b, 4+offset, 0, buf.length)
      b.writeUInt32BE(buf.length, 4+buf.length + offset) //end
      offsetCodec.encode(b, start+=buf.length+(8+fsw), 8+buf.length + offset) //length of the file, if valid
      frame.offset = _start + offset
      offset += buf.length + (8 + fsw)
    }
    return b
  }

  function getMeta (offset, cb) {
    console.log('offset: ', offset)
    console.log('start: ', offset + 4)
    blocks.readUInt32BE(offset, function (err, len) {
      if(err) return cb(err)

      //read the length of the previous item.
      //unless this falls right on the overlap between
      //two blocks, this will already be in the cache,
      //so will be just a mem read.
      console.log('length: ', len)
      
      console.log('end: ', offset + 4 + len)
      blocks.readUInt32BE(offset + 4 + len, function (err, otherLen) {
        if (err) return cb(err)

        if (otherLen !== len) {
          console.log('fooked, error!')
          console.log('start value:', len)
          console.log('end value:', otherLen)
          return cb(new Error(`offset ${offset} is not a valid offset`))
        } else {
          console.log('lengths match up, valid offset!')
        }

        if(offset === 0)
          next(4, 4+len, -1, (fsw+len+8))
        else
          blocks.readUInt32BE(offset - (4 + fsw), function (err, prev_len) {
            if(err) return cb(err)
            next(offset+4, offset+4+len, offset-(prev_len+8+fsw), offset+(len+8+fsw))
          })
      })

      function next (start, end, prev, next) {
        blocks.read(start, end, function (err, value) {
          cb(err, value, prev, next)
        })
      }
    })
  }

  const overwriteMeta = (offset, cb) => {
    if (offset <= 0) {
      return cb(new Error('cannot delete first item in database'))
    }

    blocks.readUInt32BE(offset, function (err, len) {
      // merge into previous value
      blocks.readUInt32BE(offset - fsw - 4, function (err, prevLen) {
        if (err) return cb(err)

        const prevOffset = offset - fsw - 4 - prevLen - 4
        const newLength = prevLen + 4 + fsw + 4 + len
        const prevStart = prevOffset + 4
        const prevEnd = prevStart + prevLen

        blocks.read(prevStart, prevEnd, (err, prevVal) => {
          console.log({ prevVal: prevVal.toString() })
          const bufferSize = 4 + newLength + 4
          const b = Buffer.alloc(bufferSize, ' ')
          b.writeUInt32BE(newLength, 0)
          b.writeUInt32BE(newLength, bufferSize - 4)
          b.write(prevVal.toString(), 4)
          console.log('about to write: ', b)
          console.log(b.toString())
          blocks.write(b, prevOffset, (err) => {
            if (err) return cb(err)

            // XXX: why do we have to write to this twice?
            // maybe it has to do with cached blocks?
            blocks.write(Buffer.alloc(4 + fsw + len, ' '), offset - fsw - 4, (err) => {
              if (err) return cb(err)

              // XXX: a third write?! I don't get it
              const end = Buffer.alloc(4, ' ')
              end.writeUInt32BE(newLength)
              blocks.write(end, offset + len, cb)
            })
          })
        })
      })
    })
  }

  function restore (cb) {
    blocks.offset.once(function (offset) {
      if(offset === 0) return cb(null, -1) //the file is just empty!
      
      var end = offset //the very end of the file!
      var again = Looper(function () {
        offsetCodec.decodeAsync(blocks, end-fsw, function (err, _end) {
          if(_end != end) {
            if((--end) >= 0) again()
            //completely corrupted file!
            else blocks.truncate(0, next)
          }
          else {
            if(end != offset) {
              blocks.truncate(end, next)
            } else
              next()
          }
        })
      })
      again()
      function next () {
        blocks.readUInt32BE(end-(4+fsw), function (err, length) {
          if(err) cb(err)
          else cb(null, end-(length+8+fsw)) //start of the last record
        })
      }
    })
  }

  return {
    frame: frame, getMeta: getMeta, restore: restore, overwriteMeta
  }
}


