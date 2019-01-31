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

  const previousLength = (offset, cb) =>
    blocks.readUInt32BE(offset - (4+fsw), function (err, prevLen) {
      if(err) return cb(err)
      cb(null, prevLen)
    })

  const previousOffset = (offset, cb) => 
    previousLength(offset, (err, prevLength) => {
      if (err) return cb(err)
      const prevOffset = previousOffsetFromPreviousLength(offset, prevLength)
      console.log('previousOffset', prevOffset)
      cb(null, prevOffset || -1)
    })

  const nextOffset = (offset, cb) => 
    blocks.readUInt32BE(offset, function (err, length) {
      cb(null, offset + 4 + length + 4 + fsw)
    })

  const previousOffsetFromPreviousLength = (offset, prevLen) => offset - (prevLen + 8 + fsw)

  const previousValueFromOffset = (offset, cb) =>
    previousLength(offset, (err, prevLen) => {
      if (err) return cb(err)
      const prevOffset = previousOffsetFromPreviousLength(offset, prevLen)
      blocks.read(prevOffset, cb)
    })

  const nextOffsetFromCurrentLength = (offset, len) => offset + (len + 8 + fsw)

  const offsetFromStart = (offset) => offset - 4

  const currentStart = (offset) => offset + 4

  const currentValue = (offset, cb) => {
    console.log('currentValue', { offset })
    blocks.readUInt32BE(offset, (err, len) => {
      if (err) return cb(err)
      console.log('currentValue', { len })
      blocks.read(currentStart(offset), len, cb)
    })
  }

  const currentEndFromLength = (offset, length) => offset + length + 4

  const isEmpty = (buf) => buf.equals(Buffer.alloc(buf.length))

  function getMetaStream (offset, cb) {
    blocks.readUInt32BE(offset, function (err, length) {
      if(err) return cb(err)

      // read the length of the previous item.
      //unless this falls right on the overlap between
      //two blocks, this will already be in the cache,
      //so will be just a mem read.
      if(offset === 0) {
        handle(4, 4 + length, -1, fsw + length + 8)
      } else {
        previousLength(offset, function (err, prevLen) {
          if(err) return cb(err)

          handle(
            currentStart(offset),
            currentEndFromLength(offset, length),
            previousOffsetFromPreviousLength(offset, prevLen),
            nextOffsetFromCurrentLength(offset, length)
          )
        })
      }

      function handle (start, end, prev, next) {
        console.log('handle', { originalOffset: offset, start, end, prev, next })

        if (prev === undefined) {
          console.trace('wtf')
        }

        if (start === 4) {
          prev = -1
        }

        if (start === undefined) {
          // XXX: why is prev undefined?!
          prev = 0
        }

        blocks.read(start, end, function (err, value) {
          if (isEmpty(value)) {
            value = undefined
          }

          if (prev !== -1) {
            // not the first element, check prev and next
            console.log({ prev })
            currentValue(prev, function (err, prevValue) {
              if (err) return cb(err)

              if (isEmpty(prevValue)) {
                // previous is empty, retry handle with prevprev
                previousOffset(prev, (err, prevPrev) => {
                  handle(start, end, prevPrev, next)
                })
              } else {
                // previous is fine, check next
                currentValue(next, function (err, nextValue) {
                  if (err) return cb(err)

                  if (nextValue !== undefined && isEmpty(nextValue)) {
                    // next is empty, retry handle with nextnext
                    nextOffset(next, (err, nextNext) => {
                      handle(start, end, prev, nextNext)
                    })
                  } else {
                    // next is fine
                    cb(err, value, prev, next)
                  }
                })
              }
            })
          } else {
            // previous is beginning of file, check next
            currentValue(next, function (err, nextValue) {
              if (err) return cb(err)

              console.log('nextValue', nextValue)
              console.log('nextValue.length', nextValue.length)

              if (nextValue !== undefined && nextValue.length > 0 && isEmpty(nextValue)) {
                // next is empty, retry handle with nextnext
                nextOffset(next, (err, nextNext) => {
                  handle(start, end, prev, nextNext)
                })
              } else {
                // next is fine
                cb(err, value, prev, next)
              }
            })
          }
        })
      }
    })
  }

  function getMeta (offset, cb) {
    blocks.readUInt32BE(offset, function (err, len) {
      if(err) return cb(err)

      //read the length of the previous item.
      //unless this falls right on the overlap between
      //two blocks, this will already be in the cache,
      //so will be just a mem read.
      if(offset === 0)
        next(4, 4+len, -1, (fsw+len+8))
      else
        blocks.readUInt32BE(offset - (4+fsw), function (err, prev_len) {
          if(err) return cb(err)
          next(offset+4, offset+4+len, offset-(prev_len+8+fsw), offset+(len+8+fsw))
        })

      function next (start, end, prev, next) {
        blocks.read(start, end, function (err, value) {
          if (value.equals(Buffer.alloc(value.length))) {
            cb(new Error('item has been deleted'))
          } else {
            cb(err, value, prev, next)
          }
        })
      }
    })
  }

  const overwriteMeta = (offset, cb) => {
    blocks.readUInt32BE(offset, function (err, len) {
      if (err) return cb(err)

      const bookend = Buffer.alloc(4)
      bookend.writeUInt32BE(len, 0)

      const buf = Buffer.alloc(len)
      const full = Buffer.concat([bookend, buf, bookend])

      blocks.write(full, offset, cb)
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
    frame, getMeta, getMetaStream, restore, overwriteMeta
  }
}


