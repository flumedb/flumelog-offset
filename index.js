'use strict'
var fs = require('fs')

var isBuffer = Buffer.isBuffer
var Obv = require('obv')
var Append = require('append-batch')
var Blocks = require('aligned-block-file')
var isInteger = Number.isInteger

function frame (data) {
  var length = data.reduce(function (total, value) { return total + value.length }, 0)
  var b = new Buffer(length + data.length * 8)
  var offset = 0
  for(var i = 0; i < data.length; i++) {
    var buf = data[i]
    //mutate the items
    //var buf = item.value
    b.writeUInt32BE(buf.length, 0 + offset) //start
    b.writeUInt32BE(buf.length, 4+buf.length + offset) //end
    buf.copy(b, 4 + offset, 0, buf.length)
    offset += buf.length + 8
  }
  return b
}

function format (keys, values, key, value, cursor) {
  return (
    keys !== false
    ? values !== false
      ? {key: key, value: value, seq: cursor}
      : key
    : value
  )
}

var k = 0

module.exports = function (file, length) {

  var since = Obv()
  length = length || 1024
  var blocks = Blocks(file, length, 'a+')

  var append = Append(function (batch, cb) {
    blocks.append(frame(batch), function (err) {
      if(err) return cb(err)
      //else, get offset of last item.
      since.set(blocks.offset.value - (batch[batch.length - 1].length + 8))
      cb(null, since.value)
    })
  })

  var since = Obv()
  var offset = blocks.offset

  offset.once(function (offset) {
    if(offset === 0) return since.set(-1)
    log.getPrevious(offset, function (err, value, length) {
      since.set(offset - length)
    })
  })

  var log
  return log = {
    since: since,
    //create a stream between any two records.
    //read the first value, then scan forward or backwards
    //in the direction of the log

    stream: function (opts) {
      opts = opts || {}
      var cursor
      var reverse = !!opts.reverse
      var get = reverse ? log.getPrevious : log.get
      var diff = reverse ? -1 : 1
      var live = opts.live
      var aborted = false
      if(!reverse && opts.gte == null) {
        cursor = 0
      }
      else
        cursor = reverse ? opts.lt : opts.gte

      function next (cb) {
        if(aborted) return cb(aborted)
        get(cursor, function (err, value, length) {
          if(!value.length) throw new Error('read empty value')
          var _cursor = cursor
          cursor += (length * diff)
          cb(err, format(opts.keys, opts.value, _cursor, value, cursor))
        })
      }

      return function (abort, cb) {
        if(aborted = abort) return cb(abort)

        offset.once(function (_offset) {
          //if(_offset < cursor) //throw new Error('offset smaller than cursor')
          if(cursor == null && reverse) {
            cursor = _offset; next(cb)
          }
          else if(reverse ? cursor > 0 : cursor < _offset) next(cb)
          else if(reverse ? cursor <= 0 : cursor >= _offset) {
            if(!live) return cb(true)
            offset.once(function () { next(cb) }, false)
          }
          else
            throw new Error('should never happen: cursor is invalid state:'+cursor+' offset:'+_offset)
        })
      }
    },

    //if value is an array of buffers, then treat that as a batch.
    append: append,

    get: function (_offset, cb) {
      if(!isInteger(_offset)) throw new Error('get: offset must be integer')
      //read the block that offset is in.
      //if offset is near the end of the block, read two blocks.
      blocks.readUInt32BE(_offset, function (err, length) {
        if(err) return cb(err)
        blocks.read(_offset + 4, _offset + 4 + length, function (err, value) {
          if(value.length !== length) throw new Error('incorrect length, expected:'+length+', was:'+value.length)
          setImmediate(function () {
            cb(err, value, length + 8)
          })
        })
      })
    },
    //get the record _before_ the given offset.
    getPrevious: function (_offset, cb) {
      //don't read before start of file...
      if(!isInteger(_offset)) throw new Error('getPrevious: offset must be integer')

      _offset = _offset || blocks.size()
      if(_offset == 0) return cb(new Error('attempted read previous to first object'))
      blocks.readUInt32BE(_offset - 4, function (err, length) {
        if(err) return cb(err)
        blocks.read(_offset - 4 - length, _offset - 4, function (err, value) {
          cb(err, value, length + 8)
        })
      })
    },
  }
}


