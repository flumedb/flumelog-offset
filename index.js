'use strict'
var fs = require('fs')

var isBuffer = Buffer.isBuffer
var Obv = require('obv')
var Append = require('append-batch')
var Blocks = require('aligned-block-file')
var isInteger = Number.isInteger
var ltgt = require('ltgt')

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

function format (seqs, values, seq, value, cursor) {
  return (
    seqs !== false
    ? values !== false
      ? {value: value, seq: seq}
      : seq
    : value
  )
}

var k = 0

function id (v) { return v }
var id_codec = {encode: id, decode: id}

module.exports = function (file, length, codec) {
  if(!codec) codec = id_codec
  var since = Obv()
  length = length || 1024
  var blocks = Blocks(file, length, 'a+')

  var since = Obv()
  var offset = blocks.offset

  var append = Append(function (batch, cb) {
    since.once(function () { // wait for file to load before appending...
      batch = batch.map(codec.encode).map(function (e) {
        return Buffer.isBuffer(e) ? e : new Buffer(e)
      })
      blocks.append(frame(batch), function (err) {
        if(err) return cb(err)
        //else, get offset of last item.
        since.set(blocks.offset.value - (batch[batch.length - 1].length + 8))
        cb(null, since.value)
      })
    })
  })

  offset.once(function (offset) {
    if(offset === 0) return since.set(-1)
    log.getPrevious(offset, function (err, value, length) {
      since.set(offset - length)
    })
  })

  var log
  return log = {
    filename: file,
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
      var skip = false

      if(opts.gt == -1) {
        delete opts.gt
        opts.gte = 0
      }

      if(reverse) {
        if(opts.lt != null) cursor = opts.lt
        else if(opts.lte != null) {
          cursor = opts.lte; skip = true
        }
      }
      else {
        if(opts.gte != null) cursor = opts.gte
        else if(opts.gt != null) {
          cursor = opts.gt; skip = true
        }
        else cursor = 0
      }

      var lower = ltgt.lowerBound(opts) || 0
      var includeLower = ltgt.lowerBoundInclusive(opts)
      var upper = ltgt.upperBound(opts)
      var includeUpper = ltgt.upperBoundInclusive(opts)


      function next (cb) {
        if(aborted) return cb(aborted)

        if(!reverse && upper != null && includeUpper ? cursor > upper : cursor >= upper)
          return cb(true)

        //we should not ever get here in live mode.
        if(!reverse && cursor > since.value)
          return cb(true)

        get(cursor, function (err, value, length) {
          if(err) return cb(err)
          if(!length)
            throw new Error('read empty value:'+length)

          var _cursor = reverse ? cursor - length : cursor
          cursor += (length * diff)

          if(reverse && (includeLower ? _cursor < lower : _cursor <= lower)) {
              return cb(true)
          }

          if(skip) {
            skip = false
            return next(cb)
          }

          cb(err, format(opts.seqs, opts.values, _cursor, value))
        })
      }

      return function (abort, cb) {
        if(aborted = abort) return cb(abort)

        //uh, should this actually be since.once()?
        since.once(function (_offset) {
          //if(_offset < cursor) //throw new Error('offset smaller than cursor')
          if(cursor == null && reverse) {
            offset.once(function (_offset) {
              cursor = _offset; next(cb);
            })
          }
          else if(reverse ? cursor >= 0 : cursor <= _offset)
            next(cb)
          else {
            if(!live) return cb(true)

            since.once(function () {
              next(cb)
            }, false)
          }
        })
      }
    },

    //if value is an array of buffers, then treat that as a batch.
    append: append,

    get: function (_offset, cb) {
      if(!isInteger(_offset)) throw new Error('get: offset must be integer')
      //read the block that offset is in.
      //if offset is near the end of the block, read two blocks.
      if(_offset == null)
        throw new Error('get(undefined)')

      blocks.readUInt32BE(_offset, function (err, length) {
        if(err) return cb(err)
        blocks.read(_offset + 4, _offset + 4 + length, function (err, value) {
          if(value.length !== length) throw new Error('incorrect length, expected:'+length+', was:'+value.length)
            if(!value.length) return cb(new Error('empty buffer read at:'+_offset))
            cb(err, codec.decode(value), length + 8)
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
          cb(err, codec.decode(value), length + 8)
        })
      })
    },
  }
}


