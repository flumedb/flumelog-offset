var fs = require('fs')

var isBuffer = Buffer.isBuffer
var Notify = require('pull-notify')
var Live = require('pull-live')
var pull = require('pull-stream/pull')
var Map = require('pull-stream/throughs/map')

var Blocks = require('block-reader')

function frame (data) {
  var length = data.reduce(function (total, e) { return total + e.value.length }, 0)
  var b = new Buffer(length + data.length * 8)
  var offset = 0
  for(var i = 0; i < data.length; i++) {
    var item = data[i]
    //mutate the items
    var buf = item.value
    item.offset = 0 + offset
    b.writeUInt32BE(buf.length, 0 + offset) //start
    b.writeUInt32BE(buf.length, 4+buf.length + offset) //end
    item.value.copy(b, 4 + offset, 0, buf.length)
    offset += buf.length + 8
  }
  return b
}

function format (opts) {
  var keys = opts.keys === true //default to false
  var values = opts.value !== false //default to true
  return Map(function (data) {
    return keys && values ? data : values ? data.value : data.key
  })
}

module.exports = function (file, length) {

  var notify = Notify()
  length = length || 1024
  var blocks = Blocks(file, length, 'a+')

  var queue = [], writing = false
  //TODO: check current size of file!
  var offset = -1

  try {
    offset = fs.statSync(file).size
  } catch(_) {}

  function write () {
    if(writing) return
    if(!queue.length) return
    writing = true
    var data = []
    var framed = frame(queue)
    var _queue = queue
    queue = []
    blocks.append(framed, function (err, _offset) {
      writing = false
      while(_queue.length) {
        var q = _queue.shift()
        var o = (_offset - framed.length) + q.offset
        offset = Math.max(offset, o)
        q.cb(err, o)
      }
      if(queue.length) write()
    })
  }

  var log
  return log = {
    //create a stream between any two records.
    //read the first value, then scan forward or backwards
    //in the direction of the log

    //using pull-live this way means that things added in real-time are buffered
    //in memory until they are read, that means less predictable memory usage.
    //instead, we should track the offset we are up to, and wait if necessary.
    stream: Live(function (opts) {
      var reverse = opts && opts.reverse
      var next = reverse ? (opts && opts.max || blocks.size()) : (opts && opts.min || 0)
      var diff = reverse ? -1 : 1
      var get = reverse ? log.getPrevious : log.get
      var end = offset
      return pull(function (abort, cb) {
        if(abort) cb(abort)
        else if(reverse ? next <= 0 : next > end)
          cb(true)
        else
          get(next, function (err, value) {
            if(err) return cb(true) //err)
            else if(!value || !value.length) return cb(true)
            else {
              var _offset = next
              next = next + (value.length + 8)*diff
              cb(null, {key: _offset, value: value})
            }
          })
      }, format(opts))
    }, function (opts) {
      return pull(notify.listen(), format(opts))
    }),
    //if value is an array of buffers, then treat that as a batch.
    append: function (value, cb) {
      //TODO: make this like, actually durable...
      if(Array.isArray(value)) {
        var offsets = []
        value.forEach(function (v) {
          queue.push({value: v, cb: function (err, offset) {
            offsets.push(offset)
            if(offsets.length === value.length)
              cb(null, offsets)
          }})
        })

        return write()
      }
      if(!isBuffer(value)) throw new Error('value must be a buffer')
      queue.push({value: value, cb: function (err, offset) {
        if(err) return cb(err)
        notify({key: offset, value: value})
        cb(null, offset)
      }})
      write()
    },
    get: function (offset, cb) {
      //read the block that offset is in.
      //if offset is near the end of the block, read two blocks.
      blocks.readUInt32BE(offset, function (err, length) {
        if(err) return cb(err)
        blocks.read(offset + 4, offset+4 + length, cb)
      })
    },
    //get the record _before_ the given offset.
    getPrevious: function (_offset, cb) {
      //don't read before start of file...
      _offset = _offset || blocks.size() 
      if(_offset == 0) return cb(new Error('attempted read previous to first object'))
      blocks.readUInt32BE(_offset - 4, function (err, length) {
        if(err) return cb(err)
        blocks.read(_offset - 4 - length, _offset - 4, cb)
      })
    },
  }
}







