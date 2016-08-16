var fs = require('fs')

var isBuffer = Buffer.isBuffer
var Notify = require('pull-notify')
var Live = require('pull-live')

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

function once (fn) {
  var called = false
  var err = new Error('called twice')
  return function () {
    var args = [].slice.call(arguments)
    if(called) throw err
    called = true
    return fn.apply(this, args)

  }
}

module.exports = function (file, length) {

  var notify = Notify()
  length = length || 1024
  var blocks = Blocks(file, length, 'a+')

  var queue = [], writing = false
  //TODO: check current size of file!
  var offset = 0

  function write () {
    if(writing) return
    if(!queue.length) return
    writing = true
    var data = []
    var framed = frame(queue)
    var _queue = queue
    queue = []
    blocks.append(framed, function (err, offset) {
      writing = false
      while(_queue.length) {
        var q = _queue.shift()
        q.cb(err, (offset - framed.length) + q.offset)
      }
      if(queue.length) write()
    })
  }

  var log
  return log = {
    //create a stream between any two records.
    //read the first value, then scan forward or backwards
    //in the direction of the log
    stream: Live(function (opts) {
      var reverse = opts && opts.reverse
      var next = reverse ? (opts && opts.max || offset) : (opts && opts.min || 0)
      var diff = reverse ? -1 : 1
      var get = reverse ? log.getPrevious : log.get

      return function (abort, cb) {
        cb = once(cb)
        if(abort) cb(abort)
        else if(reverse && next <= 0)
          cb(true)
        else
          get(next, once(function (err, value) {
            if(err) return cb(true) //err)
            else if(!value) return cb(true)
            else {
              next = next + (value.length + 8)*diff
              cb(null, value)
            }
          }))
      }
    }, function (opts) {
      return notify.listen()
    }),
    //if value is an array of buffers, then treat that as a batch.
    append: function (value, cb) {
      if(!isBuffer(value)) throw new Error('value must be a buffer')
      queue.push({value: value, cb: function (err, offset) {
        if(err) return cb(err)
        notify(value)
        cb(null, offset)
      }})
      write()
    },
    get: function (offset, cb) {
      //read the block that offset is in.
      //if offset is near the end of the block, read two blocks.
      blocks.readUInt32BE(offset, function (err, length) {
        if(err) return cb(err)
        blocks.read(offset + 4, offset+4 + length, once(cb))
      })
    },
    //get the record _before_ the given offset.
    getPrevious: function (_offset, cb) {
      //don't read before start of file...
      var b = new Buffer(Math.min(length, _offset))

      if(_offset == 0) return cb(new Error('attempted read previous to first object'))
      blocks.readUInt32BE(_offset - 4, function (err, length) {
        if(err) return cb(err)
        blocks.read(_offset - 4 - length, _offset - 4, cb)
      })
    },
  }
}



