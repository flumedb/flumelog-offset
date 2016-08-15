var fs = require('fs')

var isBuffer = Buffer.isBuffer
var Notify = require('pull-notify')
var Live = require('pull-live')

/*
TODO: always do aligned reads,
to a given block size.
keep the read blocks in a sparse array.

have a fixed cache size, and prune least recently used items.
successful writes should also go into the cache immediately.
*/

function frame (data, _offset) {
  var length = data.reduce(function (total, e) { return total + e.value.length }, 0)
  var b = new Buffer(length + data.length * 8)
  var offset = 0
  for(var i = 0; i < data.length; i++) {
    var item = data[i]
    //mutate the items
    var buf = item.value
    item.offset = 0 + offset + _offset
    b.writeUInt32BE(buf.length, 0 + offset) //start
    b.writeUInt32BE(buf.length, 4+buf.length + offset) //end
    item.value.copy(b, 4 + offset, 0, buf.length)
    offset += buf.length + 8
  }
  return b
}

module.exports = function (file, length) {

  var notify = Notify()
  length = length || 1024
  var fd
  if(Number.isInteger(file))
    fd = file
  else
    fd = fs.openSync(file, 'a+')

  var queue = [], writing = false
  //TODO: check current size of file!
  var offset = 0

  function write () {
    if(writing) return
    if(!queue.length) return
    writing = true
    var data = []
    var framed = frame(queue, offset)
    var _queue = queue
    queue = []
    fs.write(fd, framed, 0, framed.length, function (err, bytes) {
      writing = false

      offset = offset += bytes
      //empty the queue
      for(var i = 0; i < _queue.length; i++) {
        _queue[i].cb(err, _queue[i].offset)
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
        if(abort) cb(abort)
        else if(reverse && next <= 0)
          cb(true)
        else
          get(next, function (err, value) {
            if(err) return cb(err)
            if(!value) return cb(true)
            next = next + (value.length + 8)*diff
            cb(null, value)
          })
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
      var b = new Buffer(length)
      fs.read(fd, b, 0, b.length, offset, function (err, bytes_read) {
        if(err) return cb(err)
        //read the record length.
        //if we have the entire length already,
        //callback with the value.
        //else read the rest of the blocks necessary.
        var len = b.readUInt32BE(0)

        //check if we have the entire object already.
        if(bytes_read == 0) 
          cb()
        else if(len  < (length - 4) )
          return cb(null, b.slice(4, len+4))
        else {
          var b2 = new Buffer(len)

          fs.read(fd, b2, length - 4, b2.length - (length - 4), offset + b.length, function (err) {
            if(err) return cb(err)
            //copy the first read into the second buffer.
            b.copy(b2, 0, 4, length)
            cb(null, b2.slice(0, len+4))
          })
        }
      })
    },
    //get the record _before_ the given offset.
    getPrevious: function (_offset, cb) {
      //don't read before start of file...
      var b = new Buffer(Math.min(length, _offset))
      fs.read(fd, b, 0, b.length, _offset - b.length, function (err, bytes_read) {
        if(err) return cb(err)
        var len = b.readUInt32BE(b.length - 4)
        if(len <= (b.length -  4)) {
          return cb(null, b.slice(b.length - len - 4, b.length - 4))
        }
        else {
          var o = _offset - 4 - len
          var b2 = new Buffer(len)
          fs.read(fd, b2, 0, b2.length, o, function (err) {
            if(err) cb(err)
            else cb(null, b2)
          })
        }
      })
    },
  }
}

