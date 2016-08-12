var fs = require('fs')

var isBuffer = Buffer.isBuffer

/*
always do aligned reads,
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
    console.log('item length', buf.length)
    b.writeUInt32BE(buf.length, 0 + offset) //start
    b.writeUInt32BE(buf.length, 4+buf.length + offset) //end
    item.value.copy(b, 4 + offset, 0, buf.length)
    offset += buf.length + 8
  }
  return b
}

module.exports = function (file, length) {
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

  return {
    createLogStream: function (opts) {
      //create a stream between any two records.
      //read the first value, then scan forward or backwards
      //in the direction of the log
    },
    //if value is an array of buffers, then treat that as a batch.
    append: function (value, cb) {
      if(!isBuffer(value)) throw new Error('value must be a buffer')
      queue.push({value: value, cb: cb})
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
        if(len  < (length - 4) )
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
    }
  }
}

































