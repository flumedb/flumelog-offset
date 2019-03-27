'use strict'
var Obv = require('obv')
var Append = require('append-batch')
var createStreamCreator = require('pull-cursor')
var Cache = require('hashlru')
var Looper = require('pull-looper')
var pull = require('pull-stream')
var filter = require('pull-stream/throughs/filter')

module.exports = function (blocks, frame, codec, file, cache) {
  var since = Obv()
  cache = cache || Cache(256)
  var offset = blocks.offset

  var append = Append(function (batch, cb) {
    since.once(function () { // wait for file to load before appending...
      batch = batch.map(codec.encode).map(function (e) {
        return Buffer.isBuffer(e) ? e : new Buffer(e)
      })
      var framed = frame.frame(batch, blocks.offset.value)
      var _since = frame.frame.offset
      blocks.append(framed, function (err, offset) {
        if(err) return cb(err)
        //else, get offset of last item.
        since.set(_since)
        cb(null, since.value)
      })
    })
  })

  var isDeleted = (b) => 
    Buffer.isBuffer(b) === true && b.equals(Buffer.alloc(b.length)) === true

  var isNotDeleted = (b) => isDeleted(b) === false

  function getMeta (offset, useCache, cb) {
    if (useCache) {
      var data = cache.get(offset)
      if (data) {
        cb(null, data.value, data.prev, data.next)
        return
      }
    }

    frame.getMeta(offset, function (err, value, prev, next) {
      if(err) return cb(err)
      if (isDeleted(value)) return cb(null, value, prev, next) // skip decode

      var data = {
        value: codec.decode(codec.buffer ? value : value.toString()),
        prev: prev,
        next: next
      }

      if (useCache)
        cache.set(offset, data)
      cb(null, data.value, data.prev, data.next)
    })
  }

  var createStream = createStreamCreator(since, getMeta)

  frame.restore(function (err, offset) {
    if(err) throw err
    since.set(offset)
  })


  return {
    filename: file,
    since: since,
    stream: function (opts) {
      const filterValue = opts && opts.seqs === false
        ? (item) => isNotDeleted(item)
        : (item) => isNotDeleted(item.value)

      return pull(
        Looper(createStream(opts)),
        filter(filterValue)
      )
    },

    //if value is an array of buffers, then treat that as a batch.
    append: append,

    get: function (offset, cb) {
      frame.getMeta(offset, function (err, value) {
        if(err) return cb(err)
        if (isDeleted(value)) return cb(new Error('item has been deleted'))

        cb(null, codec.decode(value))
      })
    },
    del: function (offsets, cb) {
      if (Array.isArray(offsets) === false) {
        // The `seqs` argument may be a single value or an array.
        // To minimize complexity, this ensures `seqs` is always an array.
        offsets = [ offsets ]
      }

      Promise.all(offsets.map(offset =>
        new Promise((resolve, reject) => {
          // Simple callback handler for promises.
          const promiseCb = (err) => {
            if (err) {
              reject(err)
            } else {
              resolve()
            }
          }

          cache.remove(offset)
          frame.overwriteMeta(offset, promiseCb)
        })
      )).catch((err) => cb(err))
        .then(() => cb(null))
    }
  }
}

