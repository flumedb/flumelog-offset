'use strict'
var fs = require('fs')

var isBuffer = Buffer.isBuffer
var Obv = require('obv')
var Append = require('append-batch')
var Blocks = require('aligned-block-file')
var isInteger = Number.isInteger
var ltgt = require('ltgt')
//var createFrame = require('./frame/basic')
var createFrame = require('./frame/recoverable')
var createStreamCreator = require('pull-cursor')
var Map = require('pull-stream/throughs/map')

function id (v) { return v }
var id_codec = {encode: id, decode: id}

module.exports = function (file, length, codec, cache, frame) {
  if(!codec) codec = id_codec
  var since = Obv()
  length = length || 1024

  var blocks = Blocks(file, length, 'a+', cache)
  frame = createFrame(blocks, codec)

  var since = Obv()
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

  var createStream = require('pull-cursor')(since, frame.getMeta)

  frame.restore(function (err, offset) {
    if(err) throw err
    since.set(offset)
  })

  var log
  return log = {
    filename: file,
    since: since,
    stream: function (opts) {
      //note, this syntax means we don't need to import pull-stream.
      //im not sure about doing encodings like this. seems haphazard.
      return Map(function (data) {
        if(Buffer.isBuffer(data)) return codec.decode(data)
        else if('object' === typeof data)
          data.value = codec.decode(data.value)

        return data
      })
      (createStream(opts))
    },

    //if value is an array of buffers, then treat that as a batch.
    append: append,

    get: function (offset, cb) {
      frame.getMeta(offset, function (err, value) {
        if(err) cb(err)
        else cb(null, codec.decode(value))
      })
    }
  }
}

