
var fs = require('fs')
var tape = require('tape')
var crypto = require('crypto')
var OffsetLog = require('../')
var pull = require('pull-stream')

var file = '/tmp/flumelog_restore_'+Date.now()

var n = 13, ary = [], since
while(n--)
  ary.push(crypto.randomBytes(17))

tape('setup', function (t) {
  var log = OffsetLog(file, {blockSize: 23})


  log.since.once(function (value) {
    t.equal(value, -1)
    log.append(ary, function (err, value) {
//      t.equal(log.since.value, 1024*9 + 9*12 + 4)
      console.log(log.since.value)
      log.get(log.since.value, function (err, value) {
        if(err) throw err
        t.deepEqual(value, ary[ary.length-1])

        pull(
          log.stream(),
          pull.collect(function (err, _ary) {
            if(err) throw err
            t.deepEqual(ary, _ary.map(function (e) { return e.value }))
            since = log.since.value
            t.end()
          })
        )

      })
    })

  })
})

tape('restore, valid', function (t) {

  var log = OffsetLog(file, {blockSize: 23})

  log.since(function (v) {
    t.equal(v, since)
    t.end()
  })
})
return
tape('truncate', function (t) {
  fs.stat(file, function (err, stat) {
    if(err) throw err
    fs.readFile(file, function (err, buf) {
      if(err) throw err
      var offset = buf.readUInt32BE(buf.length - 4)
      var slice = stat.size - ~~(ary[ary.length-1].length/2)
      console.log('slice at:', slice)
      fs.truncate(file, slice, function (err) {
        if(err) throw err
        t.end()
      })
    })

  })
})


tape('restore', function (t) {
  var log = OffsetLog(file, {blockSize: 23})
  log.since.once(function (v) {
    t.ok(v < since)
    t.ok(v > 0)
    return t.end()
    pull(
      log.stream(),
      pull.collect(function (err, _ary) {
        if(err) throw err
        t.deepEqual(ary.slice(0, ary.length-1), _ary.map(function (e) { return e.value }))
        since = log.since.value
        t.end()
      })
    )

  })
})




