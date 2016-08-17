
var tape = require('tape')
var pull = require('pull-stream')
var Offset = require('../')


var file = '/tmp/offset-test_'+Date.now()+'.log'
var db = Offset(file, 16)
var live = []

pull(
  db.stream({live: true, sync: false}),
  pull.drain(function (data) {
    live.push(data)
  })
)

tape('simple', function (t) {

  db.append(new Buffer('hello world'), function (err, offset1) {
    if(err) throw err
    db.append(new Buffer('hello offset db'), function (err, offset2) {
      if(err) throw err
      t.equal(offset2, 19)
      db.get(offset1, function (err, b) {
        if(err) throw err
        t.equal(b.toString(), 'hello world')

        db.get(offset2, function (err, b2) {
          if(err) throw err
          t.equal(b2.toString(), 'hello offset db')
          db.getPrevious(offset2, function (err, b) {
            if(err) throw err
            t.equal(b.toString(), 'hello world')
            t.end()
          })
        })
      })
    })
  })
})

/*
          8,       4,      32,      32           = 76
_header = {offset, length, prev_mac, hash(data)}
        32,           76,      = 108
header = mac(_header)|_header
data (length)
         4
footer = length

OR, encrypted database?

header_mac (16)
  [offset(8), length(4), data_mac(16)]

footer_mac(16)
  [length (4)]

*/

tape('stream', function (t) {

  pull(
    db.stream({min: 0}),
    pull.collect(function (err, ary) {
      console.log("COLLECT", ary)
      t.deepEqual(ary.map(String), ['hello world', 'hello offset db'])
      t.end()
    })
  )

})

tape('live', function (t) {
  t.deepEqual(live.map(String), ['hello world', 'hello offset db'])
  t.end()
})

tape('reverse', function (t) {
  pull(
    db.stream({reverse: true}),
    pull.collect(function (err, ary) {
      t.deepEqual(ary.map(String), ['hello offset db', 'hello world'])
      t.end()
    })
  )
})

