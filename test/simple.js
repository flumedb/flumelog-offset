
var tape = require('tape')

var Offset = require('../')

tape('simple', function (t) {

  var file = '/tmp/offset-test_'+Date.now()+'.log'
  var db = Offset(file, 16)

  db.append(new Buffer('hello world'), function (err, offset1) {
    if(err) throw err
    db.append(new Buffer('hello offset db'), function (err, offset2) {
      if(err) throw err
      db.get(offset1, function (err, b) {
        t.equal(b.toString(), 'hello world')

        db.get(offset2, function (err, b2) {
          t.equal(b2.toString(), 'hello offset db')
          t.end()
        })
      })
    })
  })
})



