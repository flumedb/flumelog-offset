# offset-log

experiment for a immutable database.

## Idea

this is an experiment to see how simple a database can be.
this is strictly an append-only, buffers are written to the database,
with a light framing (the length of that buffer) and the offset is returned
as a key.

Using the offset as the key gives you a very small key that increases monotonically.
an 8 byte key can represent a 256 terabyte database. The key smallness also
means that indexes can be very small.

Since the database does very little, it can also be very fast.
Since the keys are small, that simplifies the indexes too.

I am writing this to experiment with indexes that are simply
a list of keys with a particular sort applied to them - just enough
to enable binary search.

## API ideas

there are lots of ways you could implement a frame around the data,
which might have different benefits. you could use json or csv format,
and then you could view the database in a text editor.

you could encrypt the data, with authentication so that you can detect interference.

what should the api for framing look like? given an offset,
you need to be able to get the next, and the previous 

## License

MIT




