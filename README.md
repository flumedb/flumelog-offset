# flumelog-offset

An flumelog where the offset into the file is the key.
Each value is appended to the log with a double ended framing,
and the "sequence" is the position in the physical file where the value starts,
this means if you can do a read in O(1) time!

Also, this is built on top of [aligned-block-file](https://github.com/flumedb/aligned-block-file)
so that caching works very well.

## License

MIT






