# mapreduce

A simple mapreduce implementation heavily influenced by MIT 6.824 and the original [paper](https://pdos.csail.mit.edu/6.824/papers/mapreduce.pdf). The project contains one *simple*, sequential implementation and one more *complex*, distributed implementation using gRPC.

## Usage

Simply clone the repository and run either the *simple* implementation or the more *complex* one like displayed below:

### Sequential implementation

```bash
$ cargo r --bin mapreduce-sequential file.txt anotherfile.txt ...
```

### Distributed implementation

```bash
$ cargo r --bin mapreduce-server file.txt anotherfile.txt ...
$ cargo r --bin mapreduce-client
```

### Note on the input files

Currently, the implementation supports a simple `wordcount`. Therefore, the input files just need to contain some plain text.

After executing, you should be able to locate at least one new file `/tmp/mr-out-*`. This should contain a list of words associated with their respective number of occurences in the input files. Note that increasing the number of worker nodes using the `make_coordinator()` function inside `main()` of `src/server.rs` will lead to multiple output files.

## Contributing

1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am "feat: Add something"`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create Pull Request

## License 

mapreduce (c) 2022 Jakob Waibel and contributors

SPDX-License-Identifier: AGPL-3.0
