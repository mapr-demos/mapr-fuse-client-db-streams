# Overlay File System for Streams

The idea for this project is to implement a FUSE file system that
allows streams (as in MapR ES streams) to appear as directories of
topics, thus allowing POSIX API access to the contents of the stream.

By implementing a first prototype in Python as an overlay on top of
either a FUSE or NFS mount of a MapR FS file system, we can prove out
the concept and experiment with API changes in a relatively simple
fashion without any changes to the current MapR FUSE client.

# How to run the system

Copy the test data to `/tmp`
```bash
cp data.dir /tmp
```
Start the file system and mount /tmp as a local directory
```bash
cd src/python/
mkdir tx
python3.6 fs.py /tmp tx &
```
Check out the contents of tx. Note that data.dir looks like a
directory. Look inside:
```bash
ls tx
ls tx/data.dir
```
Tail one of the topics
```bash
tail -f tx/data.dir/23
```

# Issues to be Solved

The current system still has some glaring deficiencies:

- The current system uses a simulated stream instead of a real
  one. This facilitates debugging but isn't realistic. Currently, any
  file that has a name like `*.dir` is treated as a simulated stream
  and is assumed to contain JSON data consisting of a list of JSON
  objects. Each object has two fields, `topic` and `messages` and is
  assumed to contain the data that would normally be in the stream. In
  the simulated stream, one element of the list in the `messages`
  field is released each second after the file system is started.

- In the current system, each stream is viewed as a directory of
  topics. Each topic contains messages separated by a delimiter
  string. In the real system, each topic should probably be better
  considered a directory of partitions. Also, the convention of using
  a delimiter string should be augmented with the ability to use other
  serialization for messages, most importantly a byte-count, bytes
  sequence.

- Not all operations are currently supported. As an example, unlink
  applied to a stream should cause a stream delete, applied to a topic
  should delete the topic, and applied to a partition should cause an
  error.

