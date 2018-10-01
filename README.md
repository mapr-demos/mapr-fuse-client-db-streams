# Overlay File System for Streams

The idea for this project is to implement a FUSE file system that
allows streams (as in MapR ES streams) to appear as directories of
topics, thus allowing POSIX API access to the contents of the stream.

By implementing a first prototype in Java as an overlay on top of
either a FUSE or NFS mount of a MapR FS file system, we can prove out
the concept and experiment with API changes in a relatively simple
fashion without any changes to the current MapR FUSE client.

## Prerequisites

* MapR Converged Data Platform 6.0 ([Installation guide](https://maprdocs.mapr.com/home/AdvancedInstallation/c_get_started_install.html))
* Java 1.8 or higher
* Gradle ([Installation guide](https://docs.gradle.org/current/userguide/installation.html))
* Fuse ([Installation guide](https://github.com/SerCeMan/jnr-fuse/blob/master/INSTALLATION.md))

# How to run the system

Get pre-requisites

```bash
# apt-get update
# apt-get install -y git gradle fuse
```

Clone the repo into the cluster

```bash
$ git clone https://github.com/mapr-demos/mapr-fuse-client-db-streams.git
```

Go to a project folder

```bash
$ cd mapr-fuse-client-db-streams
```

Build a project

**Gradle**
```bash
$ ./gradlew clean jarsh
```

Now, you can find an executable sh in a `build/libs` folder

```bash
$ ll build/libs/
total 265140
drwxrwxr-x 2 mapr mapr      4096 Aug 15 17:45  ./
drwxrwxr-x 6 mapr mapr      4096 Aug 15 17:45  ../
-rw-rw-r-- 1 mapr mapr  26314453 Aug 15 17:45  mapr-fuse-client-db-streams-1.0-SNAPSHOT.jar
-rw-rw-r-- 1 mapr mapr 122588232 Aug 15 17:45  mapr-fuse-client-db-streams-execjar-1.0-SNAPSHOT.jar
-rwxrw-r-- 1 mapr mapr 122588620 Aug 15 17:45 'value: mapr-fuse-client-db-streams.sh'*
```

Copy `'value: mapr-fuse-client-db-streams.sh'` to your folder and make it executable

```bash
$ cp 'value: mapr-fuse-client-db-streams.sh' mirage
$ chmod +x mirage
```

**Maven**

```bash
$ mvn package
```

Now, you can find an executable sh in a `target` folder

```bash
$ ll target/
total 268972
drwxrwxr-x  9 mapr mapr      4096 Oct  1 16:31 ./
drwxr-xr-x 10 mapr mapr      4096 Oct  1 16:30 ../
drwxrwxr-x  3 mapr mapr      4096 Oct  1 16:30 classes/
drwxrwxr-x  3 mapr mapr      4096 Oct  1 16:30 generated-sources/
drwxrwxr-x  3 mapr mapr      4096 Oct  1 16:30 generated-test-sources/
-rw-rw-r--  1 mapr mapr 124530454 Oct  1 16:31 mapr-fuse-client-db-streams-1.0-SNAPSHOT.jar
drwxrwxr-x  2 mapr mapr      4096 Oct  1 16:31 maven-archiver/
drwxrwxr-x  3 mapr mapr      4096 Oct  1 16:30 maven-status/
-rwxrwxr-x  1 mapr mapr 124530498 Oct  1 16:31 mirage*
-rw-rw-r--  1 mapr mapr  26328980 Oct  1 16:31 original-mapr-fuse-client-db-streams-1.0-SNAPSHOT.jar
drwxrwxr-x  2 mapr mapr      4096 Oct  1 16:31 surefire-reports/
drwxrwxr-x  3 mapr mapr      4096 Oct  1 16:30 test-classes/

```

### Run

Mount MapR-FS as `/mapr` (this is the standard way that MapR FS is mounted either via FUSE or NFS)

You should be able to see `/mapr/<cluster_name>` containing the contents of your cluster.

Start the mirage file system and mount it as `<your_folder>` (must exist already) 

```bash
$ sudo ./mirage /mapr/<cluster_name> ~/<your_folder>
```

* Now check out the contents of `~/<your_folder>` folder. It should look nearly identical to `/mapr/<cluster_name>`. Here you can create streams and topics. You should be root for working with the top-level of `<your_folder>`. Root should also be able to create streams and topics via `maprcli`.

For example, suppose we create a stream called `films` in the top-level directory:
```bash
$ sudo ll your_folder
total 0
drwxr-xr-x   - mapr mapr          1 2018-05-10 12:44 /apps
drwxr-xr-x   - mapr mapr          0 2018-05-10 12:48 /opt
drwxrwxrwx   - mapr mapr          0 2018-05-10 12:44 /tmp
drwxr-xr-x   - mapr mapr          3 2018-05-14 12:10 /user
drwxr-xr-x   - mapr mapr          1 2018-05-10 12:44 /var
$ sudo maprcli stream create -path /mapr/<cluster_name>/films
$ sudo ll your_folder/
total 0
drwxr-xr-x   - mapr mapr          1 2018-05-10 12:44 /apps
drwxr-xr-x   - mapr mapr          0 2018-05-10 12:48 /opt
tr--------   - mapr mapr          3 2018-07-16 13:47 /films
drwxrwxrwx   - mapr mapr          0 2018-05-10 12:44 /tmp
drwxr-xr-x   - mapr mapr          3 2018-05-14 12:10 /user
drwxr-xr-x   - mapr mapr          1 2018-05-10 12:44 /var
```
As you see, this new stream appears with some kind of strange permissions.

* Each stream has a magical configuration topic called `.configuration`. If you write to this topic, you can set various parameters like strings that prepended or appended to each message and whether a byte count should be included. If the `.configuration` topic is empty then these default settings will be used.

```
start: ''
stop: ''
separator: ''
count: false
```
This isn't very useful, however, since we won't know for sure when one message ends and another begins.

This can be changed with custom settings. Send a message in the following form to `/<stream_name>:.configuration` 
```bash
{"start": "START_MESS","stop": "END_MESS","separator": "\n","size": false}
```
If your messages are composed of JSON messages in text form and you know that there are no new lines in the messages, then it would be common to set the `start` and `stop` messages to empty strings and set the separator to `\n`. In this example, we have set the start and stop strings to non-empty strings so they can be seen. If you have a binary message format, it would be common to set `size` to `true` and use a distinctive `stop` string. The rational for using a stop string is that if you seek to some random point in a partition, you can scan to the next instance of your stop string and check to see if a valid message follows by looking at putative byte count (four bytes, big endian) to see if you find the stop string again after that many bytes. This is necessary because there is no provision in a file API for marking record boundaries out-of-band.

At this point, you can verify that there are no topics for this stream yet either by listing the contents of `your_folder` or by using a maprcli command to list the topics in the stream.

```bash
$ sudo ll your_folder/films
total 0
$ sudo maprcli stream topic list -path /mapr/<cluster_name>/films -json
{
	"timestamp":1531753300025,
	"timeofday":"2018-07-16 03:01:40.025 GMT+0000 PM",
	"status":"OK",
	"total":0,
	"data":[

	]
}
```
We can create a topic using `mkdir` on the stream via the mirage file system. This topic also shows up in the actual stream.
```bash
$ sudo mkdir your_folder/films/comedy
$ sudo ll your_folder/films
total 0
dr-------- 2 mapr mapr 4096 Jul 11 12:13 comedy
$ sudo maprcli stream topic list -path /films -json
{
	"timestamp":1531753472714,
	"timeofday":"2018-07-16 03:04:32.714 GMT+0000 PM",
	"status":"OK",
	"total":1,
	"data":[
		{
			"topic":"comedy",
			"partitions":1,
			"consumers":0,
			"physicalsize":0,
			"logicalsize":0,
			"maxlag":0
		}
	]
}
```

When you create a topic one partition will be created by default and it appears as a file inside the topic directory.
```bash
$ sudo ll your_folder/films/comedy
total 0
-r-------- 1 mapr mapr 0 Jul 11 12:13 0
```

At this point, you can read or write to the partition using standard file operations. If a program is writing messages to the stream, we might see this:

```bash
$ sudo cat your_folder/films/comedy/0
START_MESS {"type": "test", "t": 1531741609.159, "k": 0} END_MESS
START_MESS {"type": "test", "t": 1531741609.424, "k": 1} END_MESS
START_MESS {"type": "test", "t": 1531741609.675, "k": 2} END_MESS
START_MESS {"type": "test", "t": 1531741609.926, "k": 3} END_MESS
START_MESS {"type": "test", "t": 1531741610.177, "k": 4} END_MESS
START_MESS {"type": "test", "t": 1531741610.427, "k": 5} END_MESS
START_MESS {"type": "test", "t": 1531741610.678, "k": 6} END_MESS
START_MESS {"type": "test", "t": 1531741610.929, "k": 7} END_MESS
START_MESS {"type": "test", "t": 1531741611.179, "k": 8} END_MESS
START_MESS {"type": "test", "t": 1531741611.43, "k": 9} END_MESS
START_MESS {"type": "test", "t": 1531741611.681, "k": 10} END_MESS
```

Also, we can watch this partition grow in real time.

```bash
$ sudo tail -f your_folder/films/comedy/0
```

Or read from particular byte offsets in the partition. These byte offsets include the start and end strings and any byte counts, of course.

```bash
$ sudo dd skip=8192 count=100 bs=1 if=your_folder/films/comedy/0
```

New messages can be created by writing to the partition directly using standard file operations. Each distinct write forms a message.

```bash
$ sudo echo '{"type": "test", "t": 1532098619.488, "k": 301}' >> your_folder/films/comedy/0
$ sudo cat your_folder/films/comedy/0
START_MESS {"type": "test", "t": 1531741609.159, "k": 0} END_MESS
START_MESS {"type": "test", "t": 1531741609.424, "k": 1} END_MESS
START_MESS {"type": "test", "t": 1531741609.675, "k": 2} END_MESS
START_MESS {"type": "test", "t": 1531741609.926, "k": 3} END_MESS
START_MESS {"type": "test", "t": 1531741610.177, "k": 4} END_MESS
START_MESS {"type": "test", "t": 1531741610.427, "k": 5} END_MESS
START_MESS {"type": "test", "t": 1531741610.678, "k": 6} END_MESS
START_MESS {"type": "test", "t": 1531741610.929, "k": 7} END_MESS
START_MESS {"type": "test", "t": 1531741611.179, "k": 8} END_MESS
START_MESS {"type": "test", "t": 1531741611.43, "k": 9} END_MESS
START_MESS {"type": "test", "t": 1531741611.681, "k": 10} END_MESS
START_MESS {"type": "test", "t": 1532098619.488, "k": 301} END_MESS
```

We can remove topics or streams, but we can't modify partitions.

```bash
$ sudo rmdir your_folder/films/comedy
$ sudo ll your_folder/films
total 0
$ sudo maprcli stream topic list -path /films -json
{
  	"timestamp":1531753300025,
  	"timeofday":"2018-07-16 03:01:40.025 GMT+0000 PM",
  	"status":"OK",
  	"total":0,
  	"data":[
  
  	]
}
$ sudo rmdir your_folder/films
$ sudo ll your_folder/
total 0
drwxr-xr-x   - mapr mapr          1 2018-05-10 12:44 /apps
drwxr-xr-x   - mapr mapr          0 2018-05-10 12:48 /opt
drwxrwxrwx   - mapr mapr          0 2018-05-10 12:44 /tmp
drwxr-xr-x   - mapr mapr          3 2018-05-14 12:10 /user
drwxr-xr-x   - mapr mapr          1 2018-05-10 12:44 /var
```

## Limitations

Currently, this system doesn't seem to compile on a Mac. Don't know why it is hard to resolve `one-jar`. Works fine on Linux.
