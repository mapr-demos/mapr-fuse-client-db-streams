# Overlay File System for Streams

The idea for this project is to implement a FUSE file system that
allows streams (as in MapR ES streams) to appear as directories of
topics, thus allowing POSIX API access to the contents of the stream.

By implementing a first prototype in Python as an overlay on top of
either a FUSE or NFS mount of a MapR FS file system, we can prove out
the concept and experiment with API changes in a relatively simple
fashion without any changes to the current MapR FUSE client.

## Prerequisites

* MapR Converged Data Platform 6.0 ([Installation guide](https://maprdocs.mapr.com/home/AdvancedInstallation/c_get_started_install.html))
* Java 1.8 or higher
* Gradle ([Installation guide](https://docs.gradle.org/current/userguide/installation.html))
* Fuse ([Installation guide](https://github.com/SerCeMan/jnr-fuse/blob/master/INSTALLATION.md))

# How to run the system

Clone the repo into the cluster

```bash
$ git clone https://github.com/mapr-demos/mapr-fuse-client-db-streams.git
```

Go to a project folder

```bash
$ cd mapr-fuse-client-db-streams
```

Build a project

```bash
$ ./gradlew clean jarsh
```

Now, you can find an executable sh in a build/libs folder

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

Mount NFS to MapR-FS to `/mapr` folder

Start the file system and mount `<your_folder>` (must exist) as a local directory

```bash
$ sudo ./mirage /mapr/<cluster_name> ~/<your_folder>
```

* Check out the contents of `~/<your_folder>` folder. Here you can create streams and topics. You should be root for working with `<your_folder>`. Root should be able to create streams and topics via `maprcli`.

```bash
$ sudo ll your_folder
total 0
drwxr-xr-x   - mapr mapr          1 2018-05-10 12:44 /apps
drwxr-xr-x   - mapr mapr          0 2018-05-10 12:48 /opt
drwxrwxrwx   - mapr mapr          0 2018-05-10 12:44 /tmp
drwxr-xr-x   - mapr mapr          3 2018-05-14 12:10 /user
drwxr-xr-x   - mapr mapr          1 2018-05-10 12:44 /var
$ sudo maprcli stream create -path /films
$ sudo ll your_folder/
total 0
drwxr-xr-x   - mapr mapr          1 2018-05-10 12:44 /apps
drwxr-xr-x   - mapr mapr          0 2018-05-10 12:48 /opt
tr--------   - mapr mapr          3 2018-07-16 13:47 /films
drwxrwxrwx   - mapr mapr          0 2018-05-10 12:44 /tmp
drwxr-xr-x   - mapr mapr          3 2018-05-14 12:10 /user
drwxr-xr-x   - mapr mapr          1 2018-05-10 12:44 /var
```
* Each stream has configuration topic `.configuration`. If the topic is empty will be used default settings

```
start: ''
stop: ''
separator: ''
count: false
```

Or you can specify custom settings. Send to `/<stream_name>:.configuratio` message in this format
```bash
{"start": "START_MESS","stop": "END_MESS","separator": "\n","size": false}
```

* Now you can create topic for this stream by creating folder in the stream folder

```bash
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

* When you create a topic one partition will be created by default

```bash
$ sudo ll your_folder/films/comedy
total 0
-r-------- 1 mapr mapr 0 Jul 11 12:13 0
```

*After this, you can read the partition. 

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

* Also, you can read this partition in real time.

```bash
$ sudo tail -f your_folder/films/comedy/0
```

* Or read some concrete bytes from the partition.

```bash
$ sudo dd skip=8192 count=100 bs=1 if=your_folder/films/comedy/0
```

* Or you can append new messages by typing for example

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

* Also you can remove topic or stream.

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

# Issues to be Solved

The current system still has some glaring deficiencies:

- The current system uses a simulated stream instead of a real
  one. This facilitates debugging but isn't realistic. Currently, any
  file that has a name like `*.st` is treated as a simulated stream
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

