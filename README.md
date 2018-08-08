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
$ ./gradlew clean shadowJar
```

Create stream with config (required)
```bash
$ maprcli stream create -path /fuse_config
$ maprcli stream topic create -path /fuse_config -topic message_config
```

If the topic is empty will be used default settings
```
start: ''
stop: ''
separator: ''
count: false
```

Or you can specify custom settings. Send to /fuse_config:message_config message in this format
```bash
{"start": "START_MESS","stop": "END_MESS","separator": "\n","size": false}
```

Now, you can find an executable jar in a build/libs folder

```bash
$ ll build/libs/
total 122748
-rw-rw-r-- 1 mapr mapr 125687568 Jul 16 14:30 mapr-fuse-client-db-streams-1.0-SNAPSHOT-all.jar
```

Start the file system and mount <your_folder> as a local directory (<your_folder> and <tx> must exist)

```bash
$ java -jar mapr-fue-client-db-streams/build/libs/mapr-fuse-client-db-streams-1.0-SNAPSHOT-all.jar ~/<your_folder> ~/tx
```

* Check out the contents of `~/<your_folder>` folder. Here you can create a stream. For this purposes you need
to create folder with a name like `*.st`

```bash
$ hadoop fs -ls /
Found 5 items
drwxr-xr-x   - mapr mapr          1 2018-05-10 12:44 /apps
drwxr-xr-x   - mapr mapr          0 2018-05-10 12:48 /opt
drwxrwxrwx   - mapr mapr          0 2018-05-10 12:44 /tmp
drwxr-xr-x   - mapr mapr          3 2018-05-14 12:10 /user
drwxr-xr-x   - mapr mapr          1 2018-05-10 12:44 /var
$ mkdir your_folder/films.st
$ ll your_folder/
total 0
drwxrwxr-x 2 mapr mapr    4096 Jul 11 12:13 films.st
$ hadoop fs -ls /
Found 6 items
drwxr-xr-x   - mapr mapr          1 2018-05-10 12:44 /apps
drwxr-xr-x   - mapr mapr          0 2018-05-10 12:48 /opt
tr--------   - mapr mapr          3 2018-07-16 13:47 /films.st
drwxrwxrwx   - mapr mapr          0 2018-05-10 12:44 /tmp
drwxr-xr-x   - mapr mapr          3 2018-05-14 12:10 /user
drwxr-xr-x   - mapr mapr          1 2018-05-10 12:44 /var
```

* Now you can create topic for this stream by creating folder in the stream folder

```bash
$ ll your_folder/films.st
total 0
$ maprcli stream topic list -path /films.st -json
{
	"timestamp":1531753300025,
	"timeofday":"2018-07-16 03:01:40.025 GMT+0000 PM",
	"status":"OK",
	"total":0,
	"data":[

	]
}
$ mkdir your_folder/films.st/comedy
$ ll your_folder/films.st
total 0
dr-------- 2 mapr mapr 4096 Jul 11 12:13 comedy
$ maprcli stream topic list -path /films.st -json
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
$ ll your_folder/films.st/comedy
total 0
-r-------- 1 mapr mapr 0 Jul 11 12:13 0
```

*After this, you can read the partition. 

```bash
$ cat your_folder/films.st/comedy/0
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
$ tail -f your_folder/films.st/comedy/0
```

* Or read some concrete bytes from the partition.

```bash
$ dd skip=8192 count=100 bs=1 if=your_folder/films.st/comedy/0
```

* Or you can append new messages by typing for example

```bash
$ echo '{"type": "test", "t": 1532098619.488, "k": 301}' >> your_folder/films.st/comedy/0
$ cat your_folder/films.st/comedy/0
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
$ rmdir your_folder/films.st/comedy
$ ll your_folder/films.st
total 0
$ maprcli stream topic list -path /films.st -json
{
  	"timestamp":1531753300025,
  	"timeofday":"2018-07-16 03:01:40.025 GMT+0000 PM",
  	"status":"OK",
  	"total":0,
  	"data":[
  
  	]
}
$ rmdir your_folder/films.st
$ ll your_folder/
total 0
$ hadoop fs -ls /
Found 5 items
drwxr-xr-x   - mapr mapr          1 2018-05-10 12:44 /apps
drwxr-xr-x   - mapr mapr          0 2018-05-10 12:48 /opt
drwxrwxrwx   - mapr mapr          0 2018-05-10 12:44 /tmp
drwxr-xr-x   - mapr mapr          3 2018-05-14 12:10 /user
drwxr-xr-x   - mapr mapr          1 2018-05-10 12:44 /var
```
