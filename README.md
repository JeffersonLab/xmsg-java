# xMsg

xMsg is a lightweight, yet full featured publish/subscribe messaging system,
presenting asynchronous inter-process communication.

[![Build Status](https://travis-ci.org/JeffersonLab/xmsg-java.svg?branch=master)](https://travis-ci.org/JeffersonLab/xmsg-java)
[![Javadoc](https://img.shields.io/badge/javadoc-2.4--SNAPSHOT-blue.svg?style=flat)](https://claraweb.jlab.org/xmsg/api/java/v2.4)

## Overview

xMsg actors are required to publish and/or subscribe to messages.
The messages are identified by topics, and contain metadata
and user-serialized data.
xMsg topic convention defines three parts:
_domain_, _subject_, and _type_.
The data is identified by the _mime-type_ metadata field,
which can be used by applications to deserialize the raw bytes
into its proper data-type object.

Multi-threaded publication of messages is supported,
with each thread using its own connection to send messages.
Subscriptions run in a background thread,
and each received message is processed by a user-defined callback
executed by a thread-pool.
Note that the provided callback must be thread-safe.

A proxy must be running in order to pass messages between actors.
Messages must be published to the same proxy than the subscription,
or the subscriber will not receive them.
Long-lived actors can register with a global registrar service
if they are periodically publishing or subscribed to a given topic,
and others actors can search the registrar to discover them.


## Basic usage

Simple publisher:
```java
xMsg actor = new xMsg("publisher");
xMsgTopic topic = xMsgTopic.build("report");
xMsgMessage msg = xMsgMessage.createFrom(topic, "reportData");
actor.publish(msg);
actor.close();
```

Async subscriber:
```java
xMsg actor = new xMsg("subscriber");
xMsgTopic topic = xMsgTopic.build("report");
xMsgSubscription sub = actor.subscribe(topic, msg -> {
    System.out.println("Received: " + xMsgMessage.parseData(msg));
});
// subscription runs in background until actor is destroyed
```

Registration and discovery:
```java
xMsg actor = new xMsg("example");
xMsgTopic topic = xMsgTopic.build("report");
// register as publisher
actor.register(xMsgRegInfo.publisher(topic, "description"));
// find all subscribers to some topic
for (xMsgRegRecord reg : actor.discover(xMsgRegQuery.subscribers(topic))) {
    System.out.printf("%s: %s%n", reg.address(), reg.name());
}
actor.close();
```

A proxy server must be running in order to deliver messages between actors:
```
$ java org.jlab.coda.xmsg.sys.xMsgProxy
```

A registrar server must be running in order to register actors:
```
$ java org.jlab.coda.xmsg.sys.xMsgRegistrar
```


## Documentation

The reference documentation is available at <https://claraweb.jlab.org/xmsg/>.


## Installation

With Gradle:

```groovy
repositories {
    maven {
        url 'https://clasweb.jlab.org/clas12maven/'
    }
}

dependencies {
    compile 'org.jlab.coda:xmsg:2.4-SNAPSHOT'
}
```

With Maven:

```xml
<repositories>
   <repository>
      <id>clas12maven</id>
      <url>https://clasweb.jlab.org/clas12maven</url>
   </repository>
</repositories>

<dependencies>
   <dependency>
      <groupId>org.jlab.coda</groupId>
      <artifactId>xmsg</artifactId>
      <version>2.4-SNAPSHOT</version>
  </dependency>
</dependencies>
```


## Build notes

xMsg requires the Java 8 JDK.

#### Ubuntu

Support PPAs:

    $ sudo apt-get install software-properties-common

Install Oracle Java 8 from the
[Web Upd8 PPA](http://www.webupd8.org/2012/09/install-oracle-java-8-in-ubuntu-via-ppa.html):

    $ sudo add-apt-repository ppa:webupd8team/java
    $ sudo apt-get update
    $ sudo apt-get install oracle-java8-installer

Check the version:

    $ java -version
    java version "1.8.0_101"
    Java(TM) SE Runtime Environment (build 1.8.0_101-b13)
    Java HotSpot(TM) 64-Bit Server VM (build 25.101-b13, mixed mode)

You may need the following package to set Java 8 as default
(see the previous link for more details):

    $ sudo apt-get install oracle-java8-set-default

You can also set the default Java version with `update-alternatives`:

    $ sudo update-alternatives --config java

#### macOS

Install Oracle Java using [Homebrew](https://brew.sh/):

    $ brew cask install caskroom/versions/java8

Check the version:

    $ java -version
    java version "1.8.0_92"
    Java(TM) SE Runtime Environment (build 1.8.0_92-b14)
    Java HotSpot(TM) 64-Bit Server VM (build 25.92-b14, mixed mode)

### Build

To build xMsg use the provided [Gradle](https://gradle.org/) wrapper.
It will download the required Gradle version and all dependencies.

    $ ./gradlew

To run the integration tests:

    $ ./gradlew integration

To install the xMsg artifact to the local Maven repository:

    $ ./gradlew install

### Importing the project into your IDE

Gradle can generate the required configuration files to import the xMsg
project into [Eclipse](https://eclipse.org/ide/) and
[IntelliJ IDEA](https://www.jetbrains.com/idea/):

    $ ./gradlew cleanEclipse eclipse

    $ ./gradlew cleanIdea idea

See also the [Eclipse Buildship plugin](http://www.vogella.com/tutorials/EclipseGradle/article.html)
and the [Intellij IDEA Gradle Help](https://www.jetbrains.com/help/idea/2016.2/gradle.html).


## Authors

* Vardan Gyurjyan
* Sebastián Mancilla
* Ricardo Oyarzún

For assistance send an email to [clara@jlab.org](mailto:clara@jlab.org).
