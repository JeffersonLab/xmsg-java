# xMsg

xMsg is a lightweight, yet full featured publish/subscribe messaging system,
presenting asynchronous publish/subscribe inter-process communication
protocol: an API layer in Java, Python and C++.

xMsg provides in memory registration database that is used to register xMsg
actors (i.e. publishers and subscribers). Hence, xMsg API includes methods for
registering and discovering publishers and subscribers. This makes xMsg a
suitable framework to build symmetric SOA based applications. For example a
services that has a message to publishing can check to see if there are enough
subscribers of this particular message type.

To solve dynamic discovery problem in pub/sub environment the need of a proxy
server is unavoidable. xMsg is using 0MQ socket libraries and borrows 0MQ
proxy, which is a simple stateless message switch to address mentioned dynamic
discovery problem.

xMsg stores proxy connection objects internally in a connection pool for
efficiency reasons. To avoid proxy connection concurrency, thus achieving
maximum performance, connection objects are not shared between threads. Each
xMsg actor tread will reuse an available connection object, or create a new
proxy connection if it is not available in the pool.

xMsg publisher can send a message of any topic. xMsg subscribers subscribe
to abstract topics and provide callbacks to handle messages as they arrive,
in a so called subscribe-and-forget mode. Neither publisher nor subscriber
knows of each others existence. Thus publishers and subscribers are completely
independent of each others. Yet, for a proper communication they need to
establish some kind of relationship or binding, and that binding is the
communication or message topic. Note that multiple xMsg actors can
communicate without interfering with each other via simple topic
naming conventions. xMsg topic convention defines three parts: _domain_,
_subject_, and _type_, presented by the xMsgTopic class.

xMsg subscriber callbacks (implementing xMsgCallBack interface) will run in a
separate thread. For that reason xMsg provides a thread pool, simplifying the
job of a user. Note that user provided callback routines must be thread safe
and/or thread enabled.

In a conclusion we present the xMsg entire API

    createConnection
    getConnection
    releaseConnection
    destroyConnection
    publish
    syncPublish
    subscribe
    unsubscribe
    register
    deregister
    discover

For more details and API method signatures check the Javadoc.

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

Install Oracle Java using [Homebrew](http://brew.sh/):

    $ brew cask install java

Check the version:

    $ java -version
    java version "1.8.0_92"
    Java(TM) SE Runtime Environment (build 1.8.0_92-b14)
    Java HotSpot(TM) 64-Bit Server VM (build 25.92-b14, mixed mode)

### Installation

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

For assistance contact authors:

* Vardan Gyurjyan    (<gurjyan@jlab.org>)
* Sebastián Mancilla (<smancill@jlab.org>)
* Ricardo Oyarzún    (<oyarzun@jlab.org>)
