# interference

##### java-based distributed database platform
###### (c) 2010 - 2021 head systems, ltd
###### current revision: release 2021.1
###### for detailed information see:
###### http://io.digital and doc/InterferenceManual.pdf
###### contacts: info@inteference.su
##### https://github.com/interference-project/interference


## Concepts & features

- runs in the same JVM with your application
- operates with simple objects (POJOs)
- uses base JPA annotations for object mapping directly to persistent storage
- supports horizontal scaling SQL queries
- supports transactions
- supports complex event processing (CEP) and simple streaming SQL
- can be used as a local or distributed SQL database
- allows you to inserts data and run SQL queries from any node included in the cluster
- does not require the launch of any additional coordinators
- uses the simple and fast serialization
- uses indices for fast access to data and increase performance of SQL joins

## NOTE:

Interference is not a RDBMS in the classical sense, and it does 
not support ddl operations (the table structure is created on the basis 
of @Entity class JPA-compatible annotations). 

## Quick Start Application

The interference-test application shows example of using the basic 
interference use cases. Before starting and using, read the manual.

Consider a basic example when the interference service used as a 
local persistent layer of the application and runs in the same JVM 
with the application.

To get started with interference, you need to download sources of 
the current interference release (2021.1), build it and install it 
into your local maven repository (mvn install).
include the interference.jar library in your project configuration. 
For maven pom.xml, this might look like this:

```
<dependencies>
    <dependency>
        <groupId>su.interference</groupId>
        <artifactId>interference</artifactId>
        <version>2021.1</version>
    </dependency>
    ...
</dependencies>
```

Next, specify the necessary set of keys in the project 
(application) settings (jmxremote settings is optional):

```
-Dsu.interference.config=interference.properties
-Dcom.sun.management.jmxremote 
-Dcom.sun.management.jmxremote.port=8888
-Dcom.sun.management.jmxremote.local.only=false 
-Dcom.sun.management.jmxremote.authenticate=false 
-Dcom.sun.management.jmxremote.ssl=false
-Xms256m
-Xmn512m
-Xmx4g
-XX:MaxMetaspaceSize=256m
-XX:ParallelGCThreads=8
-XX:ConcGCThreads=4
```

