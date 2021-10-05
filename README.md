# interference open cluster

##### pure-java opensource distributed database platform
(c) 2010 - 2021 head systems, ltd

current revision: release 2021.1
for detailed information see doc/InterferenceManual.pdf

contacts: info@interference.su
##### https://github.com/interference-project/interference
##### http://io.digital


## Concepts & features

###### i.o.cluster also known as interference open cluster is a simple java framework enables you to launch a distributed database and complex event processing service within your java application, using JPA-like interface and annotations for structure mapping and data operations. This software inherits its name from the interference project, within which its mechanisms were developed.

###### i.o.cluster is a opensource, pure java software.

The basic unit of the i.o.cluster service is a node - it can be a standalone running service, or a service running within some java application.

Each i.o.cluster node has own persistent storage and can considered and used as a local database with following basic features:

- runs in the same JVM with your application
- operates with simple objects (POJOs)
- uses base JPA annotations for object mapping directly to persistent storage
- supports SQL queries with READ COMMITTED isolation level
- supports transactions
- supports complex event processing and streaming SQL
- can be used as a local or distributed SQL database
- uses the simple and fast serialization
- uses persistent indices for fast access to data and increase performance of SQL joins
- allows flexible management of data in memory for stable operation of a node at any ratio of storage size / available memory, which allows, depending on the problem being solved, how to allocate all data directly in memory with a sufficient heap size, or use access to huge storages with a minimum heap size of java application

Nodes can be joined into a cluster, at the cluster level with inter-node interactions, we get the following features:
- allows you to insert data and run SQL queries from any node included in the cluster
- support of horizontal scaling SQL queries with READ COMMITTED isolation level
- support of transparent cluster-level transactions
- support of complex event processing (CEP) and simple streaming SQL
- i.o.cluster nodes does not require the launch of any additional coordinators

## Overview

Initially, the service was designed in such a way that each node is a java application that can be launched both by sharing one JVM with the client application using the service, or autonomously.

Each node uses its own storage and, being included in the cluster, replicates to other nodes all changes made on it, and also reflects changes made on other nodes.

You can start the service of each specific node inside an application and use fast access to data inside the node, as well as execute queries that will automatically scale to other nodes in the cluster.

Also from your java application, you can use remote client connections to the nodes of an existing cluster without the need to deploy a full service with its own storage (see Remote Client).

Each of the nodes includes several mechanisms that ensure its operation:

- core algorithms (supports structured persistent storage, supports indices, custom serialization, heap management, local and distributed sync processes)
- SQL and CEP processor
- event transport, which is used to exchange messages between nodes, as well as between a node and a client application
a brief diagram of the internal implementation of the service on the example of one node:

![Screenshot](doc/interference.png)

## Distributed persistent model

To include a node in the cluster, you must specify the full list of cluster nodes (excluding this one) in the cluster.nodes configuration parameter. The minimum number of cluster nodes is 2, and the maximum is 64 (for more details, see cluster configuration rules below).

After such configuration, we may start all configured nodes as cluster. In this case, all nodes will be use specific messages (events) for provide inter-node data consistency and horizontal-scaling queries.

Interference open cluster is a decentralized system. This means that the cluster does not use any coordination nodes; instead, each node follows to a set of some formal rules of behavior that guarantee the integrity and availability of data within a certain interaction framework.

Within the framework of these rules, all nodes of the Interference open cluster are equivalent. This means that there is no separation in the system of master and slave nodes - changes to user tables can be made from any node, also all changes are replicated to all nodes, regardless of which node they were made on.

Running commit in a local user session automatically ensures that the changed data is visible on all nodes in the cluster.

## Distribute rules

The concept of interference open cluster is based on a simple basic requirement, which can be literally expressed as follows: we must allow insertion and modification of data at the cluster level from any node, and we must allow data retrieval from any node, using as much as possible the computing resources of the cluster as a whole. Further, we accept the condition that all cluster nodes must be healthy and powered on, if any of the nodes has been turned off for a while, it will not be turned on to receive data until her storage is synchronized with other nodes. In practice, in the absence of changes in the moment, this means that there are identical copies of the storage on the cluster nodes. To prevent conflicts of changes in cluster, several lock modes are used:

- table level (a session on a node locks the entire table)
- frame level (a session on a node locks a frame)
- disallowed changes for non-owner nodes

here it is necessary to explain in more detail: all data inserts on a certain node are performed into a frame which was allocated on the same node, for which, in turn, the node is the owner. This is done so that when there are simultaneous inserts into a table from several nodes at once, there are no conflicts during replication. Subsequently, this distinction allows us to understand whether or not to request permission to change the data in the frame at the cluster level or not. Moreover, it allows us to implement a mode when changes to frames on a non-owner node are prohibited. This mode is used on cluster nodes if one or more other nodes become unavailable (we cannot know for certain whether the node is down or there is a problem in the network connection).

Thus, let's repeat again:

- all cluster nodes should be equivalent
- all changes on any of the nodes are mapped to other nodes immediately
- data inserts are performed in local storage structure, and then the changes are replicated to other nodes.
- if replication is not possible (the node is unavailable or the connection is broken), a persistent change queue is created for this node
- the owner of any data frame is the node on which this frame has been allocated
- data changes in node own dataframe are performed immediately, else, performed distributed lock for dataframe on cluster level
- if cluster is failed (some node are offline or connection broken), all data changes are not allowed or changes in only node own dataframes allowed
- the cluster uses the generation of unique identifiers for entities (@DistributedId annotation) so that the identifier is unique within the cluster, but not just within the same node
- the cluster does not use any additional checks for uniqueness, requiring locks at the cluster level
 
## SQL horizontal-scaling queries

All SQL queries called on any of the cluster nodes will be automatically distributed among the cluster nodes for parallel processing, if such a decision is made by the node based on the analysis of the volume of tasks (the volume of the query tables is large enough, etc.)
If during the processing of a request a node is unavailable, the task distributed for this node will be automatically rescheduled to another available node.
 
## Complex event processing concepts

So, we must allow insertion and modification of data at the cluster level from any node, and we must allow data retrieval from any node, using as much as possible the computing resources of the cluster as a whole.

The next concept of interference open cluster is that any table is at the same time a queue, in particular, using the SELECT STREAM clause, we can retrieve records in exactly the same order in which they were added. In general, at the cluster level, the session.persist() operation can be considered as publishing a persistent event. Based on our basic distribution rules, we send this event to all nodes.

Interference open cluster does not currently support the standard DML UPDATE and DELETE operations, instead for bulk table processing (including the optional WHERE clause) we have implemented PROCESS and PROCESS STREAM clauses that allow us to process each record from a selection of one of the EventProcessor interface implementations.

On the one hand, this approach allows us to obtain results similar to those that we would achieve using UPDATE and DELETE, on the other hand, it significantly expands the possibilities for custom processing of records, allowing full event processing. For the sake of fairness, it is need noting that you can get similar results using standard SELECT and SELECT STREAM, using some custom code to process the result set, but PROCESS and PROCESS STREAM implement processing at the core level of the cluster, which significantly improve the performance, second, this statements are launched at the cluster level and provide a ready-made implementation for distributed event processing.

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
-verbose:gc
-Xloggc:/ioclustergc.log
-XX:+PrintGCDetails
-XX:+PrintGCDateStamps
-XX:+AggressiveOpts
-Xms1G
-Xmx4G
-XX:MaxMetaspaceSize=256m
-XX:+UseStringDeduplication
-XX:ParallelGCThreads=4
-XX:ConcGCThreads=2
-Dlogback.configurationFile=config/app-log-config.xml
-Dcom.sun.management.jmxremote 
-Dcom.sun.management.jmxremote.port=8888
-Dcom.sun.management.jmxremote.authenticate=false 
-Dcom.sun.management.jmxremote.ssl=false
```

