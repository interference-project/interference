# interference
simple distributed persistent layer for java applications
(c) 2019 head systems, ltd
                          
current revision: 2019.3
for detailed information see docs/InterferenceManual.pdf
contacts: info@inteference.su
https://github.com/interference-project/inteference

Concepts & features

- supports Base JPA annotations
- supports distributed SQL queries
- supports transactions
- runs in the same JVM with local application
- can be used as a local or distributed SQL database
- can be used as persistent layer for a distributed application
- operates with simple objects (POJOs), annotated primarily 
  according to the JPA specification
- allows you to make changes to data and query data from any node 
  included in the cluster
- does not contain any coordination nodes and does not require 
  the launch of any additional coordinators. All cluster nodes are equivalent.
- supports complex event processing and streaming SQL (in next release)


Quick Start Application

The interference-test application shows example of using the basic 
interference use cases. Before starting and using, read the manual.

Consider a basic example when the interference service used as a 
local persistent layer of the application and runs in the same JVM 
with the application.

To get started with interference, you need to include the interference.jar 
library in your project configuration. For maven pom.xml, this might look 
like this:

<dependencies>
    <dependency>
        <groupId>su.interference</groupId>
        <artifactId>interference</artifactId>
        <version>2019.3</version>
    </dependency>
    ...
</dependencies>

Next, specify the necessary set of keys in the project 
(application) settings (jmxremote settings is optional):

-Dsu.interference.config=interference.properties
-Dcom.sun.management.jmxremote 
-Dcom.sun.management.jmxremote.port=8888
-Dcom.sun.management.jmxremote.local.only=false 
-Dcom.sun.management.jmxremote.authenticate=false 
-Dcom.sun.management.jmxremote.ssl=false

To run a single local interference node, you can use the standard 
supplied interference.properties configuration. Note that file 
interference.properies should be within /config subdirectory. 
Next, see the configuration section.

Then, add following code into initializing section of your java application:

Instance instance = Instance.getInstance();
Session session = Session.getSession();
instance.startupInstance(session);

where Instance is su.inteference.core.Instance 
and Session is su.interference.persistent.Session.


Service as standalone

This option can be used when the cluster node is used solely for the purpose of further horizontal scaling of the data retrieving mechanism:

java -cp interference.jar 
-Dsu.interference.config=interference.properties
-Dcom.sun.management.jmxremote 
-Dcom.sun.management.jmxremote.port=8888 
-Dcom.sun.management.jmxremote.local.only=false 
-Dcom.sun.management.jmxremote.authenticate=false 
-Dcom.sun.management.jmxremote.ssl=false 
su.interference.standalone.Start

