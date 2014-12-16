### Gradoop : Graph Analytics Framework on Apache Hadoop
***

#### Setup development environment

##### Requirements

* Maven 3
* JDK 7 (Oracle or OpenJDK)

##### Setup Gradoop

* clone Gradoop to your local file system

    `git clone https://github.com/s1ck/gradoop.git`
    
* build and run tests

    `cd gradoop`
    
    `mvn clean install`

##### Code style for IntelliJ IDEA

* copy codestyle from dev-support to your local IDEA config folder

    `cp dev-support/gradoop-idea-codestyle.xml ~/<your IDEA folder>/config/codeStyles`
    
* restart IDEA
* `File -> Settings -> Code Style -> Java -> Scheme -> "Gradoop"`

##### Troubleshooting

* Exception while running test org.apache.giraph.io.hbase
.TestHBaseRootMarkerVertextFormat (incorrect permissions, see
http://stackoverflow.com/questions/17625938/hbase-minidfscluster-java-fails
-in-certain-environments for details)

    `umask 022`

* Ubuntu + Giraph hostname problems. To avoid hostname issues comment the
following line in /etc/hosts

    `127.0.1.1   <your-host-name>`

#### Gradoop modules

##### gradoop-core

The main contents of that module are the Extended Property Graph data
model, the corresponding graph repository and its reference implementation for
Apache HBase.

Furthermore, the module contains the HBase Bulk Loader based on MapReduce and
 file reader for user defined file/graph formats.

##### gradoop-giraph

Contains graph algorithms and EPG-operators implemented with Apache Giraph. It
also contains various formats to read and write the graph from and to the
graph repository.

##### gradoop-mapreduce

Contains EPG-operators implemented with Apache MapReduce and I/O formats used
 by these operator implementations.

##### gradoop-biiig

Contains BIIIG-specific data readers and example analytical pipelines for
business related graph data.

##### gradoop-checkstyle

Used to maintain the codestyle for the whole project.
    





