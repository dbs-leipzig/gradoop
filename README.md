### Gradoop : Graph Analytics on Apache Hadoop
***

#### Setup development environment

##### Development requirements

* Maven 3
* JDK 7 (Oracle or OpenJDK)

##### Cluster requirements

If you want to run Gradoop on a cluster, you need the following components
installed.

*   Hadoop 2.5.1
*   HBase 0.98.11-hadoop2

##### Setup Gradoop

* Clone Gradoop to your local file system

    > git clone https://github.com/s1ck/gradoop.git
    
* Build and run tests

    > cd gradoop
    
    > mvn clean install

##### Running example pipelines

###### BIIIG

The BIIIG example pipeline is used to analyze business data represented as a graph.
It consists of multiple steps:

1.  Bulk Load a data set into Gradoop
2.  Identify subgraphs using Giraph
3.  Select a (sub)set of subgraphs based on an UDF using MapReduce
4.  Aggregate these graphs based on an UDF using MapReduce
5.  Store the aggregated result as a graph property

The pipeline itself is currently represented by a Hadoop Driver
(org.gradoop.biiig.examples.BTGAnalysisDriver). Please have a look at the driver
for further details on how to implement a pipeline.

To run the pipeline on your hadoop installation, please follow these steps.

*   Copy the generated `gradoop-examples/target/gradoop-examples-<version>-jar
-with-dependencies.jar`
    to your Hadoop environment.

*   For a list of options for that example pipeline call

    > $HADOOP_PREFIX/bin/hadoop jar gradoop-examples-&lt;version&gt;
    -jar-with-dependencies.jar org.gradoop.biiig.examples.BTGAnalysisDriver --help

*   The following call runs the pipeline on a given input graph (foodbroker) using 11 giraph workers, 11 reducers and a hbase scan cache of 500 rows.

    > $HADOOP_PREFIX/bin/hadoop jar gradoop-examples-&lt;version&gt; 
    -jar-with-dependencies.jar org.gradoop.biiig.examples.BTGAnalysisDriver -libjars $HBASE_JARS -gip /user/hduser/input/foodbroker_10.graph -gop /user/hduser/output/hdfiles -w 11 -r 11 -sc 500

*   See [Foodbroker Repository](https://github.com/dbs-leipzig/foodbroker) if you want to generate your
    own graphs or implement a custom FileReader to load your own graph format.
    
###### RDF

The RDF example pipeline analyzes a given NTriple using following steps:

1. Bulk Load RDF NTriple file into Gradoop
2. Compute Connected Components using Giraph
3. For each component the contained vertices are counted using MapReduce.
4. Resulting graphs are written back to Gradoop


##### Importing data into Gradoop

*   Gradoop supports Bulk Loading graphs into the repository. The following
    command gives you a list of options

    > $HADOOP_PREFIX/bin/hadoop jar gradoop.jar org.gradoop.drivers.BulkLoadDriver --help

*   The bulk load supports user defined formats. You need to implement
    `org.gradoop.io.reader.VertexLineReader`, please have a look at
    `org.gradoop.io.reader.JsonReader` for an example.

##### Exporting data from Gradoop

*   Gradoop supports Bulk Writing graphs into the HDFS using MapReduce. The
    following command gives you a list of options

    > $HADOOP_PREFIX/bin/hadoop jar gradoop.jar org.gradoop.drivers.BulkWriteDriver --help

*   The bulk write supports user defined formats. You need to implement
    `org.gradoop.io.writer.VertexLineWriter`, please have a look at
    `org.gradoop.io.writer.JsonWriter` for an example.

#### Gradoop modules

##### gradoop-core

The main contents of that module are the Extended Property Graph data
model, the corresponding graph repository and its reference implementation for
Apache HBase.

Furthermore, the module contains the Bulk Load / Write drivers based on
MapReduce and file readers / writers for user defined file and graph formats.

##### gradoop-giraph

Contains graph algorithms and EPG-operators implemented with Apache Giraph. It
also contains various formats to read and write the graph from and to the
graph repository.

##### gradoop-mapreduce

Contains EPG-operators implemented with Apache MapReduce and I/O formats used
by these operator implementations.

##### gradoop-examples

Contains example pipelines showing use cases for Gradoop. 

*   BIIIG pipeline for business related graph data using specific data readers
*   RDF pipeline for semantic web analysis

##### gradoop-checkstyle

Used to maintain the codestyle for the whole project.

#### Developer notes

##### Code style for IntelliJ IDEA

*   copy codestyle from dev-support to your local IDEA config folder

    > cp dev-support/gradoop-idea-codestyle.xml ~/<your IDEA folder>/config/codeStyles

*   restart IDEA

*   `File -> Settings -> Code Style -> Java -> Scheme -> "Gradoop"`
    
##### Troubleshooting

* Exception while running test org.apache.giraph.io.hbase
.TestHBaseRootMarkerVertexFormat (incorrect permissions, see
http://stackoverflow.com/questions/17625938/hbase-minidfscluster-java-fails
-in-certain-environments for details)

    > umask 022

* Ubuntu + Giraph hostname problems. To avoid hostname issues comment the
following line in /etc/hosts

    `127.0.1.1   <your-host-name>`




