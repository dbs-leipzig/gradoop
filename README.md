## Gradoop: Distributed Graph Analytics on Hadoop

Gradoop is an open source (GPLv3) research framework for scalable graph analytics.
It offers an extended property graph data model (EPGM) which extends the widespread
property graph model by the concept of logical subgraphs. By providing operators
for single graphs and collections of graphs, Gradoop offers a flexible way to express
various analytical workflows.

```java
// load social network from hdfs
EPGraph db = FlinkGraphStore.fromJsonFile(...).getDatabaseGraph();
// detect communities
EPGraphCollection communities = db.callForCollection(new LabelPropagation(...));
// filter large communities
EPGraphCollection communities = communities.select((EPGraph g) -> g.vertexCount() > 100);
// combine them to a single graph
EPGraph relevantSubgraph = communities.reduce((EPGraph g1, EPGraph g2) -> g1.combine(g2));
// summarize the network based on the city users live in
EPGraph summarizedGraph = relevantSubgraph.summarize("city")
// write back to HDFS
summarizedGraph.writeAsJson(...);
```

Gradoop is **work in progress** which means APIs may change. It is currently used
as a proof of concept implementation and far from production ready.

## Data Model

Besides vertices and directed edges, the model supports the concept of logical 
subgraphs with possibly overlapping vertex and edge sets. This way, it is possible 
to define analytical operations between logical graphs. Vertices, edges and logical
subgraphs have a single label and may have multiple properties in the form of 
key-value-pairs. EPGM does not enforce any kind of schema at graph elements.

### Graph operators

Our operator implementations are based on [Apache Flink](http://flink.apache.org/).
The following table contains an overview.

| Operator      | Input               | Output         | Output description                               | Implemented  |
|:--------------|:--------------------|:----------------|:------------------------------------------------|:------------:|
| Selection     | GraphCollection     | GraphCollection | Graphs that fulfil a predicate function         | Yes          |
| Distinct      | GraphCollection     | GraphCollection | No duplicate graphs                             | No           |
| SortBy        | GraphCollection     | GraphCollection | Graphs sorted by graph property                 | No           |
| Top           | GraphCollection     | GraphCollection | The first n elements of the input collection    | No           |
| Union         | GraphCollection (2) | GraphCollection | All graphs from both collections                | Yes          |
| Intersection  | GraphCollection (2) | GraphCollection | Only graphs that exist in both collections      | Yes          |
| Difference    | GraphCollection (2) | GraphCollection | Only graphs that exist in the one collection    | Yes          |
| Combination   | Graph (2)           | Graph           | Vertices and edges from both graphs             | Yes          |
| Overlap       | Graph (2)           | Graph           | Vertices and edges that exist in both graphs    | Yes          |
| Exclusion     | Graph (2)           | Graph           | Vertices and edges that exist in only one graph | Yes          |
| Pattern Match | Graph               | GraphCollection | Graphs that fulfil a given pattern              | No           |
| Aggregation   | Graph               | Graph           | Graph with result of an aggregate function      | Yes          |
| Projection    | Graph               | Graph           | Graph with projected vertex and edge sets       | No           |
| Summarization | Graph               | Graph           | Structural condense of the input graph          | Yes          |
| Apply         | Graph               | Graph           | Structural condense of the input graph          | Yes          |

## Setup

### Build gradoop from source

* Clone Gradoop into your local file system

    > git clone https://github.com/dbs-leipzig/gradoop.git
    
* Build and execute tests

    > cd gradoop
    
    > mvn clean install

### Load data into gradoop

Gradoop supports json as input format for vertices, edges and graphs. Each document
stores the properties of the specific instance in an embedded document `data`. Meta
information, like the obligatory label, is stored in another embedded document `meta`.
The meta document of vertices and edges may contain a mapping to the logical graphs 
they are contained in.

Two users (Alice and Bob) that have three properties each, have an obligatory
vertex label (Person) and are contained in two logical graphs (0 and 2).
```
// content of nodes.json
{"id":0,"data":{"gender":"f","city":"Leipzig","name":"Alice"},"meta":{"label":"Person","graphs":[0,2]}}
{"id":1,"data":{"gender":"m","city":"Leipzig","name":"Bob"},"meta":{"label":"Person","graphs":[0,2]}}
```

Edges are represented in similar way. Alice and Bob are connected by an edge (knows).
Edges may have properties (e.g., `since:2014`) and may also be contained in logical graphs (0 and 2).

```
// content of edges.json
{"id":0,"source":0,"target":1,"data":{"since":2014},"meta":{"label":"knows","graphs":[0,2]}}
{"id":1,"source":1,"target":0,"data":{"since":2014},"meta":{"label":"knows","graphs":[0,2]}}
```

Graphs may also have properties and must have a label (e.g., Community).

```
// content of graphs.json
{"id":0,"data":{"interest":"Databases","vertexCount":3},"meta":{"label":"Community"}}
{"id":1,"data":{"interest":"Hadoop","vertexCount":3},"meta":{"label":"Community"}}
{"id":2,"data":{"interest":"Graphs","vertexCount":4},"meta":{"label":"Community"}}
```

### Example: Extract schema graph from possibly large-scale graph

```java
EPGraphStore graphStore = FlinkGraphStore.fromJsonFile(vertexInputPath, edgeInputPath, env);
EPGraph schemaGraph = graphStore.getDatabaseGraph().summarizeOnVertexAndEdgeLabels();
schemaGraph.writeAsJson(vertexOutputPath, edgeOutputPath, graphOutputPath);
```

### Cluster deployment

If you want to execute Gradoop on a cluster, you need *Hadoop 2.5.1* and 
*Flink 0.9.0 for Hadoop 2.4.1* installed and running.

* start a flink yarn-session (e.g. 5 Task managers with 4GB RAM and 4 processing slots each) 

> ./bin/yarn-session.sh -n 5 -tm 4096 -s 4

* run your program (e.g. the summarization example)

> ./bin/flink run -c org.gradoop.examples.Summarization ~/gradoop-flink-0.0.2-jar-with-dependencies.jar --vertex-input-path hdfs:///nodes.json --edge-input-path hdfs://edges.json --use-vertex-labels --use-edge-labels
    
## Gradoop modules

### gradoop-core

The main contents of that module are the Extended Property Graph Data
Model, the corresponding graph repository and its reference implementation for
Apache HBase.

Furthermore, the module contains the Bulk Load / Write drivers based on
MapReduce and file readers / writers for user defined file and graph formats.

### gradoop-flink

This module contains a reference implementation of the EPGM including the data
model and its operators. The concepts of the EPGM are mapped to the property
graph data model offered by Flink Gelly and additional Flink Datasets. The
gradoop operator implementations leverage existing Flink and Gelly operators.

### gradoop-examples

Contains example pipelines showing use cases for Gradoop. 

*   Graph summarization (build structural aggregates of property graphs)

### gradoop-checkstyle

Used to maintain the codestyle for the whole project.

## Developer notes

### Code style for IntelliJ IDEA

*   copy codestyle from dev-support to your local IDEA config folder

    > cp dev-support/gradoop-idea-codestyle.xml ~/.IntelliJIdea14/config/codeStyles

*   restart IDEA

*   `File -> Settings -> Code Style -> Java -> Scheme -> "Gradoop"`
    
### Troubleshooting

* Exception while running test org.apache.giraph.io.hbase
.TestHBaseRootMarkerVertexFormat (incorrect permissions, see
http://stackoverflow.com/questions/17625938/hbase-minidfscluster-java-fails
-in-certain-environments for details)

    > umask 022

* Ubuntu + Giraph hostname problems. To avoid hostname issues comment the
following line in /etc/hosts

    `127.0.1.1   <your-host-name>`
    
* And add your hostname to the localhost entry

    `127.0.0.1  localhost <your-host-name>`




