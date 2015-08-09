## Gradoop: Distributed Graph Analytics on Hadoop

[Gradoop](http://www.gradoop.com) is an open source (GPLv3) research framework 
for scalable graph analytics. It offers a graph data model which extends the widespread 
[property graph model](https://github.com/tinkerpop/blueprints/wiki/Property-Graph-Model) 
by the concept of logical subgraphs and further provides operators that can be applied 
on single graphs and collections of graphs. The combination of these operators allows
the flexible, declarative definition of graph analytical workflows.

```java
// load social network from hdfs
LogicalGraph db = EPGMDatabase.fromJsonFile("hdfs://...").getDatabaseGraph();
// detect communities
GraphCollection communities = db.callForCollection(new LabelPropagation(...));
// filter large communities
GraphCollection communities = communities.select((LogicalGraph g) -> g.vertexCount() > 100);
// combine them to a single graph
LogicalGraph relevantSubgraph = communities.reduce((LogicalGraph g1, LogicalGraph g2) -> g1.combine(g2));
// summarize the network based on the city users live in
LogicalGraph summarizedGraph = relevantSubgraph.summarize("city");
// write back to HDFS
summarizedGraph.writeAsJson("hdfs://...");
```

Gradoop is **work in progress** which means APIs may change. It is currently used
as a proof of concept implementation and far from production ready.

## Data Model

In the extended property graph model (EPGM), a database consists of multiple 
property graphs which are called logical graphs. These graphs are 
application-specific subsets from shared sets of vertices and edges, i.e., may
have common vertices and edges. Additionally, not only vertices and edges but
also logical graphs have a type label and can have different properties.

### Graph operators

The EPGM provides operators for both single graphs as well as collections of 
graphs; operators may also return single graphs or graph collections.

Our operator implementations are based on [Apache Flink](http://flink.apache.org/).
The following table contains an overview (GC = GraphCollection, G = Graph).

| Operator      | Definition    | Output description                              | Implemented  |
|:--------------|:--------------|:------------------------------------------------|:------------:|
| Selection     | GC -> GC      | Graphs that fulfil a predicate function         | Yes          |
| Distinct      | GC -> GC      | No duplicate graphs                             | No           |
| SortBy        | GC -> GC      | Graphs sorted by graph property                 | No           |
| Top           | GC -> GC      | The first n elements of the input collection    | No           |
| Union         | GC x GC -> GC | All graphs from both collections                | Yes          |
| Intersection  | GC x GC -> GC | Only graphs that exist in both collections      | Yes          |
| Difference    | GC x GC -> GC | Only graphs that exist in one collection        | Yes          |
| Combination   | G x G -> G    | Vertices and edges from both graphs             | Yes          |
| Overlap       | G x G -> G    | Vertices and edges that exist in both graphs    | Yes          |
| Exclusion     | G x G -> G    | Vertices and edges that exist in only one graph | Yes          |
| Pattern Match | G -> GC       | Graphs that match a given graph pattern         | No           |
| Aggregation   | G -> G        | Graph with result of an aggregate function      | Yes          |
| Projection    | G -> G        | Graph with projected vertex and edge sets       | No           |
| Summarization | G -> G        | Structural condense of the input graph          | Yes          |
| Apply         | GC -> GC      | Applies operator to each graph in collection    | No           |
| Reduce        | GC- > G       | Reduces collection to graph using operator      | No           |

## Setup

### Build gradoop from source

* Clone Gradoop into your local file system

    > git clone https://github.com/dbs-leipzig/gradoop.git
    
* Build and execute tests

    > cd gradoop
    
    > mvn clean install

### Load data into gradoop

Gradoop supports JSON as input format for vertices, edges and graphs. Besides the 
unique id, each JSON document stores the properties of the specific entity in an 
embedded document `data`. Meta information, like the obligatory label, is stored
in a second embedded document `meta`. The meta document of vertices and edges may 
contain a mapping to the logical graphs they are contained in.

Two persons (Alice and Bob) that have three properties each and are contained in 
two logical graphs (`"graphs":[0,2]`).
```
// content of nodes.json
{"id":0,"data":{"gender":"f","city":"Leipzig","name":"Alice"},"meta":{"label":"Person","graphs":[0,2]}}
{"id":1,"data":{"gender":"m","city":"Leipzig","name":"Bob"},"meta":{"label":"Person","graphs":[0,2]}}
```

Edges are represented in a similar way. Alice and Bob are connected by an edge
(knows). Edges may have properties (e.g., `"since":2014`) and may also be contained 
in logical graphs. Additionally, edge JSON documents store the obligatory source and 
target vertex identifier.

```
// content of edges.json
{"id":0,"source":0,"target":1,"data":{"since":2014},"meta":{"label":"knows","graphs":[0,2]}}
```

Graphs may also have properties and must have a label (e.g., Community).

```
// content of graphs.json
{"id":0,"data":{"interest":"Databases","vertexCount":3},"meta":{"label":"Community"}}
{"id":1,"data":{"interest":"Hadoop","vertexCount":3},"meta":{"label":"Community"}}
{"id":2,"data":{"interest":"Graphs","vertexCount":4},"meta":{"label":"Community"}}
```

### Example: Extract schema graph from possibly large-scale graph

In this example, we use the `summarize` operator to create a condensed version 
of our input graph. By summarizing on vertex and edge labels, we compute the schema
of our graph. Each vertex in the resulting graph represents all vertices with the
same label (e.g., Person or Group), each edge represents all edges with the same
label that connect vertices from the same vertex groups.

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

* run your program (e.g. the included Summarization example)

> ./bin/flink run -c org.gradoop.examples.Summarization ~/gradoop-flink-0.0.2-jar-with-dependencies.jar --vertex-input-path hdfs:///nodes.json --edge-input-path hdfs://edges.json --use-vertex-labels --use-edge-labels
    
## Gradoop modules

### gradoop-core

The main contents of that module are the Extended Property Graph Data
Model, the corresponding graph repository and its reference implementation for
Apache HBase (wip).

Furthermore, the module contains the Bulk Load / Write drivers based on
MapReduce and file readers / writers for user defined file and graph formats.

### gradoop-flink

This module contains a reference implementation of the EPGM including the data
model and its operators. The concepts of the EPGM are mapped to the property
graph data model offered by 
[Flink Gelly](https://ci.apache.org/projects/flink/flink-docs-master/libs/gelly_guide.html) 
and additional Flink Datasets. The gradoop operator implementations leverage 
existing Flink and Gelly operators.

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

### Version History

* 0.0.1 first prototype using Hadoop MapReduce and Apache Giraph for operator
 processing
* 0.0.2 support for HBase as distributed graph storage
* 0.0.3 Apache Flink replaces MapReduce and Giraph as operator implementation
 layer and distributed execution engine




