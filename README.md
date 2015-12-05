[![Build Status](https://travis-ci.org/dbs-leipzig/gradoop.svg?branch=master)](https://travis-ci.org/dbs-leipzig/gradoop)

## Gradoop: Distributed Graph Analytics on Hadoop

[Gradoop](http://www.gradoop.com) is an open source (GPLv3) research framework 
for scalable graph analytics built on top of [Apache Flink](http://flink.apache.org/)
and [Apache HBase](http://hbase.apache.org/). It offers a graph data model which 
extends the widespread 
[property graph model](https://github.com/tinkerpop/blueprints/wiki/Property-Graph-Model) 
by the concept of logical graphs and further provides operators that can be applied 
on single logical graphs and collections of logical graphs. The combination of these 
operators allows the flexible, declarative definition of graph analytical workflows.
Gradoop can be easily integrated in a workflows which already use Flink operators
and Flink libraries (i.e. Gelly, ML and Table).

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
application-specific subsets from shared sets of vertices and edges, i.e. may
have common vertices and edges. Additionally, not only vertices and edges but
also logical graphs have a type label and can have different properties.

Data Model elements (logical graphs, vertices and edges) have a unique identifier, 
a single label (e.g. User) and a number of key-value properties (e.g. name = Alice).
There is no schema involved, meaning each element can have an arbitrary number of
properties even if they have the same label.

### Graph operators

The EPGM provides operators for both single graphs as well as collections of
graphs; operators may also return single graphs or graph collections.

The following table contains an overview (GC = GraphCollection, G = Graph).

| Operator      | In      | Out           | Output description                                                      | Impl |
|:--------------|:--------|:--------------|:------------------------------------------------------------------------|:----:|
| Selection     | GC      | GC            | Filter graphs based on their attached data (i.e. label, properties)     | Yes  |
| Distinct      | GC      | GC            | No duplicate graphs                                                     | No   |
| SortBy        | GC      | GC            | Graphs sorted by a given graph property key                             | No   |
| Top           | GC      | GC            | The first n elements of the input collection                            | No   |
| Union         | GC x GC | GC            | All graphs from both input collections                                  | Yes  |
| Intersection  | GC x GC | GC            | Only graphs that exist in both collections                              | Yes  |
| Difference    | GC x GC | GC            | Only graphIds that exist in one collection                              | Yes  |
| Equality      | GC x GC | {true, false} | Compare collections in terms of contained element data or identifiers   | Yes  |
| Combination   | G x G   | G             | Graph with vertices and edges from both input graphs                    | Yes  |
| Overlap       | G x G   | G             | Graph with vertices and edges that exist in both input graphs           | Yes  |
| Exclusion     | G x G   | G             | Graph with vertices and edges that exist only in the first graph        | Yes  |
| Equality      | G x G   | {true, false} | Compares graphs in terms of contained element data or identifiers       | Yes  |
| Pattern Match | G       | GC            | Graphs that match a given graph pattern                                 | No   |
| Aggregation   | G       | G             | Graph with result of an aggregate function as a new property            | Yes  |
| Projection    | G       | G             | Graph with projected vertex and edge sets                               | Yes  |
| Summarization | G       | G             | Structural condense of the input graph                                  | Yes  |
| Apply         | GC      | GC            | Applies operator to each graph in collection                            | No   |
| Reduce        | GC      | G             | Reduces collection to graph using operator                              | Yes  |

## Setup

### Build gradoop from source

* Clone Gradoop into your local file system

    > git clone https://github.com/dbs-leipzig/gradoop.git
    
* Build and execute tests

    > cd gradoop
    
    > mvn clean install

### Load data into gradoop

#### JSON

Gradoop supports JSON as input format for vertexIds, edgeIds and graphIds. Besides the
unique id, each JSON document stores the properties of the specific entity in an 
embedded document `data`. Meta information, like the obligatory label, is stored
in a second embedded document `meta`. The meta document of vertexIds and edgeIds may
contain a mapping to the logical graphIds they are contained in.

Two persons (Alice and Bob) that have three properties each and are contained in 
two logical graphIds (`"graphIds":[0,2]`).
```
// content of hdfs:///input/nodes.json
{"id":0,"data":{"gender":"f","city":"Leipzig","name":"Alice"},"meta":{"label":"Person","graphIds":[0,2]}}
{"id":1,"data":{"gender":"m","city":"Leipzig","name":"Bob"},"meta":{"label":"Person","graphIds":[0,2]}}
```

Edges are represented in a similar way. Alice and Bob are connected by an edge
(knows). Edges may have properties (e.g., `"since":2014`) and may also be contained 
in logical graphIds. Additionally, edge JSON documents store the obligatory source and
target vertex identifier.

```
// content of hdfs:///input/edgeIds.json
{"id":0,"source":0,"target":1,"data":{"since":2014},"meta":{"label":"knows","graphIds":[0,2]}}
```

Graphs may also have properties and must have a label (e.g., Community).

```
// content of hdfs:///input/graphIds.json
{"id":0,"data":{"interest":"Databases","vertexCount":3},"meta":{"label":"Community"}}
{"id":1,"data":{"interest":"Hadoop","vertexCount":3},"meta":{"label":"Community"}}
{"id":2,"data":{"interest":"Graphs","vertexCount":4},"meta":{"label":"Community"}}
```

#### HBase

Gradoop can read and write an EPGM database from HBase using an EPGM store. The
current implementation is work in progress, at the moment one can read or write
the whole database. We are working on reading only data that is needed for the
analysis (e.g., a collection of specific communities).

The following example shows how to create an EPGM Store and how to write an EPGM
database to it.

```java
EPGMDataBase epgmDB = EPGMDatabase.fromJsonFile(...);

// do some fancy analysis ...

EPGMStore epgmStore = HBaseEPGMStore.createOrOpenEPGMStore(vertexTable, edgeTable, graphHeadTable);
epgmDB.writeToHBase(epgmStore);
epgmStore.close();
```

You can now read the database from HBase.

```java
EPGMStore epgmStore = HBaseEPGMStore.createOrOpenEPGMStore(vertexTable, edgeTable, graphHeadTable);
EPGMDatabase epgmDB = EPGMDatabase.fromHBase(epgmStore);

// do some fancy analysis ...

epgmStore.close()
```

### Example: Extract schema graph from possibly large-scale graph

In this example, we use the `summarize` operator to create a condensed version 
of our input graph. By summarizing on vertex and edge labels, we compute the schema
of our graph. Each vertex in the resulting graph represents all vertexIds with the
same label (e.g., Person or Group), each edge represents all edgeIds with the same
label that connect vertexIds from the same vertex groups.

```java
String vertexInputPath = "hdfs:///input/nodes.json";
String edgeInputPath = "hdfs:///input/edgeIds.json";
String graphInputPath = "hdfs:///input/graphIds.json";
EPGMDatabase db = EPGMDatabase.fromJsonFile(vertexInputPath, edgeInputPath, graphInputPath, env);
LogicalGraph schemaGraph = db.getDatabaseGraph().summarizeOnVertexAndEdgeLabels();
schemaGraph.writeAsJson(vertexOutputPath, edgeOutputPath, graphOutputPath);
```

### Cluster deployment

If you want to execute Gradoop on a cluster, you need *Hadoop 2.5.1* and 
*Flink 0.9.0 for Hadoop 2.4.1* installed and running.

* start a flink yarn-session (e.g. 5 Task managers with 4GB RAM and 4 processing slots each) 

> ./bin/yarn-session.sh -n 5 -tm 4096 -s 4

* run your program (e.g. the included Summarization example)

> ./bin/flink run -c org.gradoop.examples.SummarizationExample ~/gradoop-flink-0.0.2-jar-with-dependencies.jar --vertex-input-path hdfs:///nodes.json --edge-input-path hdfs://edgeIds.json --use-vertex-labels --use-edge-labels
    
## Gradoop modules

### gradoop-core

The main contents of that module are the Extended Property Graph Data
Model and a corresponding POJO implementation which is used in Flink. The
persistent representation of the EPGM is also contained in core and its mapping
to Apache HBase.

### gradoop-flink

This module contains reference implementations of the EPGM operators. The 
concepts of the EPGM are mapped to Flink DataSets and processed using Flink 
operators.

### gradoop-examples

Contains example pipelines showing use cases for Gradoop. 

*   Graph summarization (build structural aggregates of property graphs)

### gradoop-algorithms

Contains implementations of general graph algorithms (e.g. Label Propagation)
adapted to be used with the EPGM model.

### gradoop-checkstyle

Used to maintain the code style for the whole project.
    
### Version History

* 0.0.1 first prototype using Hadoop MapReduce and Apache Giraph for operator
 processing
* 0.0.2 support for HBase as distributed graph storage
* 0.0.3 Apache Flink replaces MapReduce and Giraph as operator implementation
 layer and distributed execution engine
* 0.1 Major refactoring of internal EPGM representation (e.g. ID and property handling), Equality Operators, GDL-based unit testing




