[![Build Status](https://travis-ci.org/dbs-leipzig/gradoop.svg?branch=master)](https://travis-ci.org/dbs-leipzig/gradoop)

## Gradoop: Distributed Graph Analytics on Hadoop

[Gradoop](http://www.gradoop.com) is an open source (GPLv3) research framework 
for scalable graph analytics built on top of [Apache Flink&trade;](http://flink.apache.org/) 
and [Apache HBase&trade;](http://hbase.apache.org/). It offers a graph data model which 
extends the widespread [property graph model](https://github.com/tinkerpop/blueprints/wiki/Property-Graph-Model) 
by the concept of logical graphs and further provides operators that can be applied 
on single logical graphs and collections of logical graphs. The combination of these 
operators allows the flexible, declarative definition of graph analytical workflows.
Gradoop can be easily integrated in a workflow which already uses Flink&trade; operators
and Flink&trade; libraries (i.e. Gelly, ML and Table).

Gradoop is **work in progress** which means APIs may change. It is currently used
as a proof of concept implementation and far from production ready.

##### Further Information

* [Extended Property Graphs with Apache Flink, SIGMOD NDA Workshop, June 2016](http://dbs.uni-leipzig.de/file/EPGM.pdf)
* [Gradoop @Flink/Neo4j Meetup Berlin, March 2016](http://www.slideshare.net/s1ck/gradoop-scalable-graph-analytics-with-apache-flink-flink-neo4j-meetup-berlin)
* [Gradoop @FOSDEM GraphDevroom, January 2016](https://fosdem.org/2016/schedule/event/graph_processing_gradoop_flink_analytics)
* [Gradoop @FlinkForward, September 2015](http://www.slideshare.net/FlinkForward/martin-junghans-gradoop-scalable-graph-analytics-with-apache-flink) ([YouTube](https://youtu.be/WmP9xB_sG2o?list=PLDX4T_cnKjD31JeWR1aMOi9LXPRQ6nyHO))

## Data Model

In the extended property graph model (EPGM), a database consists of multiple 
property graphs which are called logical graphs. These graphs describe
application-specific subsets of vertices and edges, i.e. a vertex or an edge can
be contained in multiple logical graphs. Additionally, not only vertices and edges 
but also logical graphs have a type label and can have different properties.

Data Model elements (logical graphs, vertices and edges) have a unique identifier, 
a single label (e.g. User) and a number of key-value properties (e.g. name = Alice).
There is no schema involved, meaning each element can have an arbitrary number of
properties even if they have the same label.

### Graph operators

The EPGM provides operators for both single logical graphs as well as collections 
of logical graphs; operators may also return single graphs or graph collections. 
The following tables contains an overview (GC = Graph Collection, G = Logical Graph).

#### Unary logical graph operators (one graph as input):

| Operator      | Output | Output description                                           | Impl |
|:--------------|:-------|:-------------------------------------------------------------|:----:|
| Aggregation   | G      | Graph with result of an aggregate function as a new property | Yes  |
| Matching      | GC     | Graphs that match a given graph pattern                      | Yes  |
| Transformation| G      | Graph with transformed (graph, vertex, edge) data            | Yes  |
| Grouping      | G      | Structural condense of the input graph                       | Yes  |
| Subgraph      | G      | Subgraph that fulfils given vertex and edge predicates       | Yes  |

#### Binary logical graph operators (two graphs as input):

| Operator      | Output        | Output description                                                     | Impl |
|:--------------|:--------------|:-----------------------------------------------------------------------|:----:|
| Combination   | G             | Graph with vertices and edges from both input graphs                   | Yes  |
| Overlap       | G             | Graph with vertices and edges that exist in both input graphs          | Yes  |
| Exclusion     | G             | Graph with vertices and edges that exist only in the first graph       | Yes  |
| Equality      | {true, false} | Compare graphs in terms of identity or equality of contained elements  | Yes  |

#### Unary graph collection operators (one collection as input):

| Operator      | Output  | Output description                                                  | Impl |
|:--------------|:--------|:--------------------------------------------------------------------|:----:|
| Selection     | GC      | Filter graphs based on their attached data (i.e. label, properties) | Yes  |
| Distinct      | GC      | Collection with no duplicate graphs                                 | Yes  |
| SortBy        | GC      | Collection sorted by values of a given property key                 | No   |
| Limit         | GC      | The first n arbitrary elements of the input collection              | Yes  |

#### Binary graph collection operators (two collections as input):

| Operator      | Output        | Output description                                                         | Impl |
|:--------------|:--------------|:---------------------------------------------------------------------------|:----:|
| Union         | GC            | All graphs from both input collections                                     | Yes  |
| Intersection  | GC            | Only graphs that exist in both collections                                 | Yes  |
| Difference    | GC            | Only graphs that exist only in the first collection                        | Yes  |
| Equality      | {true, false} | Compare collections in terms of identity or equality of contained elements | Yes  |

#### Auxiliary operators:

| Operator      | In   | Out  | Output description                                                      | Impl |
|:--------------|:-----|:-----|:------------------------------------------------------------------------|:----:|
| Apply         | GC   | GC   | Applies unary operator (e.g. aggregate) on each graph in the collection | Yes  |
| Reduce        | GC   | G    | Reduces collection to single graph using binary operator (e.g. combine) | Yes  |
| Call          | GC/G | GC/G | Applies external algorithm on graph or graph collection                 | Yes  |

## Setup

### Use gradoop via Maven

* Add repository and dependency to your maven project

```
<repositories>
  <repository>
    <id>dbleipzig</id>
    <name>Database Group Leipzig University</name>
    <url>https://wdiserv1.informatik.uni-leipzig.de:443/archiva/repository/dbleipzig/</url>
    <releases>
      <enabled>true</enabled>
    </releases>
    <snapshots>
      <enabled>true</enabled>
    </snapshots>
   </repository>
</repositories>

<dependency>
  <groupId>org.gradoop</groupId>
  <artifactId>gradoop-flink</artifactId>
  <version>0.2-SNAPSHOT</version>
</dependency>
```

### Build gradoop from source

* Clone Gradoop into your local file system

    > git clone https://github.com/dbs-leipzig/gradoop.git
    
* Build and execute tests

    > cd gradoop
    
    > mvn clean install
    
## Gradoop modules

### gradoop-common

The main contents of that module are the EPGM data model and a corresponding POJO 
implementation which is used in Flink&trade;. The persistent representation of the EPGM
is also contained in gradoop-common and together with its mapping to HBase&trade;.

### gradoop-flink

This module contains reference implementations of the EPGM operators. The 
EPGM is mapped to Flink&trade; DataSets while the operators are implemented
using DataSet transformations. The module also contains implementations of 
general graph algorithms (e.g. Label Propagation, Frequent Subgraph Mining)
adapted to be used with the EPGM model.

### gradoop-examples

Contains example pipelines showing use cases for Gradoop. 

*   Graph grouping example (build structural aggregates of property graphs)
*   Social network examples (composition of multiple operators to analyze social networks graphs)
*   Input/Output examples (usage of DataSource and DataSink implementations)
*   Benchmarks used for cluster evaluations

### gradoop-checkstyle

Used to maintain the code style for the whole project.
    
### Version History

* 0.0.1 first prototype using Hadoop MapReduce and Apache Giraph for operator
 processing
* 0.0.2 support for HBase as distributed graph storage
* 0.0.3 Apache Flink replaces MapReduce and Giraph as operator implementation
 layer and distributed execution engine
* 0.1 Major refactoring of internal EPGM representation (e.g. ID and property handling), Equality Operators, GDL-based unit testing

### Disclaimer

Apache®, Apache Flink&trade;, Flink&trade;, Apache HBase&trade; and HBase&trade; 
are either registered trademarks or trademarks of the Apache Software Foundation 
in the United States and/or other countries.




