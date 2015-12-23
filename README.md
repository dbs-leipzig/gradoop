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
Gradoop can be easily integrated in a workflow which already uses Flink operators
and Flink libraries (i.e. Gelly, ML and Table).

Gradoop is **work in progress** which means APIs may change. It is currently used
as a proof of concept implementation and far from production ready.

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

The following table contains an overview (GC = Graph Collection, G = Logical Graph).

| Operator      | In      | Out           | Output description                                                      | Impl |
|:--------------|:--------|:--------------|:------------------------------------------------------------------------|:----:|
| Selection     | GC      | GC            | Filter graphs based on their attached data (i.e. label, properties)     | Yes  |
| Distinct      | GC      | GC            | Collection with no duplicate graphs                                     | No   |
| SortBy        | GC      | GC            | Collection sorted by values of a given property key                     | No   |
| Top           | GC      | GC            | The first n elements of the input collection                            | No   |
| Union         | GC x GC | GC            | All graphs from both input collections                                  | Yes  |
| Intersection  | GC x GC | GC            | Only graphs that exist in both collections                              | Yes  |
| Difference    | GC x GC | GC            | Only graphs that exist only in the first collection                     | Yes  |
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
| Reduce        | GC      | G             | Reduces collection to graph using binary operator (e.g. combine)        | Yes  |

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

### gradoop-core

The main contents of that module are the EPGM data model and a corresponding POJO 
implementation which is used in Flink. The persistent representation of the EPGM
is also contained in gradoop-core and together with its mapping to Apache HBase.

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




