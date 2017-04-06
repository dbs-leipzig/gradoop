package org.gradoop.flink.model.impl.operators.nest.model;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.operators.nest.functions.GraphHeadToVertex;
import org.gradoop.flink.model.impl.operators.nest.model.indices.NestingIndex;

/**
 * Created by vasistas on 06/04/17.
 */
public class NestedModel {

  private LogicalGraph flattenedGraphs;
  private NestingIndex nestedRepresentation;

  public NestedModel(LogicalGraph flattenedGraphs, NestingIndex nestedRepresentation) {
    this.flattenedGraphs = flattenedGraphs;
    this.nestedRepresentation = nestedRepresentation;
  }

  public LogicalGraph getFlattenedGraphs() {
    return flattenedGraphs;
  }

  public NestingIndex getNestedRepresentation() {
    return nestedRepresentation;
  }

  public static NestedModel generateNestedModelFromOperands(LogicalGraph graph,
    GraphCollection collection) {

    DataSet<GraphHead> heads = graph.getGraphHead()
      .union(collection.getGraphHeads())
      .distinct(new Id<>());

    DataSet<Vertex> nestedVertices = heads
      .map(new GraphHeadToVertex());

    DataSet<Vertex> vertices = graph.getVertices()
      .union(collection.getVertices())
      .union(nestedVertices)
      .distinct(new Id<>());

    DataSet<Edge> edges = graph.getEdges()
      .union(collection.getEdges())
      .distinct(new Id<>());

    // Getting the model for defining the associated model
    LogicalGraph flattenedGraph = LogicalGraph.fromDataSets
      (heads, vertices, edges, graph.getConfig());

    return null;
  }

}
