package org.gradoop.flink.model.impl.operators.nest.model;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.tuple.Value1Of2;
import org.gradoop.flink.model.impl.operators.nest.NestingBase;
import org.gradoop.flink.model.impl.operators.nest.functions.AssociateAndMark;
import org.gradoop.flink.model.impl.operators.nest.functions.CollectVertices;
import org.gradoop.flink.model.impl.operators.nest.functions.GraphHeadToVertex;
import org.gradoop.flink.model.impl.operators.nest.functions.Hex4;
import org.gradoop.flink.model.impl.operators.nest.model.indices.NestedResult;
import org.gradoop.flink.model.impl.operators.nest.model.indices.NestingIndex;
import org.gradoop.flink.model.impl.operators.nest.tuples.Hexaplet;

/**
 * Defines the nested model where the operations are actually carried out
 */
public class NestedModel {

  /**
   * Flat representation for all the other graphs
   */
  private LogicalGraph flattenedGraph;

  /**
   * Nested representation induced by the nesting functions
   */
  private NestingIndex nestedRepresentation;

  /**
   * Implements the nested model using the most generic components
   * @param flattenedGraph        Flattened graph representing all the possible data values
   * @param nestedRepresentation  Nesting structures over the flattened graph
   */
  public NestedModel(LogicalGraph flattenedGraph, NestingIndex nestedRepresentation) {
    this.flattenedGraph = flattenedGraph;
    this.nestedRepresentation = nestedRepresentation;
  }

  /**
   * Returns…
   * @return  the flattened representation for the graph
   */
  public LogicalGraph getFlattenedGraph() {
    return flattenedGraph;
  }

  /**
   * Returns…
   * @return  the indexing structure inducing the nesting
   */
  public NestingIndex getNestedRepresentation() {
    return nestedRepresentation;
  }

  /**
   * Generating a nested model from the initial graph operands
   * @param graph           Graph element that is going to undergo the nesting
   * @param collection      Collection defining the nesting structure
   * @param graphIndex      Graph index previously evaluated
   * @param collectionIndex Collection index previously evaluated
   * @return   The associated nested model
   */
  public static NestedModel generateNestedModelFromOperands(LogicalGraph graph,
    GraphCollection collection, NestingIndex graphIndex, NestingIndex collectionIndex) {

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

    NestingIndex nestedRepresentation = NestingBase.mergeIndices(graphIndex, collectionIndex);

    return new NestedModel(flattenedGraph, nestedRepresentation);
  }

  /**
   * Generating a nested model from the initial graph operands, when graph indices are not already
   * evaluated
   * @param graph           Graph element that is going to undergo the nesting
   * @param collection      Collection defining the nesting structure
   * @return    The associated nested model
   */
  public static NestedModel generateNestedModelFromOperands(LogicalGraph graph,
    GraphCollection collection) {

    NestingIndex graphIndex = NestingBase.createIndex(graph);
    NestingIndex collectionIndex = NestingBase.createIndex(collection);
    return generateNestedModelFromOperands(graph, collection, graphIndex, collectionIndex);
  }

  public NestedResult nesting(NestingIndex graphIndex, NestingIndex collectionIndex,
    GradoopId nestedGraphId) {
    // Associate each gid in hypervertices.H to the merged vertices
    DataSet<GradoopId> heads = graphIndex.getGraphHeads();

    // Creates a new element to the stack
    nestedRepresentation.updateStackWith(heads, nestedGraphId);

    // Mark each vertex if either it's present or not in the final match
    // TODO       JOIN COUNT: (1)
    DataSet<Hexaplet> nestedResult = graphIndex.getGraphHeadToVertex()
      .leftOuterJoin(collectionIndex.getGraphHeadToVertex())
      .where(new Value1Of2<>()).equalTo(new Value1Of2<>())
      // If the vertex does not appear in the graph collection, the f2 element is null.
      // These vertices are the ones to be returned as vertices alongside with the new
      // graph heads
      .with(new AssociateAndMark());

    // Vertices to be returend within the NestedIndexing
    DataSet<GradoopId> tmpVert = nestedResult
      .groupBy(new Hex4())
      .reduceGroup(new CollectVertices());

    DataSet<Tuple2<GradoopId, GradoopId>> vertices = heads
      .crossWithHuge(tmpVert);

    DataSet<GradoopId> tmpEdges = graphIndex
      .getGraphHeadToEdge()
      .map(new Value1Of2<>());

    DataSet<Tuple2<GradoopId, GradoopId>> edges = heads
      .crossWithHuge(tmpEdges);

    return new NestedResult(heads, vertices, edges, nestedResult);
  }

}
