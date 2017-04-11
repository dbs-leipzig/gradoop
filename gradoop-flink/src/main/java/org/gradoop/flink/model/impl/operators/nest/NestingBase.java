/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.flink.model.impl.operators.nest;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.operators.GraphGraphCollectionToGraphOperator;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.tuple.Value0Of2;
import org.gradoop.flink.model.impl.functions.tuple.Value1Of2;
import org.gradoop.flink.model.impl.functions.utils.LeftSide;
import org.gradoop.flink.model.impl.operators.nest.functions.*;
import org.gradoop.flink.model.impl.functions.epgm.ToIdElementPair;
import org.gradoop.flink.model.impl.functions.utils.Self;
import org.gradoop.flink.model.impl.operators.nest.model.NestedModel;
import org.gradoop.flink.model.impl.operators.nest.model.WithNestedResult;
import org.gradoop.flink.model.impl.operators.nest.model.indices.NestingIndex;
import org.gradoop.flink.model.impl.operators.nest.model.indices.NestingResult;
import org.gradoop.flink.model.impl.operators.nest.tuples.Hexaplet;

/**
 * Basic operations
 */
public abstract class NestingBase implements GraphGraphCollectionToGraphOperator,
  WithNestedResult<DataSet<Hexaplet>> {

  /**
   * The actual id to be associated to the returned graph
   */
  protected final GradoopId graphId;

  /**
   * Defines the model where the elements are set
   */
  protected NestedModel model = null;
  /**
   * Left index mapping
   */
  protected NestingIndex graphIndex;
  /**
   * Right index mapping
   */
  protected NestingIndex collectionIndex;

  /**
   * Initialize the upper class with the id to be associated to the nested grapj
   * @param graphId   New graph id
   */
  public NestingBase(GradoopId graphId) {
    this.graphId = graphId;
  }

  /**
   * Initialize the upper class with the id to be associated to the nested grapj
   * @param graphId   New graph id
   * @param model     The model associated with the previous computations
   */
  public NestingBase(GradoopId graphId, NestedModel model) {
    this.graphId = graphId;
    this.model = model;
  }

  /**
   * Merge two indices together. This situation could be used while generating a model
   * @param left    An index
   * @param right   Another index
   * @return        The indices from left and right are fused with a single index
   */
  public static NestingIndex mergeIndices(NestingIndex left, NestingIndex right) {
    DataSet<GradoopId> heads = left.getGraphHeads()
      .union(right.getGraphHeads())
      .distinct(new Self<>());

    DataSet<Tuple2<GradoopId, GradoopId>> verticesIndex = left.getGraphVertexMap()
      .union(right.getGraphVertexMap())
      .distinct(0, 1);

    DataSet<Tuple2<GradoopId, GradoopId>> edgesIndex = left.getGraphEdgeMap()
      .union(right.getGraphEdgeMap())
      .distinct(0, 1);

    DataSet<Tuple2<GradoopId, GradoopId>> graphStack = left.getGraphStack();

    return new NestingIndex(heads, verticesIndex, edgesIndex, graphStack);
  }

  /**
   * Extracts the id from the normalized graph
   * @param logicalGraph  Graph where to extract the ids from
   * @return the indexed representation
   */
  public static NestingIndex createIndex(LogicalGraph logicalGraph) {
    DataSet<GradoopId> graphHeads = logicalGraph.getGraphHead()
      .map(new Id<>());
    return createIndex(graphHeads, logicalGraph.getVertices(), logicalGraph.getEdges());
  }

  /**
   * Extracts the id from the normalized graph
   * @param logicalGraph  Graph where to extract the ids from
   * @return the indexed representation
   */
  public static NestingIndex createIndex(GraphCollection logicalGraph) {
    DataSet<GradoopId> graphHeads = logicalGraph.getGraphHeads()
      .map(new Id<>());
    return createIndex(graphHeads, logicalGraph.getVertices(), logicalGraph.getEdges());
  }

  /**
   * Initializes the vertices and the edges with a similar procedure
   * @param heads pre-evaluated heads
   * @param vertices Vertices
   * @param edges Edges
   * @return the actual indices used
   */
  private static NestingIndex createIndex(
    DataSet<GradoopId> heads, DataSet<Vertex> vertices, DataSet<Edge> edges) {

    //Creating the graphVertexMap for the graphheads appearing in the logical graph for the vertices
    DataSet<Tuple2<GradoopId, GradoopId>> graphVertexMap = vertices
      .flatMap(new ToIdElementPair<>())
      .joinWithTiny(heads)
      .where(new Value0Of2<>()).equalTo(new Self<>())
      .with(new LeftSide<>())
      .filter(new NotGraphHead());

    //Creating the graphEdgeMap for the graphheads appearing in the logical graph for the edges
    DataSet<Tuple2<GradoopId, GradoopId>> graphEdgeMap = edges
      .flatMap(new ToIdElementPair<>())
      .joinWithTiny(heads)
      .where(new Value0Of2<>()).equalTo(new Self<>())
      .with(new LeftSide<>())
      .filter(new NotGraphHead());

    return new NestingIndex(heads, graphVertexMap, graphEdgeMap);
  }

  /**
   * Converts the NestedIndexing information into a GraphCollection by using the ground truth
   * information
   * @param self            Indexed representation of the data
   * @param flattenedGraph  ground truth containing all the informations
   * @return                EPGM representation
   */
  public static GraphCollection toGraphCollection(NestingIndex self,
    LogicalGraph flattenedGraph) {

    DataSet<Vertex> vertices = self.getGraphVertexMap()
      .coGroup(flattenedGraph.getVertices())
      .where(new Value1Of2<>()).equalTo(new Id<>())
      .with(new UpdateVertices());

    DataSet<Edge> edges = self.getGraphEdgeMap()
      .coGroup(flattenedGraph.getEdges())
      .where(new Value1Of2<>()).equalTo(new Id<>())
      .with(new UpdateEdges());

    DataSet<GraphHead> heads = getActualGraphHeads(self, flattenedGraph);

    return GraphCollection.fromDataSets(heads, vertices, edges, flattenedGraph.getConfig());
  }

  /**
   * Converts the NestedIndexing information into a LogicalGraph by using the ground truth
   * information
   * @param self      Indexed representation of the data
   * @param flattenedGraph  ground truth containing all the informations
   * @return          EPGM representation
   */
  public static LogicalGraph toLogicalGraph(NestingIndex self, LogicalGraph flattenedGraph) {
    DataSet<GraphHead> heads = getActualGraphHeads(self, flattenedGraph);

    DataSet<Vertex> vertices = self.getGraphVertexMap()
      .coGroup(flattenedGraph.getVertices())
      .where(new Value1Of2<>()).equalTo(new Id<>())
      .with(new UpdateVertices())
      .crossWithTiny(self.getGraphStack())
      .with(new AddElementToGraph<>());

    DataSet<Edge> edges = self.getGraphEdgeMap()
      .coGroup(flattenedGraph.getEdges())
      .where(new Value1Of2<>()).equalTo(new Id<>())
      .with(new UpdateEdges());

    return LogicalGraph.fromDataSets(heads, vertices, edges, flattenedGraph.getConfig());
  }

  /**
   * Instantiates the GraphHeads using the LogicalGraph as a primary source
   * @param self      ground truth for elements
   * @param flattenedGraph  primary source
   * @return          GraphHeads
   */
  private static DataSet<GraphHead> getActualGraphHeads(NestingIndex self,
    LogicalGraph flattenedGraph) {
    return self.getGraphHeads()
      .coGroup(flattenedGraph.getVertices())
      .where(new Self<>()).equalTo(new Id<>())
      .with(new VertexToGraphHead())
      .join(self.getGraphStack())
      .where(new Id<>()).equalTo(new Value1Of2<>())
      .with(new ReplaceHeadId());
  }

  /**
   * Generating a nested model from the initial graph operands
   * @param graph           Graph element that is going to undergo the nesting
   * @param collection      Collection defining the nesting structure
   * @param graphIndex      Graph index previously evaluated
   * @param collectionIndex Collection index previously evaluated
   * @return   The associated nested model
   */
  public static NestedModel generateModel(LogicalGraph graph,
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

    NestingIndex nestingIndex = mergeIndices(graphIndex, collectionIndex);

    return new NestedModel(flattenedGraph, nestingIndex);
  }

  /**
   * Implements the nesting operation for the nested model.
   *
   * @param model             Reference to the nested model
   * @param graphIndex        index for the search graph
   * @param collectionIndex   index for the graph collection
   * @param nestedGraphId     id to be associated to the new graph in the EPGM model
   * @return                  The updated results associated to the new graph
   */
  public NestingResult nest(NestedModel model, NestingIndex graphIndex,
    NestingIndex collectionIndex, GradoopId nestedGraphId) {

    // Associate each gid in collection's heads to the merged vertices
    DataSet<GradoopId> heads = graphIndex.getGraphHeads();

    DataSet<Tuple2<GradoopId, GradoopId>> newStackElement = heads
      .map(new ToTuple2WithF0(nestedGraphId));

    // Creates a new element to the stack
    model.getNestingIndex().addToStack(newStackElement);

    // Mark each vertex if either it's present or not in the final match
    // TODO       JOIN COUNT: (1)
    DataSet<Hexaplet> nestedResult = graphIndex.getGraphVertexMap()
      .leftOuterJoin(collectionIndex.getGraphVertexMap())
      .where(1).equalTo(1)
      // If the vertex does not appear in the graph collection, the f2 element will be null.
      // These vertices are the ones to be returned as vertices alongside with the new
      // graph heads
      .with(new AssociateAndMark());

    // Vertices to be returend within the NestedIndexing
    DataSet<GradoopId> tmpVert = nestedResult
      .groupBy(new Value4OfHexaplet())
      .reduceGroup(new CollectVertices());

    DataSet<Tuple2<GradoopId, GradoopId>> vertices = heads.crossWithHuge(tmpVert);

    DataSet<GradoopId> tmpEdges = graphIndex
      .getGraphEdgeMap()
      .map(new Value1Of2<>());

    DataSet<Tuple2<GradoopId, GradoopId>> edges = heads.crossWithHuge(tmpEdges);

    NestingResult previousResult =
      new NestingResult(heads, vertices, edges, nestedResult, newStackElement);

    model.setPreviousResult(previousResult);

    return previousResult;
  }

  @Override
  public abstract LogicalGraph execute(LogicalGraph graph, GraphCollection collection);

  @Override
  public abstract DataSet<Hexaplet> getPreviousComputation();
}
