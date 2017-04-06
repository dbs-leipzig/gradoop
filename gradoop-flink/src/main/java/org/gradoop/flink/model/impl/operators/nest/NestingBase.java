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
import org.gradoop.flink.model.impl.operators.nest.functions.AssociateElementToIdAndGraph;
import org.gradoop.flink.model.impl.operators.nest.functions.ExceptGraphHead;
import org.gradoop.flink.model.impl.operators.nest.functions.SelfId;
import org.gradoop.flink.model.impl.operators.nest.functions.UpdateEdges;
import org.gradoop.flink.model.impl.operators.nest.functions.UpdateVertices;
import org.gradoop.flink.model.impl.operators.nest.functions.VertexToGraphHead;
import org.gradoop.flink.model.impl.operators.nest.model.WithNestedResult;
import org.gradoop.flink.model.impl.operators.nest.model.indices.NestingIndex;
import org.gradoop.flink.model.impl.operators.nest.tuples.Hexaplet;

/**
 * Created by vasistas on 06/04/17.
 */
public abstract class NestingBase implements GraphGraphCollectionToGraphOperator,
  WithNestedResult<DataSet<Hexaplet>> {
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
   * @param h pre-evaluated heads
   * @param v Vertices
   * @param e Edges
   * @return the actual indices used
   */
  private static NestingIndex createIndex(DataSet<GradoopId> h, DataSet<Vertex> v, DataSet<Edge>
    e) {
    //Creating the map for the graphheads appearing in the logical graph
    DataSet<Tuple2<GradoopId, GradoopId>> graphHeadToVertex = v
      .flatMap(new AssociateElementToIdAndGraph<>())
      .joinWithTiny(h)
      .where(new Value0Of2<>()).equalTo(new SelfId())
      .with(new LeftSide<>())
      .filter(new ExceptGraphHead());

    //Same as above
    DataSet<Tuple2<GradoopId, GradoopId>> graphHeadToEdge = e
      .flatMap(new AssociateElementToIdAndGraph<>())
      .joinWithTiny(h)
      .where(new Value0Of2<>()).equalTo(new SelfId())
      .with(new LeftSide<>())
      .filter(new ExceptGraphHead());

    return new NestingIndex(h, graphHeadToVertex, graphHeadToEdge);
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

    DataSet<Vertex> vertices = self.getGraphHeadToVertex()
      .coGroup(flattenedGraph.getVertices())
      .where(new Value1Of2<>()).equalTo(new Id<>())
      .with(new UpdateVertices());

    DataSet<Edge> edges = self.getGraphHeadToEdge()
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
  public static LogicalGraph toLogicalGraph(NestingIndex self,
    LogicalGraph flattenedGraph) {

    DataSet<Vertex> vertices = self.getGraphHeadToVertex()
      .coGroup(flattenedGraph.getVertices())
      .where(new Value1Of2<>()).equalTo(new Id<>())
      .with(new UpdateVertices());

    DataSet<Edge> edges = self.getGraphHeadToEdge()
      .coGroup(flattenedGraph.getEdges())
      .where(new Value1Of2<>()).equalTo(new Id<>())
      .with(new UpdateEdges());

    DataSet<GraphHead> heads = getActualGraphHeads(self, flattenedGraph);

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
      .join(flattenedGraph.getVertices())
      .where(new SelfId()).equalTo(new Id<>())
      .with(new VertexToGraphHead());
  }

  @Override
  public abstract LogicalGraph execute(LogicalGraph graph, GraphCollection collection);

  @Override
  public abstract DataSet<Hexaplet> getPreviousComputation();
}
