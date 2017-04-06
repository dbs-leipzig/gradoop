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
import org.gradoop.flink.model.impl.operators.nest.functions.AddElementToGraph;
import org.gradoop.flink.model.impl.operators.nest.functions.AssociateElementToIdAndGraph;
import org.gradoop.flink.model.impl.operators.nest.functions.ExceptGraphHead;
import org.gradoop.flink.model.impl.operators.nest.functions.Identity;
import org.gradoop.flink.model.impl.operators.nest.functions.ReplaceHeadId;
import org.gradoop.flink.model.impl.operators.nest.functions.UpdateEdges;
import org.gradoop.flink.model.impl.operators.nest.functions.UpdateVertices;
import org.gradoop.flink.model.impl.operators.nest.functions.VertexToGraphHead;
import org.gradoop.flink.model.impl.operators.nest.model.WithNestedResult;
import org.gradoop.flink.model.impl.operators.nest.model.indices.NestingIndex;
import org.gradoop.flink.model.impl.operators.nest.tuples.Hexaplet;

/**
 * Basic operations
 */
public abstract class NestingBase implements GraphGraphCollectionToGraphOperator,
  WithNestedResult<DataSet<Hexaplet>> {

  /**
   * Merge two indices together. This situation could be used while generating a model
   * @param left    An index
   * @param right   Another index
   * @return        The indices from left and right are fused with a single index
   */
  public static NestingIndex mergeIndices(NestingIndex left, NestingIndex right) {
    DataSet<GradoopId> heads = left.getGraphHeads()
      .union(right.getGraphHeads())
      .distinct(new Identity<>());

    DataSet<Tuple2<GradoopId, GradoopId>> verticesIndex = left.getGraphHeadToVertex()
      .union(right.getGraphHeadToVertex())
      .distinct(0, 1);

    DataSet<Tuple2<GradoopId, GradoopId>> edgesIndex = left.getGraphHeadToEdge()
      .union(right.getGraphHeadToEdge())
      .distinct(0, 1);

    DataSet<Tuple2<GradoopId, GradoopId>> graphStack = left.getGraphStack();
    if (graphStack == null) {
      graphStack = right.getGraphStack();
    } else {
      DataSet<Tuple2<GradoopId, GradoopId>> rightStack = right.getGraphStack();
      if (rightStack != null) {
        graphStack = graphStack.union(rightStack);
      }
    }

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
   * @param h pre-evaluated heads
   * @param v Vertices
   * @param e Edges
   * @return the actual indices used
   */
  private static NestingIndex createIndex(DataSet<GradoopId> h, DataSet<Vertex> v, DataSet<Edge>
    e) {
    //Creating the map for the graphheads appearing in the logical graph for the vertices
    DataSet<Tuple2<GradoopId, GradoopId>> graphHeadToVertex = v
      .flatMap(new AssociateElementToIdAndGraph<>())
      .joinWithTiny(h)
      .where(new Value0Of2<>()).equalTo(new Identity<>())
      .with(new LeftSide<>())
      .filter(new ExceptGraphHead());

    //Creating the map for the graphheads appearing in the logical graph for the edges
    DataSet<Tuple2<GradoopId, GradoopId>> graphHeadToEdge = e
      .flatMap(new AssociateElementToIdAndGraph<>())
      .joinWithTiny(h)
      .where(new Value0Of2<>()).equalTo(new Identity<>())
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
  public static LogicalGraph toLogicalGraph(NestingIndex self, LogicalGraph flattenedGraph) {
    DataSet<GraphHead> heads = getActualGraphHeads(self, flattenedGraph);

    DataSet<Vertex> vertices = self.getGraphHeadToVertex()
      .coGroup(flattenedGraph.getVertices())
      .where(new Value1Of2<>()).equalTo(new Id<>())
      .with(new UpdateVertices())
      .crossWithTiny(self.getGraphStack())
      .with(new AddElementToGraph<>());

    DataSet<Edge> edges = self.getGraphHeadToEdge()
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
      .where(new Identity<>()).equalTo(new Id<>())
      .with(new VertexToGraphHead())
      .join(self.getGraphStack())
      .where(new Id<>()).equalTo(new Value1Of2<>())
      .with(new ReplaceHeadId());
  }

  @Override
  public abstract LogicalGraph execute(LogicalGraph graph, GraphCollection collection);

  @Override
  public abstract DataSet<Hexaplet> getPreviousComputation();
}
