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

package org.gradoop.flink.model.impl.operators.nest.transformations;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.tuple.Value0Of2;
import org.gradoop.flink.model.impl.functions.utils.LeftSide;
import org.gradoop.flink.model.impl.operators.nest.functions.AssociateElementToIdAndGraph;
import org.gradoop.flink.model.impl.operators.nest.functions.ExceptGraphHead;
import org.gradoop.flink.model.impl.operators.nest.functions.SelfId;
import org.gradoop.flink.model.impl.operators.nest.model.indices.NestedIndexing;
import org.gradoop.flink.model.impl.operators.nest.model.NormalizedGraph;

/**
 * Transforms the EPGM model representations into indexed ones
 */
public class EPGMToNestedIndexingTransformation {

  /**
   * Extracts the id from the normalized graph
   * @param logicalGraph  Graph where to extract the ids from
   * @return the indexed representation
   */
  public static NestedIndexing fromNormalizedGraph(NormalizedGraph logicalGraph) {
    DataSet<GradoopId> graphHeads = logicalGraph.getGraphHeads()
      .map(new Id<>());
    return initVertices(graphHeads, logicalGraph.getVertices(), logicalGraph.getEdges());
  }

  /**
   * Extracts the id from the normalized graph
   * @param logicalGraph  Graph where to extract the ids from
   * @return the indexed representation
   */
  public static NestedIndexing fromLogicalGraph(LogicalGraph logicalGraph) {
    DataSet<GradoopId> graphHeads = logicalGraph.getGraphHead()
      .map(new Id<>());
    return initVertices(graphHeads, logicalGraph.getVertices(), logicalGraph.getEdges());
  }

  /**
   * Extracts the id from the normalized graph
   * @param logicalGraph  Graph where to extract the ids from
   * @return the indexed representation
   */
  public static NestedIndexing fromGraphCollection(GraphCollection logicalGraph) {
    DataSet<GradoopId> graphHeads = logicalGraph.getGraphHeads()
      .map(new Id<>());
    return initVertices(graphHeads, logicalGraph.getVertices(), logicalGraph.getEdges());
  }

  /**
   * Initializes the vertices and the edges with a similar procedure
   * @param h pre-evaluated heads
   * @param v Vertices
   * @param e Edges
   * @return the actual indices used
   */
  private static NestedIndexing initVertices(DataSet<GradoopId> h, DataSet<Vertex> v, DataSet<Edge>
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

    return new NestedIndexing(h, graphHeadToVertex, graphHeadToEdge);
  }


}
