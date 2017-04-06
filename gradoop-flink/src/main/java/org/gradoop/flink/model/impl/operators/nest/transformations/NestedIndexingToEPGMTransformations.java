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
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.tuple.Value1Of2;
import org.gradoop.flink.model.impl.operators.nest.functions.GraphHeadToVertex;
import org.gradoop.flink.model.impl.operators.nest.functions.SelfId;
import org.gradoop.flink.model.impl.operators.nest.functions.UpdateEdges;
import org.gradoop.flink.model.impl.operators.nest.functions.UpdateVertices;
import org.gradoop.flink.model.impl.operators.nest.functions.VertexToGraphHead;
import org.gradoop.flink.model.impl.operators.nest.model.indices.NestingIndex;

/**
 * Utility functions transforming NestedIndexing into EPGM model
 */
public class NestedIndexingToEPGMTransformations {

  /**
   * Converts the NestedIndexing information into a GraphCollection by using the ground truth
   * information
   * @param self      Indexed representation of the data
   * @param dataLake  ground truth containing all the informations
   * @return          EPGM representation
   */
  public static GraphCollection toGraphCollection(NestingIndex self,
    LogicalGraph dataLake) {

    DataSet<Vertex> vertices = self.getGraphHeadToVertex()
      .coGroup(dataLake.getVertices())
      .where(new Value1Of2<>()).equalTo(new Id<>())
      .with(new UpdateVertices());

    DataSet<Edge> edges = self.getGraphHeadToEdge()
      .coGroup(dataLake.getEdges())
      .where(new Value1Of2<>()).equalTo(new Id<>())
      .with(new UpdateEdges());

    DataSet<GraphHead> heads = getActualGraphHeads(self, dataLake);

    return GraphCollection.fromDataSets(heads, vertices, edges, dataLake.getConfig());
  }

  /**
   * Converts the NestedIndexing information into a LogicalGraph by using the ground truth
   * information
   * @param self      Indexed representation of the data
   * @param dataLake  ground truth containing all the informations
   * @return          EPGM representation
   */
  public static LogicalGraph toLogicalGraph(NestingIndex self,
    LogicalGraph dataLake) {

    DataSet<Vertex> vertices = self.getGraphHeadToVertex()
      .coGroup(dataLake.getVertices())
      .where(new Value1Of2<>()).equalTo(new Id<>())
      .with(new UpdateVertices());

    DataSet<Edge> edges = self.getGraphHeadToEdge()
      .coGroup(dataLake.getEdges())
      .where(new Value1Of2<>()).equalTo(new Id<>())
      .with(new UpdateEdges());

    DataSet<GraphHead> heads = getActualGraphHeads(self, dataLake);

    return LogicalGraph.fromDataSets(heads, vertices, edges, dataLake.getConfig());
  }

  /**
   * Instantiates the GraphHeads using the LogicalGraph as a primary source
   * @param self      ground truth for elements
   * @param dataLake  primary source
   * @return          GraphHeads
   */
  public static DataSet<GraphHead> getActualGraphHeads(NestingIndex self,
                                                       LogicalGraph dataLake) {
    return self.getGraphHeads()
      .join(dataLake.getVertices())
      .where(new SelfId()).equalTo(new Id<>())
      .with(new VertexToGraphHead());
  }

  /**
   * Recreates the LogicalGraph of the informations stored within
   * the NestedIndexing through the DataLake
   * @param ni        Element to be transformed
   * @param dataLake  Element containing all the required and necessary
   *                  informations
   * @return          Actual values
   */
  public static LogicalGraph asLogicalGraph(NestingIndex ni, LogicalGraph dataLake) {
    DataSet<Vertex> vertices = ni.getGraphHeadToVertex()
      .coGroup(dataLake.getVertices())
      .where(new Value1Of2<>()).equalTo(new Id<>())
      .with(new UpdateVertices());

    DataSet<Edge> edges = ni.getGraphHeadToEdge()
      .coGroup(dataLake.getEdges())
      .where(new Value1Of2<>()).equalTo(new Id<>())
      .with(new UpdateEdges());

    DataSet<GraphHead> heads = getActualGraphHeads(ni, dataLake);

    vertices = vertices
      .union(heads.map(new GraphHeadToVertex()))
      .distinct(new Id<>());

    return LogicalGraph.fromDataSets(heads, vertices, edges, dataLake.getConfig());
  }

}
