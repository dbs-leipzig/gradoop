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

package org.gradoop.flink.model.impl.operators.distinct;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.operators.JoinOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.operators.UnaryCollectionToCollectionOperator;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.epgm.IdInBroadcast;
import org.gradoop.flink.model.impl.functions.graphcontainment.GraphsContainmentFilterBroadcast;
import org.gradoop.flink.model.impl.functions.graphcontainment.InAnyGraph;
import org.gradoop.flink.model.impl.functions.graphcontainment.InAnyGraphBroadcast;
import org.gradoop.flink.model.impl.functions.tuple.Value1Of2;
import org.gradoop.flink.model.impl.functions.utils.LeftSide;
import org.gradoop.flink.model.impl.operators.distinct.functions.IdFromGraphHeadString;
import org.gradoop.flink.model.impl.operators.tostring.CanonicalAdjacencyMatrixBuilder;
import org.gradoop.flink.model.impl.operators.tostring.functions.EdgeToDataString;
import org.gradoop.flink.model.impl.operators.tostring.functions.GraphHeadToEmptyString;
import org.gradoop.flink.model.impl.operators.tostring.functions.VertexToDataString;
import org.gradoop.flink.model.impl.operators.tostring.tuples.GraphHeadString;

/**
 * Returns a distinct collection of logical graphs.
 * Graphs are compared by isomorphism testing.
 */
public class DistinctByIsomorphism implements UnaryCollectionToCollectionOperator {

  @Override
  public GraphCollection execute(GraphCollection collection) {

    // Init builder for canonical labels
    CanonicalAdjacencyMatrixBuilder camBuilder = new CanonicalAdjacencyMatrixBuilder(
      new GraphHeadToEmptyString(),  new VertexToDataString(), new EdgeToDataString(), true);

    // create canonical labels for all graph heads and choose representative for all distinct ones
    DataSet<GradoopId> graphIds = camBuilder
      .getGraphHeadStrings(collection)
      .distinct(1)
      .map(new IdFromGraphHeadString());

    DataSet<GraphHead> graphHeads = collection.getGraphHeads()
      .filter(new IdInBroadcast<>())
      .withBroadcastSet(graphIds, IdInBroadcast.IDS);

    // TODO: wait for delete operator

    // use graph ids to filter vertices from the actual graph structure
    DataSet<Vertex> vertices = collection.getVertices()
      .filter(new InAnyGraphBroadcast<>())
      .withBroadcastSet(graphIds, GraphsContainmentFilterBroadcast.GRAPH_IDS);

    DataSet<Edge> edges = collection.getEdges()
      .filter(new InAnyGraphBroadcast<>())
      .withBroadcastSet(graphIds, GraphsContainmentFilterBroadcast.GRAPH_IDS);

    return GraphCollection.fromDataSets(
      graphHeads, vertices, edges, collection.getConfig());
  }

  @Override
  public String getName() {
    return DistinctByIsomorphism.class.getName();
  }
}
