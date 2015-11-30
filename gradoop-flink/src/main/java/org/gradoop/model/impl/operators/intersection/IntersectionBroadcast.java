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
 * along with Gradoop.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.model.impl.operators.intersection;

import org.apache.flink.api.java.DataSet;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.functions.epgm.Id;
import org.gradoop.model.impl.functions.graphcontainment.GraphsContainmentFilterBroadcast;
import org.gradoop.model.impl.functions.graphcontainment.InAllGraphsBroadcast;
import org.gradoop.model.impl.functions.graphcontainment.InAnyGraphBroadcast;
import org.gradoop.model.impl.id.GradoopId;

/**
 * Returns a collection with all logical graphs that exist in both input
 * collections. Graph equality is based on their identifiers.
 * <p>
 * This operator implementation requires that a list of subgraph identifiers
 * in the resulting graph collections fits into the workers main memory.
 *
 * @param <G> EPGM graph head type
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
public class IntersectionBroadcast<
  G extends EPGMGraphHead,
  V extends EPGMVertex,
  E extends EPGMEdge>
  extends Intersection<G, V, E> {

  @Override
  protected DataSet<V> computeNewVertices(
    DataSet<G> newSubgraphs) throws Exception {
    DataSet<GradoopId> identifiers = secondCollection.getGraphHeads()
      .map(new Id<G>());

    return firstCollection.getVertices()
      .filter(new InAnyGraphBroadcast<V>())
      .withBroadcastSet(
        identifiers, GraphsContainmentFilterBroadcast.GRAPH_IDS);
  }

  @Override
  public String getName() {
    return IntersectionBroadcast.class.getName();
  }
}
