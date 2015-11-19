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

package org.gradoop.model.impl.operators.collection.binary;

import org.apache.flink.api.java.DataSet;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.functions.isolation.ElementIdOnly;
import org.gradoop.model.impl.functions.filterfunctions
  .VertexInGraphsFilterWithBC;
import org.gradoop.model.impl.id.GradoopId;

/**
 * Returns a collection with all logical graphs that exist in both input
 * collections. Graph equality is based on their identifiers.
 * <p>
 * This operator implementation requires that a list of subgraph identifiers
 * in the resulting graph collections fits into the workers main memory.
 *
 * @param <VD> EPGM vertex type
 * @param <ED> EPGM edge type
 * @param <GD> EPGM graph head type
 */
public class IntersectUsingList<
  VD extends EPGMVertex,
  ED extends EPGMEdge,
  GD extends EPGMGraphHead>
  extends Intersect<VD, ED, GD> {

  @Override
  protected DataSet<VD> computeNewVertices(
    DataSet<GD> newSubgraphs) throws Exception {
    DataSet<GradoopId> identifiers = secondCollection.getGraphHeads()
      .map(new ElementIdOnly<GD>());

    DataSet<VD> vertices = firstCollection.getVertices();
    return vertices.filter(new VertexInGraphsFilterWithBC<VD>())
      .withBroadcastSet(identifiers, VertexInGraphsFilterWithBC.BC_IDENTIFIERS);
  }

  @Override
  public String getName() {
    return IntersectUsingList.class.getName();
  }
}
