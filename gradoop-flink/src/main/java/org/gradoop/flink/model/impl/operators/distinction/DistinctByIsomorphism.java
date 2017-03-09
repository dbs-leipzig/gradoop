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

package org.gradoop.flink.model.impl.operators.distinction;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.functions.epgm.IdInBroadcast;
import org.gradoop.flink.model.impl.operators.distinction.functions.FirstGraphHead;
import org.gradoop.flink.model.impl.operators.distinction.functions.IdFromGraphHeadString;

/**
 * Returns a distinct collection of logical graphs.
 * Graphs are compared by isomorphism testing.
 */
public class DistinctByIsomorphism extends GroupByIsomorphism {

  /**
   * Default constructor.
   */
  public DistinctByIsomorphism() {
    super(new FirstGraphHead());
  }

  @Override
  public GraphCollection execute(GraphCollection collection) {
    // create canonical labels for all graph heads and choose representative for all distinct ones
    DataSet<GradoopId> graphIds = getCanonicalLabels(collection)
      .distinct(1)
      .map(new IdFromGraphHeadString());

    DataSet<GraphHead> graphHeads = collection.getGraphHeads()
      .filter(new IdInBroadcast<>())
      .withBroadcastSet(graphIds, IdInBroadcast.IDS);

    return selectVerticesAndEdges(collection, graphHeads);
  }

  @Override
  public String getName() {
    return DistinctByIsomorphism.class.getName();
  }
}
