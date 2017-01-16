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

package org.gradoop.flink.datagen.foodbroker.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdList;
import org.gradoop.common.model.impl.pojo.Edge;

/**
 * Creates a tuple containing the gradoop id of an edges target and the edges graph ids.
 */
public class GraphIdsTupleFromEdge implements
  MapFunction<Edge, Tuple2<GradoopId, GradoopIdList>> {

  @Override
  public Tuple2<GradoopId, GradoopIdList> map(Edge edge) throws Exception {
    return
      new Tuple2<GradoopId, GradoopIdList>(
        edge.getTargetId(), edge.getGraphIds());
  }
}
