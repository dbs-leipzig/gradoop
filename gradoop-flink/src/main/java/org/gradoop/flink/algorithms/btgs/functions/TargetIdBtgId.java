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
package org.gradoop.flink.algorithms.btgs.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * edge -> (targetId, firstGraphId)
 * @param <E> edge type
 */
public class TargetIdBtgId<E extends EPGMEdge> implements
  MapFunction<E, Tuple2<GradoopId, GradoopId>> {

  @Override
  public Tuple2<GradoopId, GradoopId> map(E e) throws Exception {
    return new Tuple2<>(e.getTargetId(), e.getGraphIds().iterator().next());
  }
}
