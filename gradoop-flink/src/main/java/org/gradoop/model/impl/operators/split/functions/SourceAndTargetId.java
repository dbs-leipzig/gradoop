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

package org.gradoop.model.impl.operators.split.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.util.Collector;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.impl.id.GradoopId;

/**
 * Returns the source and target ids of an edge as tuple1.
 *
 * @param <E> EPGM edge type
 */
public class SourceAndTargetId<E extends EPGMEdge>
  implements FlatMapFunction<E, Tuple1<GradoopId>> {

  /**
   * Reduce object instantiation.
   */
  private Tuple1<GradoopId> reuseTuple = new Tuple1<>();

  @Override
  public void flatMap(E edge, Collector<Tuple1<GradoopId>> collector) {
    reuseTuple.f0 = edge.getSourceId();
    collector.collect(reuseTuple);

    reuseTuple.f0 = edge.getTargetId();
    collector.collect(reuseTuple);
  }
}
