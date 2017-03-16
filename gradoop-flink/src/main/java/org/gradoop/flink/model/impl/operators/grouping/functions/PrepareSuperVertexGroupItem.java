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

package org.gradoop.flink.model.impl.operators.grouping.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.grouping.tuples.SuperEdgeGroupItem;

import java.util.Set;

/**
 * Returns a tuple which assigns each source/target set of gradoop ids the edhe id
 */
public class PrepareSuperVertexGroupItem
  implements FlatMapFunction<SuperEdgeGroupItem, Tuple2<Set<GradoopId>, GradoopId>> {

  /**
   * Avoid object initialization in each call.
   */
  private Tuple2<Set<GradoopId>, GradoopId> reuseTuple;

  /**
   * Constructor to initialize object.
   */
  public PrepareSuperVertexGroupItem() {
    reuseTuple = new Tuple2<Set<GradoopId>, GradoopId>();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void flatMap(SuperEdgeGroupItem superEdgeGroupItem,
    Collector<Tuple2<Set<GradoopId>, GradoopId>> collector) throws Exception {

    reuseTuple.setFields(superEdgeGroupItem.getSourceIds(), superEdgeGroupItem.getEdgeId());
    collector.collect(reuseTuple);

    reuseTuple.setFields(superEdgeGroupItem.getTargetIds(), superEdgeGroupItem.getEdgeId());
    collector.collect(reuseTuple);
  }
}
