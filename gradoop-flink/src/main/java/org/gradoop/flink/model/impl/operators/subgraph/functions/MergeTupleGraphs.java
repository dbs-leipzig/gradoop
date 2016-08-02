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

package org.gradoop.flink.model.impl.operators.subgraph.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;

/**
 * Reduces groups of tuples 2 containin two gradoop ids into one tuple
 * per group, containing the first gradoop id and a gradoop id set,
 * containing all the second gradoop ids.
 */
@FunctionAnnotation.ReadFields("f1")
@FunctionAnnotation.ForwardedFields("f0->f0")
public class MergeTupleGraphs implements
  GroupReduceFunction<
    Tuple2<GradoopId, GradoopId>,
    Tuple2<GradoopId, GradoopIdSet>> {

  @Override
  public void reduce(Iterable<Tuple2<GradoopId, GradoopId>> iterable,
    Collector<Tuple2<GradoopId, GradoopIdSet>> collector) throws Exception {
    GradoopIdSet set = new GradoopIdSet();
    boolean empty = true;
    GradoopId first = null;
    for (Tuple2<GradoopId, GradoopId> tuple : iterable) {
      set.add(tuple.f1);
      empty = false;
      first = tuple.f0;
    }
    if (!empty) {
      collector.collect(new Tuple2<>(first, set));
    }
  }
}
