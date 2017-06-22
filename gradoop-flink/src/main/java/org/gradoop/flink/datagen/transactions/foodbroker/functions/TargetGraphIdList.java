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

package org.gradoop.flink.datagen.transactions.foodbroker.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdList;

import java.util.Iterator;

/**
 * Reduces for each target id of an edge all the graph ids to one graph id list.
 */
public class TargetGraphIdList
  implements GroupReduceFunction<Tuple2<GradoopId, GradoopId>, Tuple2<GradoopId, GradoopIdList>> {

  @Override
  public void reduce(Iterable<Tuple2<GradoopId, GradoopId>> values,
    Collector<Tuple2<GradoopId, GradoopIdList>> out) throws Exception {

    Iterator<Tuple2<GradoopId, GradoopId>> iterator = values.iterator();

    Tuple2<GradoopId, GradoopId> pair = iterator.next();

    // the target id is the same for each iterator element
    GradoopId targetId = pair.f0;
    GradoopIdList graphIds = GradoopIdList.fromExisting(pair.f1);

    while (iterator.hasNext()) {
      graphIds.add(iterator.next().f1);
    }

    out.collect(new Tuple2<>(targetId, graphIds));
  }
}