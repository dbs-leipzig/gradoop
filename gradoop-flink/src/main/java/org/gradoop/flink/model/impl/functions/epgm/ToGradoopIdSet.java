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

package org.gradoop.flink.model.impl.functions.epgm;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;

/**
 * id1,..,idn => {id1,..,idn}
 */
public class ToGradoopIdSet
  implements GroupReduceFunction<GradoopId, GradoopIdSet> {

  @Override
  public void reduce(Iterable<GradoopId> iterable,
    Collector<GradoopIdSet> collector) throws Exception {

    GradoopIdSet ids = new GradoopIdSet();

    for (GradoopId id : iterable) {
      ids.add(id);
    }

    collector.collect(ids);
  }
}
