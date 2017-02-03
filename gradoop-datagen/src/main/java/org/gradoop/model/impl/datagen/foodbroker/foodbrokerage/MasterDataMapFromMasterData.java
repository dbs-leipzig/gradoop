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

package org.gradoop.model.impl.datagen.foodbroker.foodbrokerage;

import com.google.common.collect.Maps;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.datagen.foodbroker.tuples.AbstractMasterDataTuple;
import org.gradoop.model.impl.id.GradoopId;

import java.util.Map;

/**
 * Returns a map from each gradoop id to the object.
 */
public class MasterDataMapFromMasterData
  implements GroupReduceFunction<AbstractMasterDataTuple,
  Map<GradoopId, AbstractMasterDataTuple>> {

  @Override
  public void reduce(Iterable<AbstractMasterDataTuple> iterable,
    Collector<Map<GradoopId, AbstractMasterDataTuple>> collector) throws
    Exception {
    Map<GradoopId, AbstractMasterDataTuple> map = Maps.newHashMap();
    for(AbstractMasterDataTuple tuple : iterable) {
      map.put(tuple.getId(), tuple);
    }
    collector.collect(map);
  }
}
