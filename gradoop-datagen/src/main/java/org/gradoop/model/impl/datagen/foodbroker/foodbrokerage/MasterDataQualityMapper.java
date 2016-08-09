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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.datagen.foodbroker.config.Constants;
import org.gradoop.model.impl.datagen.foodbroker.tuples.MasterDataTuple;
import org.gradoop.model.impl.id.GradoopId;

/**
 * Creates a master data tuple from the given vertex.
 *
 * @param <V> EPGM vertex type
 */
public class MasterDataQualityMapper<V extends EPGMVertex>
  implements MapFunction<V, Tuple2<GradoopId, Float>> {

  @Override
  public Tuple2<GradoopId, Float> map(V v) throws Exception {
    return new Tuple2<>(v.getId(), v.getPropertyValue(
      Constants.QUALITY).getFloat());
  }
}
