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
package org.gradoop.benchmark.nesting.serializers.data;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdList;

/**
 * Serializing a vertex in a proper way
 */
public class DeserializeInGraphInformation
  implements MapFunction<String, Tuple2<GradoopId, GradoopIdList>> {


  /**
   * Reusable builder
   */
  private Tuple2<GradoopId, GradoopIdList> sb;

  /**
   * Default constructor
   */
  public DeserializeInGraphInformation() {
    sb = new Tuple2<>();
    sb.f1 = new GradoopIdList();
  }

  @Override
  public Tuple2<GradoopId, GradoopIdList> map(String value) throws Exception {
    sb.f1.clear();
    String[] array = value.split(",");
    sb.f0 = GradoopId.fromString(array[0]);
    for (int i = 1; i < array.length; i++) {
      sb.f1.add(GradoopId.fromString(array[i]));
    }
    return sb;
  }
}
