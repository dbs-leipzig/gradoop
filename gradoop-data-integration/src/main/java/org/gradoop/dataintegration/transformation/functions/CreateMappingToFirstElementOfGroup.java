/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.dataintegration.transformation.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.api.entities.Identifiable;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * A group reduce function reducing a group of element to pairs of IDs.<p>
 * This will return one pair per group element, containing the following fields:
 * <ul start="0">
 *   <li>the ID of the current element of the group</li>
 *   <li>the ID of the first element of the group</li>
 * </ul>
 *
 * @param <E> The type of the elements to reduce.
 */
public class CreateMappingToFirstElementOfGroup<E extends Identifiable>
  implements GroupReduceFunction<E, Tuple2<GradoopId, GradoopId>> {

  /**
   * Reduce object instantiations.
   */
  private final Tuple2<GradoopId, GradoopId> reuse = new Tuple2<>();

  @Override
  public void reduce(Iterable<E> elements, Collector<Tuple2<GradoopId, GradoopId>> out) {
    boolean first = true;
    for (E element : elements) {
      if (first) {
        reuse.f1 = element.getId();
        first = false;
      }
      reuse.f0 = element.getId();
      out.collect(reuse);
    }
  }
}
