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
package org.gradoop.flink.model.impl.operators.keyedgrouping.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.api.entities.Identifiable;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Create a mapping (in the form of a {@link Tuple2}) from the ID of an element to itself.
 *
 * @param <E> The element type.
 */
public class CreateElementMappingToSelf<E extends Identifiable>
  implements MapFunction<E, Tuple2<GradoopId, GradoopId>> {

  /**
   * Reduce object instantiations.
   */
  private final Tuple2<GradoopId, GradoopId> reuse = new Tuple2<>();

  @Override
  public Tuple2<GradoopId, GradoopId> map(E element) {
    reuse.f0 = element.getId();
    reuse.f1 = element.getId();
    return reuse;
  }
}
