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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.api.entities.Element;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Creates a mapping between IDs as pairs where the entries are IDs of two elements which are considered
 * duplicates of eachother. Note that there may be any number of duplicates per element.
 * <p>
 * The mapping will be extracted from elements annotated with the {@link MarkDuplicatesInGroup#PROPERTY_KEY}
 * property key. The first pair entry will be the ID of the element and the second the value of the property.
 *
 * @param <E> The type of the elements.
 */
public class CreateMappingFromMarkedDuplicates<E extends Element>
  implements FlatMapFunction<E, Tuple2<GradoopId, GradoopId>> {

  /**
   * Reduce object instantiations.
   */
  private final Tuple2<GradoopId, GradoopId> reuse = new Tuple2<>();

  @Override
  public void flatMap(E element, Collector<Tuple2<GradoopId, GradoopId>> out) {
    if (element.hasProperty(MarkDuplicatesInGroup.PROPERTY_KEY)) {
      reuse.f0 = element.getId();
      reuse.f1 = element.getPropertyValue(MarkDuplicatesInGroup.PROPERTY_KEY).getGradoopId();
      out.collect(reuse);
    }
  }
}
