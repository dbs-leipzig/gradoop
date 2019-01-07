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
import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;

import java.util.Objects;

/**
 * A simple {@link FlatMapFunction} that prepares element data for further processing.
 * Since not all elements necessarily have the property a flat map is used.
 *
 * @param <E> The element type.
 */
public class BuildIdPropertyValuePairs<E extends EPGMElement>
  implements FlatMapFunction<E, Tuple2<GradoopId, PropertyValue>> {

  /**
   * The label of elements from which to propagate the property.
   */
  private final String label;

  /**
   * The property key of the property to propagate.
   */
  private final String propertyKey;

  /**
   * Reduce object instantiations.
   */
  private final Tuple2<GradoopId, PropertyValue> reuse;

  /**
   * The constructor of the {@link FlatMapFunction} to create {@link GradoopId} /
   * {@link PropertyValue} pairs.
   *
   * @param label       The label of the elements to propagate from.
   * @param propertyKey The property key of the property to propagate.
   */
  public BuildIdPropertyValuePairs(String label, String propertyKey) {
    this.label = Objects.requireNonNull(label);
    this.propertyKey = Objects.requireNonNull(propertyKey);
    this.reuse = new Tuple2<>();
  }

  @Override
  public void flatMap(E element, Collector<Tuple2<GradoopId, PropertyValue>> out) {
    if (!label.equals(element.getLabel()) || !element.hasProperty(propertyKey)) {
      return;
    }
    reuse.f0 = element.getId();
    reuse.f1 = element.getPropertyValue(propertyKey);
    out.collect(reuse);
  }
}
