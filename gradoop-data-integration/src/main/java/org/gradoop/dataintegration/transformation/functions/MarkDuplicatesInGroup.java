/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
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
import org.apache.flink.util.Collector;
import org.gradoop.common.model.api.entities.Element;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.PropertyValue;

/**
 * Given a group of elements, this function marks all but the first element as duplicates by adding a
 * property storing the original element ID (the element of which the others are duplicates).<p>
 * Note that this will delete unnecessary properties from duplicate elements.
 *
 * @param <E> The element type.
 */
public class MarkDuplicatesInGroup<E extends Element> implements GroupReduceFunction<E, E> {

  /**
   * The property key used to identify the original element, i.e. the element of which the current element
   * is a duplicate.
   */
  public static final String PROPERTY_KEY = "__dup";

  @Override
  public void reduce(Iterable<E> elements, Collector<E> out) {
    boolean first = true;
    Properties propertiesForDuplicate = Properties.create();
    for (E element : elements) {
      if (first) {
        PropertyValue originalId = PropertyValue.create(element.getId());
        propertiesForDuplicate.set(PROPERTY_KEY, originalId);
        first = false;
      } else {
        element.setProperties(propertiesForDuplicate);
      }
      out.collect(element);
    }
  }
}
