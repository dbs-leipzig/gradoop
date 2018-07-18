/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.impl.operators.sampling.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.api.entities.EPGMElement;
import java.util.List;

/**
 *  Maps an element to the same element in which some given properties are removed
 * @param <E> the given element type
 */
public class RemoveUnnecessaryPropertiesMap<E extends EPGMElement> implements MapFunction<E, E> {
  /**
   * The property names which will be removed from the element
   */
  private List<String> propertyNames;

  /**
   * Constructor
   * @param propertyNames the names of porperties which should be rempoved
   */
  public RemoveUnnecessaryPropertiesMap(List<String> propertyNames) {
    this.propertyNames = propertyNames;
  }

  /**
   * Maps an element to the same element in which some given properties are removed
   *
   * @param epgmElement the element for which the properties should be removed
   * @return the same element
   * @throws Exception in which the properties with the given names are removed
   */
  @Override
  public E map(E epgmElement) throws Exception {
    for (String propertyName : propertyNames) {
      epgmElement.removeProperty(propertyName);
    }
    return epgmElement;
  }
}
