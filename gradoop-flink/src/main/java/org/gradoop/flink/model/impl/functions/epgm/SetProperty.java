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
package org.gradoop.flink.model.impl.functions.epgm;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.common.model.impl.properties.PropertyValue;

/**
 * The SetProperty MapFunction assigns a new property to a given graph element using the supplied
 * property key and value.
 *
 * @param <E> gradoop element
 */
@FunctionAnnotation.ForwardedFields("id;label")
public class SetProperty<E extends Element> implements MapFunction<E, E> {

  /**
   * serialVersionUID
   */
  private static final long serialVersionUID = 42L;

  /**
   * Property key used to set the value.
   */
  private String propertyKey;

  /**
   * Property value to be set.
   */
  private PropertyValue propertyValue;

  /**
   * Creates an instance of SetProperty.
   *
   * @param propertyKey the property key
   * @param propertyValue property value to be set
   */
  public SetProperty(String propertyKey, PropertyValue propertyValue) {
    this.propertyKey = propertyKey;
    this.propertyValue = propertyValue;
  }

  /**
   * Adds a new property to the element.
   *
   * @param element original element to be updated
   * @return updated element
   */
  @Override
  public E map(E element) throws Exception {
    element.setProperty(propertyKey, propertyValue);
    return element;
  }
}
