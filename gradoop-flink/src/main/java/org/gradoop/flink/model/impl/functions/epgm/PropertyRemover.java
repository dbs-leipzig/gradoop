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
package org.gradoop.flink.model.impl.functions.epgm;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.pojo.Element;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Remove a property from an EPGM element.
 *
 * @param <E> EPGM element type.
 */
public class PropertyRemover<E extends Element> implements MapFunction<E, E> {

  /**
   * Property key to remove.
   */
  private final String propertyKey;

  /**
   * Initialize.
   *
   * @param propertyKey Property key to remove.
   */
  public PropertyRemover(String propertyKey) {
    this.propertyKey = checkNotNull(propertyKey);
  }

  @Override
  public E map(E e) throws Exception {
    e.removeProperty(propertyKey);
    return e;
  }
}
