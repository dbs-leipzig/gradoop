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
package org.gradoop.flink.model.impl.functions.epgm;

import org.gradoop.common.model.api.entities.Element;
import org.gradoop.flink.model.api.functions.TransformationFunction;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A serializable function that is applied on an element (i.e. graph head, vertex and edge)
 * to remove some of its properties.
 *
 * @param <T> the {@link Element} which is target of change.
 */
public class RemoveProperties<T extends Element> implements TransformationFunction<T> {

  /**
   * Keys of properties to be deleted.
   */
  private final String[] propertyKeys;

  /**
   * Constructs {@link RemoveProperties} instance.
   *
   * @param propertyKeys keys of properties to be removed
   */
  public RemoveProperties(String... propertyKeys) {
    checkNotNull(propertyKeys);
    for (String propertyKey : propertyKeys) {
      checkNotNull(propertyKey);
    }
    this.propertyKeys = propertyKeys;
  }

  @Override
  public T apply(T current, T transformed) {
    for (String propertyKey : propertyKeys) {
      current.removeProperty(propertyKey);
    }
    return current;
  }
}
