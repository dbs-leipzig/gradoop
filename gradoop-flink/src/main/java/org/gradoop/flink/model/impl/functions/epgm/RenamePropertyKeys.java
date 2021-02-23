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

import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A serializable function that is applied on an element (i.e. graph head,
 * vertex and edge) to rename property keys. If the new property key is already in use
 * the value will also be overwritten
 *
 *  @param <T> the {@link Element} which is target of change.
 */
public class RenamePropertyKeys<T extends Element> implements TransformationFunction<T> {

  /**
   * a map containing the mappings from old property key names to the new ones
   */
  private final Map<String, String> keyMappings;

  /**
   * Constructor
   *
   * @param keyMappings the map consists of {@code <old Key , new Key>}
   */
  public RenamePropertyKeys(Map<String, String> keyMappings) {
    this.keyMappings = checkNotNull(keyMappings);
    for (Map.Entry mapping : keyMappings.entrySet()) {
      checkNotNull(mapping.getKey());
      checkNotNull(mapping.getValue());
    }
  }

  @Override
  public T apply(T current, T transformed) {

    for (Map.Entry<String, String> mapping : keyMappings.entrySet()) {
      if (current.getPropertyValue(mapping.getKey()) != null) {
        current.setProperty(mapping.getValue(), current.getPropertyValue(mapping.getKey()));
        current.removeProperty(mapping.getKey());
      }
    }

    return current;
  }
}
