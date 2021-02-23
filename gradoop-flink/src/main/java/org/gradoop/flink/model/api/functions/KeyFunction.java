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
package org.gradoop.flink.model.api.functions;

import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.Serializable;

/**
 * A function extracting a key from an element. This key may be used as a grouping key.
 * The key will be an object of a certain type, depending on this function.
 *
 * @param <E> The type of the object from which the grouping key is extracted.
 * @param <K> The type of the extracted key.
 */
public interface KeyFunction<E, K> extends Serializable {

  /**
   * Get the key from the element.
   *
   * @param element The element to extract the key from.
   * @return The key.
   */
  K getKey(E element);

  /**
   * Store a key on an element.<p>
   * This is used to store the grouping key on the super element after grouping.<p>
   * The default implementation of this function does not change the element.
   *
   * @param element The element where the key should be stored.
   * @param key     The key to store on the element. The type of the key is expected to be the same as the
   *                type of keys extracted by this key function.
   */
  default void addKeyToElement(E element, Object key) {
  }

  /**
   * Get the type of the key.
   *
   * @return The key type.
   */
  TypeInformation<K> getType();
}
