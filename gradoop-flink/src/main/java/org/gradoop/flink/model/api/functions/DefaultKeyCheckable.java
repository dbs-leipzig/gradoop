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
package org.gradoop.flink.model.api.functions;

/**
 * Indicates that a key-function can check if a key is considered a default value for that function.<p>
 * This interface needs to be implemented on {@link KeyFunction key functions} where the default key
 * is not unique, but depending on some other conditions, like the label of an element.
 */
public interface DefaultKeyCheckable {

  /**
   * Check if a key is considered a default key for this function.
   *
   * @param key The key to check.
   * @return {@code true}, if the key is effectively a default key.
   */
  boolean isDefaultKey(Object key);
}
