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

/**
 * A (grouping) key function with a default value. The value will be used in some cases where the key can
 * not be determined or when this key function is not applicable for an element.<p>
 * This key function will work exactly like a {@link KeyFunction} in most cases.
 *
 * @param <E> The type of the object from which the grouping key is extracted.
 * @param <K> The type of the extracted key.
 */
public interface KeyFunctionWithDefaultValue<E, K> extends KeyFunction<E, K> {

  /**
   * Get the default key value for all elements.
   *
   * @return The default key.
   */
  K getDefaultKey();
}
