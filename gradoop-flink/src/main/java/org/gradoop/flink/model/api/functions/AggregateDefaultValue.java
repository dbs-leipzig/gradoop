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
package org.gradoop.flink.model.api.functions;

import org.gradoop.common.model.impl.properties.PropertyValue;

/**
 * Describes an extension of an {@link AggregateFunction}, in the case there
 * is a logical default, e.g., when counting specific vertex labels and there
 * is no such vertex than one can specify 0 which will be returned instead of
 * NULL.
 */
public interface AggregateDefaultValue {

  /**
   * Defines the default value.
   *
   * @return default value.
   */
  PropertyValue getDefaultValue();
}
