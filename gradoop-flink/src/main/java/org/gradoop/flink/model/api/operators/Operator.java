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
package org.gradoop.flink.model.api.operators;

import org.apache.flink.api.java.Utils;

/**
 * Base interface for all graph operators.
 */
public interface Operator {

  /**
   * Modifies Flink operator names to include the Gradoop operator and its call location.
   *
   * @param name name to be formatted
   * @return operator name formatted with brackets
   */
  default String formatName(String name) {
    return String.format("[%s] %s at %s", getName(), name, Utils.getCallLocationName());
  }

  /**
   * Returns the operators name. The operator name is the same as the simple class name, per default.
   *
   * @return operator name
   */
  default String getName() {
    return this.getClass().getSimpleName();
  }
}
