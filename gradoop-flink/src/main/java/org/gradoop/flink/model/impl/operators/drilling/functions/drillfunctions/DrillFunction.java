/**
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
package org.gradoop.flink.model.impl.operators.drilling.functions.drillfunctions;

import org.gradoop.common.model.impl.properties.PropertyValue;

import java.io.Serializable;

/**
 * Interface for all drill functions which are used for drill down / drill up operations.
 */
public interface DrillFunction extends Serializable {

  /**
   * Returns a changed property value based in the property before drilling.
   *
   * @param property current property
   * @return property after drilling
   */
  PropertyValue execute(PropertyValue property);

}
