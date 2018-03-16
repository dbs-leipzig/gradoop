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
package org.gradoop.flink.model.impl.operators.drilling.functions.transformations;

import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.flink.model.impl.operators.drilling.functions.drillfunctions.DrillFunction;

/**
 * Logical graph transformation which drills up a property value of a given key. This class is
 * used for vertices and edges.
 *
 * @param <EL> element
 */
public class DrillUpTransformation<EL extends Element> extends DrillTransformation<EL> {

  /**
   * Valued constructor.
   *
   * @param label                  label of the element whose property shall be drilled
   * @param propertyKey            property key
   * @param function               drill function which shall be applied to a property
   * @param newPropertyKey         new property key
   * @param drillAllLabels         true, if all elements of a kind (vertex / edge) shall be drilled
   * @param keepCurrentPropertyKey true, if the current property key shall be reused
   */
  public DrillUpTransformation(String label, String propertyKey, DrillFunction function,
    String newPropertyKey, boolean drillAllLabels, boolean keepCurrentPropertyKey) {
    super(label, propertyKey, function, newPropertyKey, drillAllLabels, keepCurrentPropertyKey);
  }

  @Override
  public EL apply(EL current, EL transformed) {
    // transformed will have the current id and graph ids, but not the label or the properties
    transformed.setLabel(current.getLabel());
    transformed.setProperties(current.getProperties());
    // filters relevant elements
    if (drillAllLabels() || getLabel().equals(current.getLabel())) {
      if (current.hasProperty(getPropertyKey())) {
        // save drilled up value with the same key
        if (keepCurrentPropertyKey()) {
          // save the original value with the version number in the property key
          transformed.setProperty(
            getPropertyKey() + PROPERTY_VERSION_SEPARATOR + getNextDrillUpVersionNumber(current),
            current.getPropertyValue(getPropertyKey()));
          // save the new drilled value
          transformed.setProperty(
            getPropertyKey(),
            getFunction().execute(current.getPropertyValue(getPropertyKey())));
          // new key is used, so the old property is untouched
        } else {
          // store the drilled value with the new key
          transformed.setProperty(
            getNewPropertyKey(),
            getFunction().execute(current.getPropertyValue(getPropertyKey())));
        }
      }
    }
    return transformed;
  }

}
