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
package org.gradoop.flink.model.impl.operators.propertytransformation;

import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.flink.model.api.functions.PropertyTransformationFunction;
import org.gradoop.flink.model.api.functions.TransformationFunction;

/**
 * Base class for property transformation.
 *
 * @param <EL> graph element to be considered by the transformation
 */
public class BasePropertyTransformationFunction<EL extends Element> implements TransformationFunction<EL> {

  /**
   * Separator between the iteration number and the original property key when the property key
   * shall be kept.
   */
  static final String PROPERTY_VERSION_SEPARATOR = "__";
  /**
   * Label of the element whose property shall be transformed.
   */
  private String label;
  /**
   * Property key.
   */
  private String propertyKey;
  /**
   * Transformation function which shall be applied to a property.
   */
  private PropertyTransformationFunction transformationFunction;
  /**
   * New property key.
   */
  private String newPropertyKey;
  /**
   * True, if all elements if a kind (vertex / edge / graphHead) shall be transformed.
   */
  private boolean transformAllLabels;
  /**
   * True, if the current property key shall be reused.
   */
  private boolean keepCurrentPropertyKey;
  /**
   * True, if the history of a property key shall be kept.
   */
  private boolean keepHistory;

  /**
   * Valued constructor.
   *
   * @param label                      label of the element whose property shall be
   *                                   transformed
   * @param propertyKey                property key
   * @param transformationFunction     transformation function which shall be applied to a
   *                                   property
   * @param newPropertyKey             new property key
   * @param keepHistory                flag to enable versioning
   */
  public BasePropertyTransformationFunction(String label, String propertyKey,
      PropertyTransformationFunction transformationFunction,
      String newPropertyKey, boolean keepHistory) {
    this.label = label;
    this.propertyKey = propertyKey;
    this.transformationFunction = transformationFunction;
    this.newPropertyKey = newPropertyKey;
    this.transformAllLabels = label == null;
    this.keepCurrentPropertyKey = newPropertyKey == null;
    this.keepHistory = keepHistory;
  }

  @Override
  public EL apply(EL current, EL transformed) {
    if (transformationFunction == null) {
      throw new IllegalArgumentException(
        "You must supply a ProppertyTransformationFunction!");
    }

    // transformed will have the current id and graph ids, but not the label or the properties
    transformed.setLabel(current.getLabel());
    transformed.setProperties(current.getProperties());
    // filters relevant elements
    if (transformAllLabels || label.equals(current.getLabel())) {
      if (current.hasProperty(propertyKey)) {
        // save transformed value with the same key
        if (keepCurrentPropertyKey) {
          if (keepHistory) {
            // save the original value with the version number in the property key
            transformed.setProperty(
              propertyKey + PROPERTY_VERSION_SEPARATOR + getNextVersionNumber(current),
              current.getPropertyValue(propertyKey));
          }
          // save the new transformed value
          transformed.setProperty(
            propertyKey,
            transformationFunction.execute(current.getPropertyValue(propertyKey)));
          // new key is used, so the old property is untouched
        } else {
          // store the transformed value with the new key
          transformed.setProperty(
            newPropertyKey,
            transformationFunction.execute(current.getPropertyValue(propertyKey)));
        }
      }
    }
    return transformed;
  }

  /**
   * Returns the next unused version number.
   *
   * @param element element whose property shall be transformed up
   * @return next unused version number
   */
  protected int getNextVersionNumber(EL element) {
    int i = 1;
    while (element.hasProperty(propertyKey + PROPERTY_VERSION_SEPARATOR + i)) {
      i++;
    }
    return i;
  }
}
