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

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.flink.model.api.functions.PropertyTransformationFunction;

/**
 * Executes the transformation function.
 *
 * @param <EL> EPGM element type
 */
public class PropertyTransformationBase<EL extends Element>
  implements MapFunction<EL, EL> {

  /**
   * Element modification function
   */
  private final BasePropertyTransformationFunction<EL> transformationFunction;

  /**
   * Constructor
   *
   * @param label                      label of the element whose property shall be
   *                                   transformed
   * @param propertyKey                property key
   * @param transformationFunction     element modification function
   * @param newPropertyKey             new property key
   * @param keepHistory                flag to enable versioning
   */
  protected PropertyTransformationBase(String label, String propertyKey,
        PropertyTransformationFunction transformationFunction, String newPropertyKey,
        boolean keepHistory) {
    this.transformationFunction = new BasePropertyTransformationFunction<>(label, propertyKey,
        transformationFunction, newPropertyKey, keepHistory);
  }

  /**
   * Applies the modification function on the current element and its copy.
   *
   * @param element current element
   * @return modified element
   * @throws Exception
   */
  @Override
  public EL map(EL element) throws Exception {
    return transformationFunction.apply(element, element);
  }
}
