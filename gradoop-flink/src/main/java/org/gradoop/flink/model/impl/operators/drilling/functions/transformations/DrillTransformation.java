/**
 * Copyright © 2014 - 2017 Leipzig University (Database Research Group)
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
import org.gradoop.flink.model.api.functions.TransformationFunction;
import org.gradoop.flink.model.impl.operators.drilling.Drill;
import org.gradoop.flink.model.impl.operators.drilling.functions.drillfunctions.DrillFunction;

/**
 * Base class for drill up transformations.
 *
 * @param <EL> element
 */
public abstract class DrillTransformation<EL extends Element>
  implements TransformationFunction<EL> {

  /**
   * Label of the element whose property shall be drilled.
   */
  private String label;
  /**
   * Property key.
   */
  private String propertyKey;
  /**
   * Drill function which shall be applied to a property.
   */
  private DrillFunction function;
  /**
   * New property key.
   */
  private String otherPropertyKey;

  /**
   * Valued constructor.
   *
   * @param label           label of the element whose property shall be drilled, or
   *                        see {@link Drill#DRILL_ALL_ELEMENTS}
   * @param propertyKey     property key
   * @param function        drill function which shall be applied to a property
   * @param newPropertyKey  new property key, or see {@link Drill#KEEP_CURRENT_PROPERTY_KEY}
   */
  public DrillTransformation(String label, String propertyKey, DrillFunction function,
    String newPropertyKey) {
    this.label = label;
    this.propertyKey = propertyKey;
    this.function = function;
    this.otherPropertyKey = newPropertyKey;
  }

  /**
   * Returns the next unused version number used in drill up.
   *
   * @param element element whose property shall be drilled up
   * @return next unused version number
   */
  protected int getNextDrillUpVersionNumber(EL element) {
    int i = 1;
    while (element.hasProperty(getPropertyKey() + Drill.PROPERTY_VERSION_SEPARATOR + i)) {
      i++;
    }
    return i;
  }

  protected String getLabel() {
    return label;
  }

  protected String getPropertyKey() {
    return propertyKey;
  }

  protected DrillFunction getFunction() {
    return function;
  }

  protected String getOtherPropertyKey() {
    return otherPropertyKey;
  }

  /**
   * Returns true if there is a function specified.
   *
   * @return true if function not null
   */
  protected boolean hasFunction() {
    return function != null;
  }
}
