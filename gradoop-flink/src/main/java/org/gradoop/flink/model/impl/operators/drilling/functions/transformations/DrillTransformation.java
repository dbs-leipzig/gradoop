/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.flink.model.impl.operators.drilling.functions.transformations;

import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.flink.model.api.functions.TransformationFunction;
import org.gradoop.flink.model.impl.operators.drilling.Drill;
import org.gradoop.flink.model.impl.operators.drilling.functions.drillfunctions.DrillFunction;

/**
 * Base class for roll up / drill transformations.
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
   * @param label label of the element whose property shall be drilled, or
   *              see {@link Drill#DRILL_ALL_ELEMENTS}
   * @param propertyKey property key
   * @param function drill function which shall be applied to a property
   * @param newPropertyKey new property key, or see {@link Drill#KEEP_CURRENT_PROPERTY_KEY}
   */
  public DrillTransformation(String label, String propertyKey, DrillFunction function,
    String newPropertyKey) {
    this.label = label;
    this.propertyKey = propertyKey;
    this.function = function;
    this.otherPropertyKey = newPropertyKey;
  }

  /**
   * Returns the next unused version number used in roll up.
   *
   * @param element element whose property shall be rolled up
   * @return next unused version number
   */
  protected int getNextRollUpVersionNumber(EL element) {
    int i = 1;
    while (element.hasProperty(getPropertyKey() + Drill.PROPERTY_VERSION_SEPARATOR + i)){
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

  protected boolean hasFunction() {
    return function != null;
  }
}
