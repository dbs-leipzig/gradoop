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
import org.gradoop.flink.model.impl.operators.drilling.Drill;
import org.gradoop.flink.model.impl.operators.drilling.functions.drillfunctions.DrillFunction;

/**
 * Logical graph transformation which rolls up a property value of a given key. This class is
 * used for vertices and edges.
 *
 * @param <EL> element
 */
public class RollUpTransformation<EL extends Element> extends DrillTransformation<EL> {

  /**
   * Valued constructor.
   *
   * @param label          getLabel() of the element whose property shall be drilled, or
   *                       see {@link Drill#DRILL_ALL_ELEMENTS}
   * @param propertyKey    property key
   * @param function       drill function which shall be applied to a property
   * @param newPropertyKey new property key, or see {@link Drill#KEEP_CURRENT_PROPERTY_KEY}
   */
  public RollUpTransformation(String label, String propertyKey, DrillFunction function,
    String newPropertyKey) {
    super(label, propertyKey, function, newPropertyKey);
  }

  @Override
  public EL apply(EL current, EL transformed) {
    // transformed will have the current id and graph ids, but not the label or the properties
    transformed.setLabel(current.getLabel());
    transformed.setProperties(current.getProperties());
    // filters relevant elements
    if (getLabel().equals(Drill.DRILL_ALL_ELEMENTS) || getLabel().equals(current.getLabel())) {
      if (current.hasProperty(getPropertyKey())) {
        // safe rolled up value with the same key
        if (getOtherPropertyKey().equals(Drill.KEEP_CURRENT_PROPERTY_KEY)) {
          // safe the original value with the version number in the property key
          transformed.setProperty(
            getPropertyKey() + Drill.PROPERTY_VERSION_SEPARATOR +
              getNextRollUpVersionNumber(current),
            current.getPropertyValue(getPropertyKey()));
          // safe the new rolled value
          transformed.setProperty(
            getPropertyKey(),
            getFunction().execute(current.getPropertyValue(getPropertyKey())));
          // new key is used, so the old property is untouched
        } else {
          // store the rolled value with the new key
          transformed.setProperty(
            getOtherPropertyKey(),
            getFunction().execute(current.getPropertyValue(getPropertyKey())));
        }
      }
    }
    return transformed;
  }

}
