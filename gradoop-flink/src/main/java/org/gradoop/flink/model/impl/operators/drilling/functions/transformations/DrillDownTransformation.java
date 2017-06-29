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
 * Logical graph transformation which drills down a property value of a given key. This class is
 * used for vertices and edges.
 *
 * @param <EL> element
 */
public class DrillDownTransformation<EL extends Element> extends DrillTransformation<EL> {

  /**
   * Valued constructor.
   *
   * @param label          label of the element whose property shall be drilled, or
   *                       see {@link Drill#DRILL_ALL_ELEMENTS}
   * @param propertyKey    property key
   * @param function       drill function which shall be applied to a property
   * @param oldPropertyKey new property key, or see {@link Drill#KEEP_CURRENT_PROPERTY_KEY}
   */
  public DrillDownTransformation(String label, String propertyKey, DrillFunction function,
    String oldPropertyKey) {
    super(label, propertyKey, function, oldPropertyKey);
  }

  @Override
  public EL apply(EL current, EL transformed) {
    // transformed will have the current id and graph ids, but not the label or the properties
    transformed.setLabel(current.getLabel());
    transformed.setProperties(current.getProperties());
    // filters relevant elements
    if (getLabel().equals(Drill.DRILL_ALL_ELEMENTS) || getLabel().equals(current.getLabel())) {
      if (current.hasProperty(getPropertyKey())) {
        // drill down is preceded by roll up
        if (!hasFunction()) {
          // roll up was stored with the same label
          if (getOtherPropertyKey().equals(Drill.KEEP_CURRENT_PROPERTY_KEY)) {
            // get the last used number in the roll up step
            int i = getNextRollUpVersionNumber(current) - 1;
            // save the property on the next level to the key
            transformed.setProperty(getPropertyKey(),
              current.getPropertyValue(getPropertyKey() + Drill.PROPERTY_VERSION_SEPARATOR + i));
            // remove the property which is now one level above
            transformed.getProperties().remove(
              getPropertyKey() + Drill.PROPERTY_VERSION_SEPARATOR + i);
          // roll was stored under a new label
          } else {
            if (current.hasProperty(getOtherPropertyKey())) {
              transformed.getProperties().remove(getPropertyKey());
            }
          }
        // drill down has its own function
        } else {
          // drill shall be saved with the same label
          if (getOtherPropertyKey().equals(Drill.KEEP_CURRENT_PROPERTY_KEY)) {
            int i = getNextDrillDownVersionNumber(current);
            // store current value under the next version number
            transformed.setProperty(
              getPropertyKey() + Drill.PROPERTY_VERSION_SEPARATOR + i,
              current.getPropertyValue(getPropertyKey()));
            // store the drilled value under the property key
            transformed.setProperty(
              getPropertyKey(),
              getFunction().execute(current.getPropertyValue(getPropertyKey())));
          // drill shall be saved with new label
          } else {
            // store the drilled value with the new key
            transformed.setProperty(
              getOtherPropertyKey(),
              getFunction().execute(current.getPropertyValue(getPropertyKey())));
          }
        }
      }
    }
    return transformed;
  }



}
