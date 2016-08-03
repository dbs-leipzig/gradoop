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

package org.gradoop.flink.model.impl.functions.epgm;

import com.google.common.collect.Lists;
import org.gradoop.flink.model.api.functions.UnaryFunction;
import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.common.model.impl.properties.PropertyValue;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Extracts a list of {@link PropertyValue} instances from a given entity using
 * a list of property keys. The order of the property keys determines the order
 * of the values in the result. If the EPGM element does not have a property,
 * the property value will be {@code PropertyValue.NULL_VALUE}.
 *
 * @param <EL> EPGM element
 */
public class PropertyGetter<EL extends Element>
  implements UnaryFunction<EL, List<PropertyValue>> {

  /**
   * Property keys to retrieve property values
   */
  private final List<String> propertyKeys;

  /**
   * Constructor
   *
   * @param propertyKeys property keys to retrieve values for
   */
  public PropertyGetter(List<String> propertyKeys) {
    this.propertyKeys = checkNotNull(propertyKeys);
  }

  @Override
  public List<PropertyValue> execute(EL entity) throws Exception {
    List<PropertyValue> propertyValueList =
      Lists.newArrayListWithCapacity(propertyKeys.size());

    for (String propertyKey : propertyKeys) {
      if (entity.hasProperty(propertyKey)) {
        propertyValueList.add(entity.getPropertyValue(propertyKey));
      } else {
        propertyValueList.add(PropertyValue.NULL_VALUE);
      }
    }
    return propertyValueList;
  }
}
