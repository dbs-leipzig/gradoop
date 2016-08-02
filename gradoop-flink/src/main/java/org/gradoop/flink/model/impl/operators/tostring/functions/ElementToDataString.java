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
package org.gradoop.flink.model.impl.operators.tostring.functions;

import org.apache.commons.lang3.StringUtils;
import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.common.model.impl.properties.Property;
import org.gradoop.common.model.impl.properties.PropertyList;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * superclass of data-bases string representations of EPGM elements, i.e.,
 * such including label and properties
 * @param <EL> EPGM element type
 */
public abstract class ElementToDataString<EL extends Element> {

  /**
   * generalization of label and properties string concatenation
   * @param el graph head, vertex or edge
   * @return string representation
   */
  protected String label(EL el) {

    String label = el.getLabel();

    if (label == null) {
      label = "";
    }

    label += "{";

    PropertyList properties = el.getProperties();

    if (properties != null) {
      List<String> propertyLabels = new ArrayList<>();

      for (Property property : el.getProperties()) {
        propertyLabels.add(property.toString());
      }

      Collections.sort(propertyLabels);

      label += StringUtils.join(propertyLabels, ",");
    }

    label += "}";

    return label;
  }
}
