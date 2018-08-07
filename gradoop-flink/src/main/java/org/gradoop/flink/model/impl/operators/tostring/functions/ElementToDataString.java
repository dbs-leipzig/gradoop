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
package org.gradoop.flink.model.impl.operators.tostring.functions;

import org.apache.commons.lang3.StringUtils;
import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.common.model.impl.properties.Property;
import org.gradoop.common.model.impl.properties.Properties;

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

    Properties properties = el.getProperties();

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
