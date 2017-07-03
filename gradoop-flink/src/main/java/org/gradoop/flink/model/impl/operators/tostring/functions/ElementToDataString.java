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
