package org.gradoop.model.impl.operators.cam.functions;

import org.apache.commons.lang3.StringUtils;
import org.gradoop.model.api.EPGMElement;
import org.gradoop.model.impl.properties.Property;
import org.gradoop.model.impl.properties.PropertyList;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public abstract class EPGMElementDataLabeler<EL extends EPGMElement> {

  protected String label(EL el) {

    String label = el.getLabel();

    if(label == null) {
      label = "";
    }

    label += "{";

    PropertyList properties = el.getProperties();

    if(properties != null) {
      List<String> propertyLabels = new ArrayList<>();

      for(Property property : el.getProperties()) {
        propertyLabels.add(property.toString());
      }

      Collections.sort(propertyLabels);

       label += StringUtils.join(propertyLabels, ",");
    }

    label += "}";

    return label;
  }
}
