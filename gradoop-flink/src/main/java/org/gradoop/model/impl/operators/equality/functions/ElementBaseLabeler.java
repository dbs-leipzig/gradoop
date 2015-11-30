package org.gradoop.model.impl.operators.equality.functions;

import com.google.common.collect.Lists;
import org.gradoop.model.impl.properties.PropertyList;

import java.util.Collections;
import java.util.List;

public abstract class ElementBaseLabeler {

  protected String label(PropertyList properties) {

    StringBuilder builder = new StringBuilder("{");

    if (properties != null) {

      List<String> keys = Lists.newArrayList(properties.getKeys());
      Collections.sort(keys);

      boolean first = true;

      for(String key : keys) {

        if(!first) {
          builder.append(",");
        } else {
          first = false;
        }

        Object value = properties.get(key).getObject();

        builder.append(key).append("=");

        if(value instanceof String) {
          builder.append('"').append(value.toString()).append('"');
        } else {
          builder.append(value);
        }
      }

    }

    builder.append("}");

    return builder.toString();
  }
}
