package org.gradoop.model.impl.operators.equality.functions;

import com.google.common.collect.Lists;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Created by peet on 23.11.15.
 */
public abstract class ElementBaseLabeler {

  protected String label(Map<String, Object> properties) {



    StringBuilder builder = new StringBuilder("{");

    if (properties != null) {

      List<String> keys = Lists.newArrayList(properties.keySet());
      Collections.sort(keys);


      boolean first = true;

      for(String key : keys) {

        if(!first) {
          builder.append(",");
        } else {
          first = false;
        }

        Object value = properties.get(key);

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
