package org.gradoop.model.inmemory;

import com.google.common.collect.Maps;
import org.gradoop.model.Attributed;

import java.util.Map;

/**
 * Abstract entity that holds properties.
 */
public abstract class PropertyContainer implements Attributed {
  protected Map<String, Object> properties;

  protected PropertyContainer(Map<String, Object> properties) {
    this.properties = properties;
  }

  @Override
  public Iterable<String> getPropertyKeys() {
    return properties.keySet();
  }

  @Override
  public Object getProperty(String key) {
    return properties.get(key);
  }

  @Override
  public void addProperty(String key, Object value) {
    if (key == null || "".equals(key)) {
      throw new IllegalArgumentException("key must not be null or empty");
    }
    if (value == null) {
      throw new IllegalArgumentException("value must not be null");
    }
    if (this.properties == null) {
      this.properties = Maps.newHashMap();
    }
    this.properties.put(key, value);
  }
}
