package org.gradoop.model.inmemory;

import com.google.common.collect.Maps;
import org.gradoop.model.Attributed;

import java.util.Map;

/**
 * Abstract entity that holds properties.
 */
public abstract class PropertyContainer implements Attributed {
  protected Map<String, Object> properties;

  /**
   * Creates an object from the given parameters. Can only be called by
   * inheriting classes.
   *
   * @param properties key-value-map (can be {@code null})
   */
  protected PropertyContainer(Map<String, Object> properties) {
    this.properties = properties;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Iterable<String> getPropertyKeys() {
    return (properties != null) ? properties.keySet() : null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Object getProperty(String key) {
    return (properties != null) ? properties.get(key) : null;
  }

  /**
   * {@inheritDoc}
   */
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

  @Override
  public int getPropertyCount() {
    return (this.properties != null) ? this.properties.size() : 0;
  }
}
