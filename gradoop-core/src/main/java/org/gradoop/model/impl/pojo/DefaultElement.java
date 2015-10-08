package org.gradoop.model.impl.pojo;

import com.google.common.collect.Maps;
import org.gradoop.model.api.Element;

import java.util.Map;

/**
 * Abstract base class for graphs, vertices and edges.
 */
public abstract class DefaultElement implements Element {
  /**
   * Entity identifier.
   */
  protected Long id;

  /**
   * Label of that entity.
   */
  protected String label;

  /**
   * Internal property storage
   */
  protected Map<String, Object> properties;

  /**
   * Default constructor.
   */
  protected DefaultElement() {
  }

  /**
   * Creates an object from the given parameters. Can only be called by
   * inheriting classes.
   *
   * @param id         entity identifier
   * @param label      label
   * @param properties key-value properties
   */
  protected DefaultElement(Long id, String label,
    Map<String, Object> properties) {
    this.id = id;
    this.label = label;
    this.properties = properties;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Long getId() {
    return id;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setId(Long id) {
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getLabel() {
    return label;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setLabel(String label) {
    this.label = label;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Map<String, Object> getProperties() {
    return properties;
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
  public <T> T getProperty(String key, Class<T> type) {
    return type.cast(getProperty(key));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setProperties(Map<String, Object> properties) {
    this.properties = properties;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setProperty(String key, Object value) {
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

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    DefaultElement that = (DefaultElement) o;

    return !(id != null ? !id.equals(that.id) : that.id != null);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int hashCode() {
    int result = id.hashCode();
    result = 31 * result + id.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "EPGMElement{" +
      "id=" + id +
      ", label='" + label + '\'' +
      ", properties=" + properties +
      '}';
  }
}
