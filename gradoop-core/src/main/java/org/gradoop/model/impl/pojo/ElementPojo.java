package org.gradoop.model.impl.pojo;

import com.google.common.base.Preconditions;
import org.gradoop.model.api.EPGMElement;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.properties.Property;
import org.gradoop.model.impl.properties.PropertyList;
import org.gradoop.model.impl.properties.PropertyValue;

/**
 * Abstract base class for graphs, vertices and edges.
 */
public abstract class ElementPojo implements EPGMElement {
  /**
   * Entity identifier.
   */
  protected GradoopId id;

  /**
   * Label of that entity.
   */
  protected String label;

  /**
   * Internal property storage
   */
  protected PropertyList properties;

  /**
   * Default constructor.
   */
  protected ElementPojo() {
  }

  /**
   * Creates an object from the given parameters. Can only be called by
   * inheriting classes.
   *
   * @param id         entity identifier
   * @param label      entity label
   * @param properties key-value properties
   */
  protected ElementPojo(
    GradoopId id, String label, PropertyList properties) {
    this.id = id;
    this.label = label;
    this.properties = properties;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public GradoopId getId() {
    return id;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setId(GradoopId id) {
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
  public PropertyList getProperties() {
    return properties;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Iterable<String> getPropertyKeys() {
    return (properties != null) ? properties.getKeys() : null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public PropertyValue getPropertyValue(String key) {
    return (properties != null) ? properties.get(key) : null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setProperties(PropertyList properties) {
    this.properties = properties;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setProperty(Property property) {
    Preconditions.checkNotNull(property, "Property was null");
    initProperties();
    this.properties.set(property);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setProperty(String key, Object value) {
    initProperties();
    this.properties.set(key, value);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void setProperty(String key, PropertyValue value) {
    initProperties();
    this.properties.set(key, value);
  }

  @Override
  public int getPropertyCount() {
    return (this.properties != null) ? this.properties.size() : 0;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean hasProperty(String key) {
    return getProperties() != null && getProperties().containsKey(key);
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

    ElementPojo that = (ElementPojo) o;

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
    return String.format("id=%s, label=\"%s\", properties=%s",
      id,
      label,
      properties);
  }

  /**
   * Initializes the internal properties field if necessary.
   */
  private void initProperties() {
    if (this.properties == null) {
      this.properties = new PropertyList();
    }
  }
}
