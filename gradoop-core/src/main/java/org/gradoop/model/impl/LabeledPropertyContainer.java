package org.gradoop.model.impl;

import org.gradoop.model.Identifiable;
import org.gradoop.model.Labeled;

import java.util.Map;

/**
 * Abstract entity that holds a single labels and properties.
 */
public abstract class LabeledPropertyContainer extends
  PropertyContainer implements Identifiable, Labeled {

  /**
   * Entity identifier.
   */
  protected final Long id;

  /**
   * Label of that entity.
   */
  protected String label;

  /**
   * Creates an object from the given parameters. Can only be called by
   * inheriting classes.
   *
   * @param id         entity identifier
   * @param label      label
   * @param properties key-value-map
   */
  protected LabeledPropertyContainer(Long id, String label,
    Map<String, Object> properties) {
    super(properties);
    this.id = id;
    this.label = label;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Long getID() {
    return id;
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
}
