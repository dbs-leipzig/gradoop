package org.gradoop.model.inmemory;

import org.gradoop.model.SingleLabeled;

import java.util.Map;

/**
 * Abstract entity that holds a single labels and properties.
 */
public abstract class SingleLabeledPropertyContainer extends PropertyContainer
  implements SingleLabeled {

  /**
   * Label of that entity.
   */
  private final String label;

  /**
   * Creates an object from the given parameters. Can only be called by
   * inheriting classes.
   *
   * @param label      label (must not be {@code null} or empty)
   * @param properties key-value-map (can be {@code null})
   */
  protected SingleLabeledPropertyContainer(String label, Map<String,
    Object> properties) {
    super(properties);
    if (label == null || "".equals(label)) {
      throw new IllegalArgumentException("label must not be null or empty");
    }
    this.label = label;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getLabel() {
    return this.label;
  }
}
