package org.gradoop.model.inmemory;

import org.gradoop.model.SingleLabeled;

import java.util.Map;

/**
 * Created by s1ck on 11/22/14.
 */
public abstract class SingleLabeledPropertyContainer extends PropertyContainer
  implements SingleLabeled {

  private final String label;

  protected SingleLabeledPropertyContainer(String label, Map<String,
    Object> properties) {
    super(properties);
    if (label == null || "".equals(label)) {
      throw new IllegalArgumentException("label must not be null or empty");
    }
    this.label = label;
  }

  @Override
  public String getLabel() {
    return this.label;
  }
}
