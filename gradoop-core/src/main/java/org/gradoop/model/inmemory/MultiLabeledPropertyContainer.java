package org.gradoop.model.inmemory;

import com.google.common.collect.Lists;
import org.gradoop.model.Identifiable;
import org.gradoop.model.MultiLabeled;

import java.util.List;
import java.util.Map;

/**
 * Abstract entity that holds multiple labels and properties.
 */
public abstract class MultiLabeledPropertyContainer extends PropertyContainer
  implements Identifiable, MultiLabeled {

  protected final Long id;

  protected List<String> labels;

  protected MultiLabeledPropertyContainer(Long id, Iterable<String> labels,
                                          Map<String, Object> properties) {
    super(properties);
    if (id == null) {
      throw new IllegalArgumentException("id must not be null");
    }
    this.id = id;
    this.labels = (labels != null) ? Lists.newArrayList(labels) : null;

  }

  @Override
  public Long getID() {
    return id;
  }

  @Override
  public Iterable<String> getLabels() {
    return labels;
  }

  @Override
  public void addLabel(String label) {
    if (label == null || "".equals(label)) {
      throw new IllegalArgumentException("label must not be null or empty");
    }
    if (this.labels == null) {
      this.labels = Lists.newArrayList();
    }
    this.labels.add(label);
  }
}
