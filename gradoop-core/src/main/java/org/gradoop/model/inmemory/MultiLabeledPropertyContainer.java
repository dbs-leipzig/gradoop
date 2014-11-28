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

  /**
   * Unique identifier.
   */
  protected final Long id;

  /**
   * Holds all labels of that entity.
   */
  protected List<String> labels;

  /**
   * Creates an object from the given parameters. Can only be called by
   * inheriting classes.
   *
   * @param id         unique id
   * @param labels     labels (can be {@code null})
   * @param properties key-value-map (can be {@code null})
   */
  protected MultiLabeledPropertyContainer(Long id, Iterable<String> labels,
                                          Map<String, Object> properties) {
    super(properties);
    if (id == null) {
      throw new IllegalArgumentException("id must not be null");
    }
    this.id = id;
    this.labels = (labels != null) ? Lists.newArrayList(labels) : null;

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
  public Iterable<String> getLabels() {
    return labels;
  }

  /**
   * {@inheritDoc}
   */
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
