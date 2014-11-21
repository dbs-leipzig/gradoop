package org.gradoop.model.inmemory;

import com.google.common.collect.Lists;
import org.gradoop.model.Attributed;
import org.gradoop.model.Identifiable;
import org.gradoop.model.Labeled;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by martin on 05.11.14.
 */
public abstract class LabeledPropertyContainer implements Identifiable,
  Attributed, Labeled {

  protected final Long id;

  protected final List<String> labels;

  protected final Map<String, Object> properties;

  protected LabeledPropertyContainer(Long id, Iterable<String> labels,
                                     Map<String, Object> properties) {
    if (id == null) {
      throw new IllegalArgumentException("id must not be null");
    }
    this.id = id;

    this.labels = (labels != null) ? Lists.newArrayList(labels) : new
      ArrayList<String>();
    this.properties = properties;
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
    this.labels.add(label);
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
  public String toString() {
    return "LabeledPropertyContainer{" +
      "id=" + id +
      ", labels=" + labels +
      ", properties=" + properties +
      '}';
  }
}
