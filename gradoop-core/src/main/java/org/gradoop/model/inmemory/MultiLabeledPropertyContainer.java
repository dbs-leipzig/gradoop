package org.gradoop.model.inmemory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.gradoop.model.Attributed;
import org.gradoop.model.Identifiable;
import org.gradoop.model.MultiLabeled;

import java.util.List;
import java.util.Map;

/**
 * Created by martin on 05.11.14.
 */
public abstract class MultiLabeledPropertyContainer implements Identifiable,
  Attributed, MultiLabeled {

  protected final Long id;

  protected List<String> labels;

  protected Map<String, Object> properties;

  protected MultiLabeledPropertyContainer(Long id, Iterable<String> labels,
                                          Map<String, Object> properties) {
    if (id == null) {
      throw new IllegalArgumentException("id must not be null");
    }
    this.id = id;

    this.labels = (labels != null) ? Lists.newArrayList(labels) : null;
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
    if (this.labels == null) {
      this.labels = Lists.newArrayList();
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
  public String toString() {
    return "LabeledPropertyContainer{" +
      "id=" + id +
      ", labels=" + labels +
      ", properties=" + properties +
      '}';
  }
}
