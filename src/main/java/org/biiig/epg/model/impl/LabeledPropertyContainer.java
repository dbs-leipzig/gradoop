package org.biiig.epg.model.impl;

import org.biiig.epg.model.Attributed;
import org.biiig.epg.model.Identifiable;
import org.biiig.epg.model.Labeled;

import java.util.Map;

/**
 * Created by martin on 05.11.14.
 */
public abstract class LabeledPropertyContainer implements Identifiable, Attributed, Labeled {

  protected final Long id;

  protected final Iterable<String> labels;

  protected final Map<String, Object> properties;

  protected LabeledPropertyContainer(Long id, Iterable<String> labels,
      Map<String, Object> properties) {
    this.id = id;
    this.labels = labels;
    this.properties = properties;
  }

  @Override public Long getID() {
    return id;
  }

  @Override public Iterable<String> getLabels() {
    return labels;
  }

  @Override public Iterable<String> getPropertyKeys() {
    return properties.keySet();
  }

  @Override public Object getProperty(String key) {
    return properties.get(key);
  }

  @Override public String toString() {
    return "LabeledPropertyContainer{" +
        "id=" + id +
        ", labels=" + labels +
        ", properties=" + properties +
        '}';
  }
}
