package org.gradoop.model.impl.datagen.foodbroker.model;

import org.apache.flink.api.java.tuple.Tuple7;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.properties.PropertyList;

import java.util.HashMap;
import java.util.Map;

public class TransactionalDataObject extends Tuple7<GradoopId, String,
  PropertyList, Map<String, GradoopId>, Long, Long, Map<String, Short>> {

  public TransactionalDataObject() {
    setId(GradoopId.get());
    setReferences(new HashMap<String, GradoopId>());
    setInfluences(new HashMap<String, Short>());
    setProperties(new PropertyList());
    setForeignKey(0L);
  }

  public Map<String, GradoopId> getReferences() {
    return this.f3;
  }

  public void setReferences(HashMap<String, GradoopId> references) {
    this.f3 = references;
  }

  public PropertyList getProperties() {
    return this.f2;
  }

  public void setForeignKey(Long foreignKey) {
    this.f4 = foreignKey;
  }

  public void setCaseId(Long caseId) {
    this.f5 = caseId;
  }

  public Long getForeignKey() {
    return this.f4;
  }

  public Long getCaseId() {
    return this.f5;
  }

  public GradoopId getId() {
    return this.f0;
  }

  public void setInfluences(HashMap<String,Short> influences) {
    this.f6 = influences;
  }

  public Map<String, Short> getQualities() {
    return this.f6;
  }

  public void setProperties(PropertyList properties) {
    this.f2 = properties;
  }

  public void setId(GradoopId id) {
    this.f0 = id;
  }

  public String getLabel() {
    return this.f1;
  }

  public void setLabel(String label) {
    this.f1 = label;
  }
}
