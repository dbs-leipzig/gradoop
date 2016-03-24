package org.gradoop.model.impl.datagen.foodbroker.model;

import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.model.api.EPGMVertex;

import java.util.HashMap;
import java.util.Map;

public class TransactionalDataObject<V extends EPGMVertex> extends
  Tuple3<Long, Map<Short, MasterDataReference>, V> {

  public TransactionalDataObject() {

  }

  public TransactionalDataObject(V vertex) {
    this.f0 = 0L;
    this.f1 = new HashMap<>();
    this.f2 = vertex;
  }

  public void setJoinKey(long joinKey) {
    this.f0 = joinKey;
  }

  public void addReference(Short key, MasterDataReference value) {
    getReferences().put(key, value);
  }

  public Map<Short, MasterDataReference> getReferences() {
    return this.f1;
  }
}
