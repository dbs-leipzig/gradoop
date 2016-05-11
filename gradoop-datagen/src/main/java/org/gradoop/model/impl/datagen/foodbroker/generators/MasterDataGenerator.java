package org.gradoop.model.impl.datagen.foodbroker.generators;

import org.apache.flink.api.java.DataSet;
import org.gradoop.model.api.EPGMVertex;

public interface MasterDataGenerator<V extends EPGMVertex> {

  DataSet<V> generate();
}
