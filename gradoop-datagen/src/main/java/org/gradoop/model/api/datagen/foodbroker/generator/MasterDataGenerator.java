package org.gradoop.model.api.datagen.foodbroker.generator;

import org.apache.flink.api.java.DataSet;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.datagen.foodbroker.model.MasterDataObject;

public interface MasterDataGenerator<V extends EPGMVertex> {

  DataSet<MasterDataObject<V>> generate();
}
