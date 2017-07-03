package org.gradoop.flink.datagen.transactions.foodbroker.generators;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.api.entities.EPGMVertex;

/**
 * Interface for generators of master data objects.
 * @param <V> EPGM vertex type
 */
public interface MasterDataGenerator<V extends EPGMVertex> {

  /**
   * Generates the master data vertices.
   *
   * @return data set of vertices
   */
  DataSet<V> generate();
}
