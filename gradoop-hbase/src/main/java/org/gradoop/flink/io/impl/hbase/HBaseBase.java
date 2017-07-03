
package org.gradoop.flink.io.impl.hbase;

import org.gradoop.common.config.GradoopHBaseConfig;
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.common.storage.impl.hbase.HBaseEPGMStore;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * Base class for HBase data source and sink.
 *
 * @param <G> EPGM graph head type
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
abstract class HBaseBase
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge> {
  /**
   * HBase Store implementation
   */
  private final HBaseEPGMStore<G, V, E> epgmStore;
  /**
   * Gradoop Flink configuration
   */
  private final GradoopFlinkConfig config;

  /**
   * Creates a new HBase data source/sink.
   *
   * @param epgmStore store implementation
   * @param config    Gradoop Flink configuration
   */
  HBaseBase(HBaseEPGMStore<G, V, E> epgmStore, GradoopFlinkConfig config) {
    this.epgmStore  = epgmStore;
    this.config     = config;
  }

  HBaseEPGMStore<G, V, E> getStore() {
    return epgmStore;
  }

  GradoopFlinkConfig getFlinkConfig() {
    return config;
  }

  GradoopHBaseConfig<G, V, E> getHBaseConfig() {
    return epgmStore.getConfig();
  }
}
