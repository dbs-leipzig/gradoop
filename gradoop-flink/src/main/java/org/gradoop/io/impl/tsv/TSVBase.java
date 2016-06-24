package org.gradoop.io.impl.tsv;

import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.util.GradoopFlinkConfig;

/**
 * Base class for file based I/O formats.
 *
 * @param <G> EPGM graph head type
 * @param <V> EPGM vertex type
 * @param <E> EPGM edge type
 */
abstract class TSVBase
  <G extends EPGMGraphHead, V extends EPGMVertex, E extends EPGMEdge> {
  /**
   * Gradoop Flink configuration
   */
  private final GradoopFlinkConfig<G, V, E> config;
  /**
   * Path to tsv file
   */
  private final String tsvPath;


  /**
   * Creates a new data source/sink. Paths can be local (file://) or HDFS
   * (hdfs://).
   *
   * @param tsvPath       path to tsv file
   * @param config        Gradoop Flink configuration
   */
  TSVBase(String tsvPath, GradoopFlinkConfig<G,V,E> config){
    if (config == null) {
      throw new IllegalArgumentException("config must not be null");
    }
    if (tsvPath == null) {
      throw new IllegalArgumentException("tsv path must not be null");
    }
    this.config = config;
    this.tsvPath = tsvPath;
  }

  public GradoopFlinkConfig<G, V, E> getConfig() {
    return config;
  }

  public String getTsvPath() {
    return tsvPath;
  }

}
