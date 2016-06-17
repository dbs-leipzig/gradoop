package org.gradoop.io.impl.tsv;

import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.util.GradoopFlinkConfig;

abstract class TSVBase <G extends EPGMGraphHead, V extends EPGMVertex, E
  extends EPGMEdge> {
  /**
   * Gradoop Flink configuration
   */
  private final GradoopFlinkConfig<G, V, E> config;
  /**
   * Path to tsv file
   */
  private final String tsvPath;


  TSVBase(String tsvPath, GradoopFlinkConfig<G,V,E> config){
    if (config == null) {
      throw new IllegalArgumentException("config must not be null");
    }
    if (tsvPath == null) {
      throw new IllegalArgumentException("vertex file must not be null");
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
