package org.gradoop.util;

import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMGraphHead;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.LogicalGraph;

public class FlinkAsciiGraphLoader<
  V extends EPGMVertex,
  E extends EPGMEdge,
  G extends EPGMGraphHead> {

  private final GradoopFlinkConfig<V, E, G> config;

  private AsciiGraphLoader loader;

  public FlinkAsciiGraphLoader(GradoopFlinkConfig<V, E, G> config) {
    this.config = config;
  }

  public void readDatabaseFromString(String asciiGraphs) {
    loader = AsciiGraphLoader.fromString(asciiGraphs, config);

  }

  public LogicalGraph<V, E, G>
  getLogicalGraphByVariable(String variable) {
    return null;
  }

  public GraphCollection<V, E, G>
  getGraphCollectionByVariables(String... variables) {
    return null;
  }
}
