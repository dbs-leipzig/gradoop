package org.gradoop.flink.model.api.epgm;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.layouts.LogicalGraphLayoutFactory;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.util.Collection;
import java.util.Objects;

public class LogicalGraphFactory {

  private LogicalGraphLayoutFactory layoutFactory;

  private final GradoopFlinkConfig config;

  public LogicalGraphFactory(LogicalGraphLayoutFactory layoutFactory, GradoopFlinkConfig config) {
    this.layoutFactory = layoutFactory;
    this.config = config;
  }

  public void setLayoutFactory(LogicalGraphLayoutFactory layoutFactory) {
    Objects.requireNonNull(layoutFactory);
    this.layoutFactory = layoutFactory;
  }

  public LogicalGraph fromDataSets(DataSet<Vertex> vertices) {
    return new LogicalGraph(layoutFactory.fromDataSets(vertices, config), config);
  }

  public LogicalGraph fromDataSets(DataSet<Vertex> vertices, DataSet<Edge> edges) {
    return new LogicalGraph(layoutFactory.fromDataSets(vertices, edges, config), config);
  }

  public LogicalGraph fromDataSets(DataSet<GraphHead> graphHead, DataSet<Vertex> vertices,
    DataSet<Edge> edges) {
    return new LogicalGraph(layoutFactory.fromDataSets(graphHead, vertices, edges, config), config);
  }

  public LogicalGraph fromCollections(GraphHead graphHead, Collection<Vertex> vertices,
    Collection<Edge> edges) {
    return new LogicalGraph(layoutFactory.fromCollections(graphHead, vertices, edges, config), config);
  }

  public LogicalGraph fromCollections(Collection<Vertex> vertices, Collection<Edge> edges) {
    return new LogicalGraph(layoutFactory.fromCollections(vertices, edges, config), config);
  }

  public LogicalGraph createEmptyGraph() {
    return new LogicalGraph(layoutFactory.createEmptyGraph(config), config);
  }
}
