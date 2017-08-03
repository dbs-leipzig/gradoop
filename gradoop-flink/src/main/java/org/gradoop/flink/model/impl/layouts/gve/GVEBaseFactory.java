package org.gradoop.flink.model.impl.layouts.gve;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.layouts.BaseLayoutFactory;
import org.gradoop.flink.model.impl.functions.bool.False;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.util.Collection;
import java.util.Objects;

/**
 * Base class for GVE layout factories.
 */
abstract class GVEBaseFactory implements BaseLayoutFactory {

  /**
   * Gradoop Flink config
   */
  protected GradoopFlinkConfig config;

  @Override
  public void setGradoopFlinkConfig(GradoopFlinkConfig config) {
    Objects.requireNonNull(config);
    this.config = config;
  }

  /**
   * Creates a graph head dataset from a given collection.
   * Encapsulates the workaround for dataset creation from an empty collection.
   *
   * @param graphHeads  graph heads
   * @return graph head dataset
   */
  DataSet<GraphHead> createGraphHeadDataSet(Collection<GraphHead> graphHeads) {

    ExecutionEnvironment env = config.getExecutionEnvironment();

    DataSet<GraphHead> graphHeadSet;
    if (graphHeads.isEmpty()) {
      graphHeadSet = config.getExecutionEnvironment()
        .fromElements(config.getGraphHeadFactory().createGraphHead())
        .filter(new False<GraphHead>());
    } else {
      graphHeadSet =  env.fromCollection(graphHeads);
    }
    return graphHeadSet;
  }

  /**
   * Creates a vertex dataset from a given collection.
   * Encapsulates the workaround for dataset creation from an empty collection.
   *
   * @param vertices  vertex collection
   * @return vertex dataset
   */
  DataSet<Vertex> createVertexDataSet(Collection<Vertex> vertices) {

    ExecutionEnvironment env = config.getExecutionEnvironment();

    DataSet<Vertex> vertexSet;
    if (vertices.isEmpty()) {
      vertexSet = config.getExecutionEnvironment()
        .fromElements(config.getVertexFactory().createVertex())
        .filter(new False<Vertex>());
    } else {
      vertexSet = env.fromCollection(vertices);
    }
    return vertexSet;
  }

  /**
   * Creates an edge dataset from a given collection.
   * Encapsulates the workaround for dataset creation from an empty collection.
   *
   * @param edges edge collection
   * @return edge dataset
   */
  DataSet<Edge> createEdgeDataSet(Collection<Edge> edges) {

    DataSet<Edge> edgeSet;
    if (edges.isEmpty()) {
      GradoopId dummyId = GradoopId.get();
      edgeSet = config.getExecutionEnvironment()
        .fromElements(config.getEdgeFactory().createEdge(dummyId, dummyId))
        .filter(new False<Edge>());
    } else {
      edgeSet = config.getExecutionEnvironment().fromCollection(edges);
    }
    return edgeSet;
  }
}
