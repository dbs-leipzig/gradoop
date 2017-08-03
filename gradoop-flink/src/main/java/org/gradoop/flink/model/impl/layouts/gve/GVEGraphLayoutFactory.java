package org.gradoop.flink.model.impl.layouts.gve;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.layouts.LogicalGraphLayoutFactory;
import org.gradoop.flink.model.impl.functions.graphcontainment.AddToGraph;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

/**
 * Responsible for creating a {@link GVELayout} from given data.
 */
public class GVEGraphLayoutFactory extends GVEBaseFactory implements LogicalGraphLayoutFactory {

  @Override
  public GVELayout fromDataSets(DataSet<Vertex> vertices) {
    return fromDataSets(vertices,
      createEdgeDataSet(Lists.newArrayListWithCapacity(0)));
  }

  @Override
  public GVELayout fromDataSets(DataSet<Vertex> vertices, DataSet<Edge> edges) {
    Objects.requireNonNull(vertices, "Vertex DataSet was null");
    Objects.requireNonNull(edges, "Edge DataSet was null");
    Objects.requireNonNull(config, "Config was null");
    GraphHead graphHead = config
      .getGraphHeadFactory()
      .createGraphHead();

    DataSet<GraphHead> graphHeadSet = config.getExecutionEnvironment()
      .fromElements(graphHead);

    // update vertices and edges with new graph head id
    vertices = vertices
      .map(new AddToGraph<>(graphHead))
      .withForwardedFields("id;label;properties");
    edges = edges
      .map(new AddToGraph<>(graphHead))
      .withForwardedFields("id;sourceId;targetId;label;properties");

    return new GVELayout(graphHeadSet, vertices, edges);
  }

  @Override
  public GVELayout fromDataSets(DataSet<GraphHead> graphHead, DataSet<Vertex> vertices,
    DataSet<Edge> edges) {
    return new GVELayout(graphHead, vertices, edges);
  }

  @Override
  public GVELayout fromCollections(GraphHead graphHead, Collection<Vertex> vertices,
    Collection<Edge> edges) {
    List<GraphHead> graphHeads;
    if (graphHead == null) {
      graphHeads = Lists.newArrayListWithCapacity(0);
    } else {
      graphHeads = Lists.newArrayList(graphHead);
    }

    if (edges == null) {
      edges = Lists.newArrayListWithCapacity(0);
    }

    Objects.requireNonNull(vertices, "Vertex collection was null");
    Objects.requireNonNull(edges, "Edge collection was null");
    Objects.requireNonNull(config, "Config was null");

    return fromDataSets(
      createGraphHeadDataSet(graphHeads),
      createVertexDataSet(vertices),
      createEdgeDataSet(edges));
  }

  @Override
  public GVELayout fromCollections(Collection<Vertex> vertices, Collection<Edge> edges) {
    Objects.requireNonNull(vertices, "Vertex collection was null");
    Objects.requireNonNull(edges, "Edge collection was null");
    Objects.requireNonNull(config, "Config was null");

    GraphHead graphHead = config.getGraphHeadFactory().createGraphHead();

    DataSet<Vertex> vertexDataSet = createVertexDataSet(vertices)
      .map(new AddToGraph<>(graphHead))
      .withForwardedFields("id;label;properties");

    DataSet<Edge> edgeDataSet = createEdgeDataSet(edges)
      .map(new AddToGraph<>(graphHead))
      .withForwardedFields("id;sourceId;targetId;label;properties");

    return fromDataSets(
      createGraphHeadDataSet(new ArrayList<>(0)), vertexDataSet, edgeDataSet);
  }

  @Override
  public GVELayout createEmptyGraph() {
    Collection<Vertex> vertices = new ArrayList<>(0);
    Collection<Edge> edges = new ArrayList<>(0);
    return fromCollections(null, vertices, edges);
  }
}
