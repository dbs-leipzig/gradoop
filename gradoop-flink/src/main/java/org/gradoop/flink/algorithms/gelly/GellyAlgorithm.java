package org.gradoop.flink.algorithms.gelly;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.algorithms.gelly.functions.EdgeToGellyEdge;
import org.gradoop.flink.algorithms.gelly.functions.VertexToGellyVertex;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.api.operators.UnaryGraphToGraphOperator;

/**
 * Base class for Algorithms executed in Flink Gelly.
 *
 * @param <E> Value type for vertices.
 * @param <F> Value type for edges.
 */
public abstract class GellyAlgorithm<E, F> implements UnaryGraphToGraphOperator {

  /**
   * Function mapping to edge to gelly edge.
   */
  private final EdgeToGellyEdge<F> toGellyEdge;

  /**
   * Function mapping vertex to gelly vertex.
   */
  private final VertexToGellyVertex<E> toGellyVertex;

  /**
   * The graph used in {@link GellyAlgorithm#execute(LogicalGraph)}.
   */
  protected LogicalGraph currentGraph;

  /**
   * Base constructor, only setting the mapper functions.
   *
   * @param vertexValue Function mapping vertices from Gradoop to Gelly.
   * @param edgeValue   function mapping edges from Gradoop to Gelly.
   */
  protected GellyAlgorithm(VertexToGellyVertex<E> vertexValue, EdgeToGellyEdge<F> edgeValue) {
    this.toGellyVertex = vertexValue;
    this.toGellyEdge = edgeValue;
  }

  @Override
  public LogicalGraph execute(LogicalGraph graph) {
    currentGraph = graph;
    return executeInGelly(transformToGelly(graph));
  }

  /**
   * Perform some operation in Gelly and transform the Gelly graph back to a Gradoop {@link LogicalGraph}.
   *
   * @param graph The Gelly graph.
   * @return The Gradoop graph.
   */
  protected abstract LogicalGraph executeInGelly(Graph<GradoopId, E, F> graph);

  /**
   * Default transformation from a Gradoop Graph to a Gelly Graph.
   *
   * @param graph Gradoop Graph.
   * @return Gelly Graph.
   */
  protected Graph<GradoopId, E, F> transformToGelly(LogicalGraph graph) {
    DataSet<Vertex<GradoopId, E>> gellyVertices = graph.getVertices().map(toGellyVertex);
    DataSet<Edge<GradoopId, F>> gellyEdges = graph.getEdges().map(toGellyEdge);
    return Graph.fromDataSet(gellyVertices, gellyEdges, graph.getConfig().getExecutionEnvironment());
  }

}
