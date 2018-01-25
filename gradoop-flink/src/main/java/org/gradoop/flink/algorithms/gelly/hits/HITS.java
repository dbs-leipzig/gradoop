package org.gradoop.flink.algorithms.gelly.hits;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.NullValue;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.algorithms.gelly.GellyAlgorithm;
import org.gradoop.flink.algorithms.gelly.functions.EdgeToGellyEdgeWithNullValue;
import org.gradoop.flink.algorithms.gelly.functions.VertexToGellyVertexWithNullValue;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.Id;

/**
 * A gradoop operator wrapping {@link org.apache.flink.graph.library.link_analysis.HITS}
 * <p>
 * This algorithm can be configured to terminate either by a limit on the number of iterations, a
 * convergence threshold, or both.
 * <p>
 * The Results are stored as properties of the vertices (with given keys).
 */
public class HITS extends GellyAlgorithm<NullValue, NullValue> {

  private String authorityPropertyKey;
  private String hubPropertyKey;

  private org.apache.flink.graph.library.link_analysis.HITS<GradoopId, NullValue, NullValue> hits;

  /**
   * HITS with fixed number of iterations
   *
   * @param authorityPropertyKey Property key to store the authority score.
   * @param hubPropertyKey       Property key to store the hub score.
   * @param iterations           number of iterations
   */
  public HITS(String authorityPropertyKey, String hubPropertyKey, int iterations) {
    super(new VertexToGellyVertexWithNullValue(), new EdgeToGellyEdgeWithNullValue());
    this.authorityPropertyKey = authorityPropertyKey;
    this.hubPropertyKey = hubPropertyKey;
    hits = new org.apache.flink.graph.library.link_analysis.HITS<>(iterations, Double.MAX_VALUE);
  }


  /**
   * HITS with convergence threshold
   *
   * @param authorityPropertyKey Property key to store the authority score.
   * @param hubPropertyKey       Property key to store the hub score.
   * @param convergenceThreshold convergence threshold for sum of scores
   */
  public HITS(String authorityPropertyKey, String hubPropertyKey, double convergenceThreshold) {
    super(new VertexToGellyVertexWithNullValue(), new EdgeToGellyEdgeWithNullValue());
    this.authorityPropertyKey = authorityPropertyKey;
    this.hubPropertyKey = hubPropertyKey;
    hits = new org.apache.flink.graph.library.link_analysis.HITS<>(Integer.MAX_VALUE,
      convergenceThreshold);
  }

  /**
   * HITS with convergence threshold and maximum number of iterations
   *
   * @param authorityPropertyKey Property key to store the authority score.
   * @param hubPropertyKey       Property key to store the hub score.
   * @param convergenceThreshold convergence threshold for sum of scores
   * @param maxIterations        maximum number of iterations
   */
  public HITS(String authorityPropertyKey, String hubPropertyKey, int maxIterations,
    double convergenceThreshold) {
    super(new VertexToGellyVertexWithNullValue(), new EdgeToGellyEdgeWithNullValue());
    this.authorityPropertyKey = authorityPropertyKey;
    this.hubPropertyKey = hubPropertyKey;
    hits =
      new org.apache.flink.graph.library.link_analysis.HITS<>(maxIterations, convergenceThreshold);
  }


  @Override
  protected LogicalGraph executeInGelly(Graph<GradoopId, NullValue, NullValue> graph) throws
    Exception {

    DataSet<Vertex> newVertices =
      hits.run(graph).join(currentGraph.getVertices()).where(0).equalTo(new Id<>())
        .with(new HITSToAttributes(authorityPropertyKey, hubPropertyKey));

    return currentGraph.getConfig().getLogicalGraphFactory()
      .fromDataSets(newVertices, currentGraph.getEdges());
  }

  @Override
  public String getName() {
    return HITS.class.getName();
  }
}
