/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.flink.algorithms.gelly.hits;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.NullValue;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.algorithms.gelly.GellyAlgorithm;
import org.gradoop.flink.algorithms.gelly.functions.EdgeToGellyEdgeWithNullValue;
import org.gradoop.flink.algorithms.gelly.functions.VertexToGellyVertexWithNullValue;
import org.gradoop.flink.algorithms.gelly.hits.functions.HITSToAttributes;
import org.gradoop.flink.algorithms.gelly.hits.functions.HitsResultKeySelector;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.functions.epgm.Id;

/**
 * A gradoop operator wrapping {@link org.apache.flink.graph.library.linkanalysis.HITS}
 * <p>
 * This algorithm can be configured to terminate either by a limit on the number of iterations, a
 * convergence threshold, or both.
 * <p>
 * The Results are stored as properties of the vertices (with given keys).
 */
public class HITS extends GellyAlgorithm<NullValue, NullValue> {

  /**
   * Property key to store the authority score.
   */
  private String authorityPropertyKey;

  /**
   * Property key to store the hub score.
   */
  private String hubPropertyKey;

  /**
   * Gelly HITS implementation
   */
  private org.apache.flink.graph.library.linkanalysis.HITS<GradoopId, NullValue, NullValue> hits;

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
    hits = new org.apache.flink.graph.library.linkanalysis.HITS<>(iterations, Double.MAX_VALUE);
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
    hits = new org.apache.flink.graph.library.linkanalysis.HITS<>(Integer.MAX_VALUE,
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
      new org.apache.flink.graph.library.linkanalysis.HITS<>(maxIterations, convergenceThreshold);
  }


  @Override
  protected LogicalGraph executeInGelly(Graph<GradoopId, NullValue, NullValue> graph)
    throws Exception {

    DataSet<Vertex> newVertices = hits.runInternal(graph)
      .join(currentGraph.getVertices())
      .where(new HitsResultKeySelector()).equalTo(new Id<>())
      .with(new HITSToAttributes(authorityPropertyKey, hubPropertyKey));

    return currentGraph.getConfig().getLogicalGraphFactory()
      .fromDataSets(newVertices, currentGraph.getEdges());
  }

  @Override
  public String getName() {
    return HITS.class.getName();
  }

}
