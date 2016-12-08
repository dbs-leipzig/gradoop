/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.flink.algorithms.fsm.transactional.gspan.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.algorithms.fsm.transactional.gspan.algorithm.GSpanKernel;
import org.gradoop.flink.algorithms.fsm.transactional.gspan.tuples.GraphEmbeddingsPair;
import org.gradoop.flink.model.impl.tuples.IdWithLabel;
import org.gradoop.flink.representation.transactional.AdjacencyList;
import org.gradoop.flink.representation.transactional.traversalcode.TraversalCode;
import org.gradoop.flink.representation.transactional.traversalcode.TraversalEmbedding;

import java.util.Collection;
import java.util.Map;

/**
 * graph => (graph, 1-edge pattern -> embeddings)
 */
public class InitSingleEdgeEmbeddings implements
  MapFunction<AdjacencyList<GradoopId, String, IdWithLabel, IdWithLabel>, GraphEmbeddingsPair> {

  /**
   * pattern generation logic
   */
  private final GSpanKernel gSpan;

  /**
   * Constructor.
   *
   * @param gSpan pattern generation logic
   */
  public InitSingleEdgeEmbeddings(GSpanKernel gSpan) {
    this.gSpan = gSpan;
  }

  @Override
  public GraphEmbeddingsPair map(
    AdjacencyList<GradoopId, String, IdWithLabel, IdWithLabel> graph) throws Exception {

    Map<TraversalCode<String>, Collection<TraversalEmbedding>> codeEmbeddings =
      gSpan.getSingleEdgePatternEmbeddings(graph);

    return new GraphEmbeddingsPair(graph, codeEmbeddings);
  }

}
