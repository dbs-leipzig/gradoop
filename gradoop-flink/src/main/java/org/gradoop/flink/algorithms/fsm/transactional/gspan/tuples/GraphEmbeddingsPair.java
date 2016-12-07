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

package org.gradoop.flink.algorithms.fsm.transactional.gspan.tuples;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.tuples.IdWithLabel;
import org.gradoop.flink.representation.transactional.AdjacencyList;
import org.gradoop.flink.representation.transactional.traversalcode.TraversalCode;
import org.gradoop.flink.representation.transactional.traversalcode.TraversalEmbedding;

import java.util.Collection;
import java.util.Map;

public class GraphEmbeddingsPair extends
  Tuple2<AdjacencyList<GradoopId, String, IdWithLabel, IdWithLabel>, Map<TraversalCode<String>, Collection<TraversalEmbedding>>> {


  public GraphEmbeddingsPair(AdjacencyList<GradoopId, String, IdWithLabel, IdWithLabel> graph,
    Map<TraversalCode<String>, Collection<TraversalEmbedding>> patternEmbeddings) {
    super(graph, patternEmbeddings);
  }

  public GraphEmbeddingsPair() {
  }

  public AdjacencyList<GradoopId, String, IdWithLabel, IdWithLabel> getAdjacencyList() {
    return f0;
  }

  public Map<TraversalCode<String>, Collection<TraversalEmbedding>> getPatternEmbeddings() {
    return f1;
  }

  public void setPatternEmbeddings(
    Map<TraversalCode<String>, Collection<TraversalEmbedding>> patternEmbeddings) {
    this.f1 = patternEmbeddings;
  }
}
