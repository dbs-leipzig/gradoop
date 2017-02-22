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

package org.gradoop.examples.dimspan.dimspan.tuples;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.examples.dimspan.dimspan.representation.PatternEmbeddingsMap;

/**
 * (graph, pattern->embeddings)
 */
public class GraphEmbeddingsPair extends Tuple2<int[], PatternEmbeddingsMap> {

  /**
   * Default constructor.
   */
  public GraphEmbeddingsPair() {
  }

  /**
   * Constructor.
   *  @param graph graph
   * @param patternEmbeddings pattern->embeddings
   */
  public GraphEmbeddingsPair(int[] graph, PatternEmbeddingsMap patternEmbeddings) {
    super(graph, patternEmbeddings);
  }

  /**
   * Convenience method to check if the element is the "collector"
   *
   * @return true, if collector
   */
  public boolean isCollector() {
    return f0.length <= 1;
  }

  // GETTERS AND SETTERS

  public int[] getGraph() {
    return f0;
  }

  public void setGraph(int[] graph) {
    this.f0 = graph;
  }

  public PatternEmbeddingsMap getPatternEmbeddings() {
    return f1;
  }

  public void setPatternEmbeddings(PatternEmbeddingsMap patternEmbeddings) {
    this.f1 = patternEmbeddings;
  }

}
