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
package org.gradoop.flink.algorithms.fsm.dimspan.tuples;

import org.apache.flink.api.java.tuple.Tuple2;

/**
 * (graph, pattern->embeddings)
 */
public class GraphWithPatternEmbeddingsMap extends Tuple2<int[], PatternEmbeddingsMap> {

  /**
   * Default constructor.
   */
  public GraphWithPatternEmbeddingsMap() {
  }

  /**
   * Constructor.
   *  @param graph graph
   * @param patternEmbeddings pattern->embeddings
   */
  public GraphWithPatternEmbeddingsMap(int[] graph, PatternEmbeddingsMap patternEmbeddings) {
    super(graph, patternEmbeddings);
  }

  /**
   * Convenience method to check if the element is the "collector"
   *
   * @return true, if collector
   */
  public boolean isFrequentPatternCollector() {
    return f0.length <= 1;
  }

  // GETTERS AND SETTERS

  public int[] getGraph() {
    return f0;
  }

  public void setGraph(int[] graph) {
    this.f0 = graph;
  }

  public PatternEmbeddingsMap getMap() {
    return f1;
  }

  public void setPatternEmbeddings(PatternEmbeddingsMap patternEmbeddings) {
    this.f1 = patternEmbeddings;
  }

}
