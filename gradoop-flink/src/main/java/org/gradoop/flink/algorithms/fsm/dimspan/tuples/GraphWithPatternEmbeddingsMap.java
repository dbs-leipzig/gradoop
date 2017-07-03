
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
