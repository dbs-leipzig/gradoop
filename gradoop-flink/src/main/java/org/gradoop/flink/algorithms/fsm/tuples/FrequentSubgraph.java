package org.gradoop.flink.algorithms.fsm.tuples;

import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.flink.algorithms.fsm.pojos.Embedding;

public class FrequentSubgraph extends Tuple3<String, Long, Embedding> {

  public FrequentSubgraph() {

  }

  public FrequentSubgraph(String subgraph, Long frequency, Embedding embedding) {
    super(subgraph, frequency, embedding);
  }

  public String getSubgraph() {
    return f0;
  }

  public void setSubgraph(String subgraph) {
    f0 = subgraph;
  }

  public Long getFrequency() {
    return f1;
  }

  public void setFrequency(Long frequency) {
    f1 = frequency;
  }

  public Embedding getEmbedding() {
    return f2;
  }

  public void setEmbedding(Embedding embedding) {
    f2 = embedding;
  }
}
