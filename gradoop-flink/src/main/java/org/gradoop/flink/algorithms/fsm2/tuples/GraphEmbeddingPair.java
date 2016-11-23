package org.gradoop.flink.algorithms.fsm2.tuples;

import com.google.common.collect.Maps;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.flink.representation.transactional.adjacencylist.AdjacencyList;
import org.gradoop.flink.representation.transactional.traversalcode.TraversalCode;
import org.gradoop.flink.representation.transactional.traversalcode.TraversalEmbedding;

import java.util.Collection;
import java.util.Map;

public class GraphEmbeddingPair extends
  Tuple2<AdjacencyList<LabelPair>, Map<TraversalCode<String>, Collection<TraversalEmbedding>>> {


  public GraphEmbeddingPair(AdjacencyList<LabelPair> adjacencyLists,
    Map<TraversalCode<String>, Collection<TraversalEmbedding>> codeEmbeddings) {
    super(adjacencyLists, codeEmbeddings);
  }

  public GraphEmbeddingPair() {
    super(new AdjacencyList<>(), Maps.newHashMap());
  }

  public AdjacencyList<LabelPair> getAdjacencyList() {
    return f0;
  }

  public Map<TraversalCode<String>, Collection<TraversalEmbedding>> getCodeEmbeddings() {
    return f1;
  }

  public void setCodeEmbeddings(
    Map<TraversalCode<String>, Collection<TraversalEmbedding>> codeEmbeddings) {
    this.f1 = codeEmbeddings;
  }
}
