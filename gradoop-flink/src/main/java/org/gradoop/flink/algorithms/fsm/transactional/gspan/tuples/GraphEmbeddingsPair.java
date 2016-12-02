package org.gradoop.flink.algorithms.fsm.transactional.gspan.tuples;

import com.google.common.collect.Maps;
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


  public GraphEmbeddingsPair(AdjacencyList<GradoopId, String, IdWithLabel, IdWithLabel> adjacencyLists,
    Map<TraversalCode<String>, Collection<TraversalEmbedding>> codeEmbeddings) {
    super(adjacencyLists, codeEmbeddings);
  }

  public GraphEmbeddingsPair() {
    super(new AdjacencyList<>(), Maps.newHashMap());
  }

  public AdjacencyList<GradoopId, String, IdWithLabel, IdWithLabel> getAdjacencyList() {
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
