package org.gradoop.flink.algorithms.fsm;

import org.apache.flink.api.java.tuple.Tuple4;
import org.gradoop.common.model.impl.id.GradoopId;

import java.util.Collection;

public class SubgraphEmbeddings
  extends Tuple4<GradoopId, Integer, String, Collection<Embedding>> {
  
  public SubgraphEmbeddings() {
    super();
  }

  public SubgraphEmbeddings(GradoopId graphId, Integer size, String subgraph,
    Collection<Embedding> embeddings) {
    super(graphId, size, subgraph, embeddings);
  }

  public GradoopId getGraphId() {
    return f0;
  }

  public void setGraphId(GradoopId graphId) {
    f0 = graphId;
  }

  public Integer getSize() {
    return f1;
  }

  public void setSize(Integer size) {
    f1 = size;
  }


  public String getSubgraph() {
    return f2;
  }

  public void setSubgraph(String subgraph) {
    f2 = subgraph;
  }

  public Collection<Embedding> getEmbeddings() {
    return f3;
  }

  public void setEmbeddings(Collection<Embedding> embeddings) {
    f3 = embeddings;
  }
}
