package org.gradoop.flink.algorithms.fsm;

import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.id.GradoopId;

import java.util.Collection;

/**
 * Created by peet on 09.09.16.
 */
public class CodeEmbeddings 
  extends Tuple3<GradoopId, String, Collection<AdjacencyMatrix>> {
  
  public CodeEmbeddings() {
    super();
  }

  public CodeEmbeddings(GradoopId graphId, 
    String subgraph, Collection<AdjacencyMatrix> embeddings) {
    super(graphId, subgraph, embeddings);
  }

  public GradoopId getGraphId() {
    return f0;
  }

  public void setGraphId(GradoopId graphId) {
    f0 = graphId;
  }

  public String getSubgraph() {
    return f1;
  }

  public void setSubgraph(String subgraph) {
    f1 = subgraph;
  }

  public Collection<AdjacencyMatrix> getMatrices() {
    return f2;
  }

  public void setEmbeddings(Collection<AdjacencyMatrix> embeddings) {
    f2 = embeddings;
  }
}
