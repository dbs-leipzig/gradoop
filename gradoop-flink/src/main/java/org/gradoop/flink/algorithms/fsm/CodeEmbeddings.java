package org.gradoop.flink.algorithms.fsm;

import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.algorithms.fsm.gspan.pojos.CompressedDFSCode;
import org.gradoop.flink.algorithms.fsm.gspan.pojos.DFSEmbedding;

import java.util.Collection;

/**
 * Created by peet on 09.09.16.
 */
public class CodeEmbeddings 
  extends Tuple3<GradoopId, CompressedDFSCode, Collection<DFSEmbedding>> {
  
  public CodeEmbeddings() {
    super();
  }

  public CodeEmbeddings(GradoopId graphId, 
    CompressedDFSCode subgraph, Collection<DFSEmbedding> embeddings) {
    super(graphId, subgraph, embeddings);
  }

  public GradoopId getGraphId() {
    return f0;
  }

  public void setGraphId(GradoopId graphId) {
    f0 = graphId;
  }

  public CompressedDFSCode getSubgraph() {
    return f1;
  }

  public void setSubgraph(CompressedDFSCode subgraph) {
    f1 = subgraph;
  }

  public Collection<DFSEmbedding> getEmbeddings() {
    return f2;
  }

  public void setEmbeddings(Collection<DFSEmbedding> embeddings) {
    f2 = embeddings;
  }
}
