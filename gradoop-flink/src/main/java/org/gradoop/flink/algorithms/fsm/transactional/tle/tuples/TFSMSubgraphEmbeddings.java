
package org.gradoop.flink.algorithms.fsm.transactional.tle.tuples;

import org.apache.flink.api.java.tuple.Tuple4;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.algorithms.fsm.transactional.tle.pojos.Embedding;

import java.util.List;

/**
 * Representation of a subgraph supported by a graph and all its local
 * embeddings.
 *
 * (graphId, size, canonicalLabel, embeddings)
 */
public class TFSMSubgraphEmbeddings
  extends Tuple4<GradoopId, Integer, String, List<Embedding>>
  implements SubgraphEmbeddings {

  /**
   * Default constructor
   */
  public TFSMSubgraphEmbeddings() {
    super();
  }

  @Override
  public GradoopId getGraphId() {
    return f0;
  }

  @Override
  public void setGraphId(GradoopId graphId) {
    f0 = graphId;
  }

  @Override
  public Integer getSize() {
    return f1;
  }

  @Override
  public void setSize(Integer size) {
    f1 = size;
  }

  @Override
  public String getCanonicalLabel() {
    return f2;
  }

  @Override
  public void setCanonicalLabel(String label) {
    f2 = label;
  }

  @Override
  public List<Embedding> getEmbeddings() {
    return f3;
  }

  @Override
  public void setEmbeddings(List<Embedding> embeddings) {
    f3 = embeddings;
  }
}
