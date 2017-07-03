
package org.gradoop.flink.algorithms.fsm.transactional.tle.tuples;

import org.apache.flink.api.java.tuple.Tuple5;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.algorithms.fsm.transactional.tle.pojos.Embedding;

import java.util.List;

/**
 * Representation of a subgraph supported by a graph and all its local
 * embeddings.
 *
 * (category, graphId, size, canonicalLabel, embeddings)
 */
public class CCSSubgraphEmbeddings
  extends Tuple5<GradoopId, Integer, String, List<Embedding>, String>
  implements SubgraphEmbeddings {

  /**
   * Default constructor
   */
  public CCSSubgraphEmbeddings() {
  }


  public String getCategory() {
    return f4;
  }

  public void setCategory(String category) {
    f4 = category;
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


  public String getCanonicalLabel() {
    return f2;
  }

  public void setCanonicalLabel(String label) {
    f2 = label;
  }

  public List<Embedding> getEmbeddings() {
    return f3;
  }

  public void setEmbeddings(List<Embedding> embeddings) {
    f3 = embeddings;
  }
}
