
package org.gradoop.flink.algorithms.fsm.transactional.tle.tuples;

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.algorithms.fsm.transactional.tle.pojos.Embedding;

import java.util.List;

/**
 * Describes a subgraph and its embeddings in a certain graph.
 */
public interface SubgraphEmbeddings {

  /**
   * Getter.
   *
   * @return graph id
   */
  GradoopId getGraphId();

  /**
   * Setter.
   *
   * @param graphId graph id
   */
  void setGraphId(GradoopId graphId);

  /**
   * Getter.
   *
   * @return edge count
   */
  Integer getSize();

  /**
   * Setter.
   *
   * @param size edge count
   */
  void setSize(Integer size);

  /**
   * Getter.
   *
   * @return canonical label
   */
  String getCanonicalLabel();

  /**
   * Setter.
   *
   * @param label canonical label
   */
  void setCanonicalLabel(String label);

  /**
   * Getter.
   *
   * @return embeddings
   */
  List<Embedding> getEmbeddings();

  /**
   * Setter.
   *
   * @param embeddings embeddings
   */
  void setEmbeddings(List<Embedding> embeddings);
}
