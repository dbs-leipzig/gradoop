
package org.gradoop.flink.algorithms.fsm.transactional.tle.tuples;

import org.gradoop.flink.algorithms.fsm.transactional.tle.pojos.Embedding;
import org.gradoop.flink.model.api.tuples.Countable;

import java.io.Serializable;

/**
 * Describes a subgraph in the context of transactional FSM,
 * i.e., a canonical label and a sample embedding
 */
public interface Subgraph extends Countable, Serializable {

  /**
   * Returns canonical label of a subgraph.
   *
   * @return canonical label
   */
  String getCanonicalLabel();

  /**
   * Sets the canonical label of a subgraph.
   *
   * @param label canonical label
   */
  void setCanonicalLabel(String label);

  /**
   * Returns a sample embedding.
   *
   * @return sample embedding
   */
  Embedding getEmbedding();

  /**
   * Sets the sample embedding.
   *
   * @param embedding sample embedding
   */
  void setEmbedding(Embedding embedding);

}
