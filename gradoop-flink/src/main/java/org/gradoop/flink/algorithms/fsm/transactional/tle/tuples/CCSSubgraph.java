
package org.gradoop.flink.algorithms.fsm.transactional.tle.tuples;

import org.apache.flink.api.java.tuple.Tuple5;
import org.gradoop.flink.algorithms.fsm.transactional.tle.pojos.Embedding;

/**
 * Representation of a subgraph.
 *
 * (canonicalLabel, frequency, sample embedding)
 */
public class CCSSubgraph
  extends Tuple5<String, Long, Embedding, String, Boolean> implements Subgraph {

  /**
   * Default constructor.
   */
  public CCSSubgraph() {
    super();
  }

  /**
   * Constructor.
   * @param category category
   * @param subgraph canonical label
   * @param frequency frequency
   * @param embedding sample embedding
   * @param interesting true, if interesting
   */
  public CCSSubgraph(String category, String subgraph, Long frequency,
    Embedding embedding, Boolean interesting) {
    super(subgraph, frequency, embedding, category, interesting);
  }

  @Override
  public String getCanonicalLabel() {
    return f0;
  }

  @Override
  public void setCanonicalLabel(String subgraph) {
    f0 = subgraph;
  }

  @Override
  public long getCount() {
    return f1;
  }

  @Override
  public void setCount(long frequency) {
    f1 = frequency;
  }

  @Override
  public Embedding getEmbedding() {
    return f2;
  }

  @Override
  public void setEmbedding(Embedding embedding) {
    f2 = embedding;
  }

  public String getCategory() {
    return f3;
  }

  public void setCategory(String category) {
    this.f3 = category;
  }

  public boolean isInteresting() {
    return f4;
  }

  public void setInteresting(boolean interesting) {
    this.f4 = interesting;
  }
}
