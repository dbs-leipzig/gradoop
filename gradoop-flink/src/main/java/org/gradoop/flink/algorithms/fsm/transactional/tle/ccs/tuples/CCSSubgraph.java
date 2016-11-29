/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.flink.algorithms.fsm.transactional.tle.ccs.tuples;

import org.apache.flink.api.java.tuple.Tuple5;
import org.gradoop.flink.algorithms.fsm.transactional.tle.common.pojos.Embedding;
import org.gradoop.flink.algorithms.fsm.transactional.tle.common.tuples.Subgraph;

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
