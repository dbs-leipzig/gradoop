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

package org.gradoop.flink.algorithms.fsm_old.tfsm.tuples;

import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.flink.algorithms.fsm_old.common.pojos.Embedding;
import org.gradoop.flink.algorithms.fsm_old.common.tuples.Subgraph;

/**
 * Representation of a subgraph.
 *
 * (canonicalLabel, frequency, sample embedding)
 */
public class TFSMSubgraph
  extends Tuple3<String, Long, Embedding> implements Subgraph {

  /**
   * Default constructor.
   */
  public TFSMSubgraph() {
    super();
  }

  /**
   * Constructor.
   *
   * @param subgraph canonical label
   * @param frequency frequency
   * @param embedding sample embedding
   */
  public TFSMSubgraph(String subgraph, Long frequency, Embedding embedding) {
    super(subgraph, frequency, embedding);
  }

  public String getCanonicalLabel() {
    return f0;
  }

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

  public Embedding getEmbedding() {
    return f2;
  }

  public void setEmbedding(Embedding embedding) {
    f2 = embedding;
  }
}
