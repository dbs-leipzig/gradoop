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

package org.gradoop.flink.algorithms.fsm.tuples;

import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.flink.algorithms.fsm.pojos.Embedding;
import org.gradoop.flink.model.api.tuples.Countable;

/**
 * Representation of a subgraph.
 *
 * (canonicalLabel, frequency, sample embedding)
 */
public class Subgraph
  extends Tuple3<String, Long, Embedding> implements Countable {

  /**
   * Default constructor.
   */
  public Subgraph() {
  }

  /**
   * Constructor.
   *
   * @param subgraph canonical label
   * @param frequency frequency
   * @param embedding sample embedding
   */
  public Subgraph(String subgraph, Long frequency, Embedding embedding) {
    super(subgraph, frequency, embedding);
  }

  public String getSubgraph() {
    return f0;
  }

  public void setSubgraph(String subgraph) {
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
