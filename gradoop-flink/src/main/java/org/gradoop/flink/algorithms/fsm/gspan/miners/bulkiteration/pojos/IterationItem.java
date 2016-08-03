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

package org.gradoop.flink.algorithms.fsm.gspan.miners.bulkiteration.pojos;

import org.apache.commons.lang.StringUtils;
import org.gradoop.flink.algorithms.fsm.gspan.pojos.CompressedDFSCode;

import org.gradoop.flink.algorithms.fsm.gspan.pojos.GSpanGraph;
import org.gradoop.flink.model.impl.tuples.WithCount;

import java.io.Serializable;
import java.util.Collection;

/**
 * Wrapper for single graphs (transactions)
 * OR
 * a collection of frequent subgraphs (collector)
 */
public class IterationItem implements Serializable {

  /**
   * graph
   */
  private final GSpanGraph transaction;

  /**
   * collection of frequent subgraph
   */
  private final Collection<WithCount<CompressedDFSCode>> frequentSubgraphs;

  /**
   * graph constructor
   *
   * @param transaction graph
   */
  public IterationItem(GSpanGraph transaction) {
    this.transaction = transaction;
    this.frequentSubgraphs = null;
  }

  /**
   * collector constructor
   *
   * @param frequentSubgraphs collection of frequent subgraphs
   */
  public IterationItem(
    Collection<WithCount<CompressedDFSCode>> frequentSubgraphs) {

    this.transaction = null;
    this.frequentSubgraphs = frequentSubgraphs;
  }

  public boolean isTransaction() {
    return transaction != null;
  }

  public boolean isCollector() {
    return frequentSubgraphs != null;
  }

  public GSpanGraph getTransaction() {
    return this.transaction;
  }

  public Collection<WithCount<CompressedDFSCode>> getFrequentSubgraphs() {
    return this.frequentSubgraphs;
  }

  @Override
  public String toString() {
    return transaction != null ?
      transaction.toString() :
      StringUtils.join(frequentSubgraphs, "\n");
  }
}
