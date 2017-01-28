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

package org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.unary;

import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.EmbeddingMetaData;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.filter.FilterEmbeddings;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.PlanNode;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.UnaryNode;

/**
 * Unary nodes that wraps a {@link FilterEmbeddings} operator.
 */
public class FilterEmbeddingsNode extends UnaryNode {
  /**
   * Filter predicate that is applied on the embedding
   */
  private CNF filterPredicate;
  /**
   * Creates a new node.
   *
   * @param childNode input plan node
   * @param filterPredicate filter predicate to be applied on embeddings
   */
  public FilterEmbeddingsNode(PlanNode childNode, CNF filterPredicate) {
    super(childNode);
    this.filterPredicate = filterPredicate;
  }

  @Override
  public DataSet<Embedding> execute() {
    return new FilterEmbeddings(getChildNode().execute(), filterPredicate, getEmbeddingMetaData())
      .evaluate();
  }

  /**
   * Returns a copy of the filter predicate attached to this node.
   *
   * @return filter predicate
   */
  public CNF getFilterPredicate() {
    return new CNF(filterPredicate);
  }

  @Override
  protected EmbeddingMetaData computeEmbeddingMetaData() {
    return new EmbeddingMetaData(getChildNode().getEmbeddingMetaData());
  }

  @Override
  public String toString() {
    return String.format("FilterEmbeddingsNode{filterPredicate=%s", filterPredicate);
  }
}
