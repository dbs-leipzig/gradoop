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

package org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.unary;

import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.EmbeddingMetaData;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.EmbeddingMetaDataFactory;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.filter.FilterEmbeddings;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.PlanNode;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.UnaryNode;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.estimation.Estimator;

/**
 * Unary nodes that wraps a {@link FilterEmbeddings} operator.
 */
public class FilterEmbeddingsNode implements UnaryNode {
  /**
   * Input plan node
   */
  private PlanNode childNode;
  /**
   * Filter predicate that is applied on the embedding
   */
  private CNF filterPredicate;
  /**
   * Meta data describing the output of that node
   */
  private EmbeddingMetaData embeddingMetaData;

  /**
   * Creates a new node.
   *
   * @param childNode input plan node
   * @param filterPredicate filter predicate to be applied on embeddings
   */
  public FilterEmbeddingsNode(PlanNode childNode, CNF filterPredicate) {
    this.childNode = childNode;
    this.filterPredicate = filterPredicate;
    this.embeddingMetaData = EmbeddingMetaDataFactory
      .forFilterEmbeddings(childNode.getEmbeddingMetaData());
  }

  @Override
  public PlanNode getChild() {
    return childNode;
  }

  @Override
  public DataSet<Embedding> execute() {
    return new FilterEmbeddings(childNode.execute(), filterPredicate, embeddingMetaData).evaluate();
  }

  @Override
  public Estimator getEstimator() {
    return null;
  }

  @Override
  public EmbeddingMetaData getEmbeddingMetaData() {
    return embeddingMetaData;
  }
}
