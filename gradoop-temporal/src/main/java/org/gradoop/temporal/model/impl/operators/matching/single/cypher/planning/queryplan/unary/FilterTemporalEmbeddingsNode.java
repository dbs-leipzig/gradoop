/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.temporal.model.impl.operators.matching.single.cypher.planning.queryplan.unary;

import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.FilterNode;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.TemporalCNF;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.filter.FilterTemporalEmbeddings;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.planning.queryplan.PlanNode;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.planning.queryplan.UnaryNode;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGM;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGMMetaData;

/**
 * Unary nodes that wraps a {@link FilterTemporalEmbeddings} operator.
 */
public class FilterTemporalEmbeddingsNode extends UnaryNode implements FilterNode {
  /**
   * Filter predicate that is applied on the embedding
   */
  private final TemporalCNF filterPredicate;

  /**
   * Creates a new node.
   *
   * @param childNode       input plan node
   * @param filterPredicate filter predicate to be applied on embeddings
   */
  public FilterTemporalEmbeddingsNode(PlanNode childNode, TemporalCNF filterPredicate) {
    super(childNode);
    this.filterPredicate = filterPredicate;
  }

  @Override
  public DataSet<EmbeddingTPGM> execute() {
    FilterTemporalEmbeddings op =
      new FilterTemporalEmbeddings(getChildNode().execute(), filterPredicate, getEmbeddingMetaData());
    op.setName(toString());

    return op.evaluate();
  }

  /**
   * Returns a copy of the filter predicate attached to this node.
   *
   * @return filter predicate
   */
  public TemporalCNF getFilterPredicate() {
    return new TemporalCNF(filterPredicate);
  }

  @Override
  protected EmbeddingTPGMMetaData computeEmbeddingMetaData() {
    return new EmbeddingTPGMMetaData(getChildNode().getEmbeddingMetaData());
  }

  @Override
  public String toString() {
    return String.format("FilterTemporalEmbeddingsNode{filterPredicate=%s}", filterPredicate);
  }
}
