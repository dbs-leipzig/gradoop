
package org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.unary;

import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.EmbeddingMetaData;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.filter.FilterEmbeddings;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.FilterNode;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.PlanNode;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.UnaryNode;

/**
 * Unary nodes that wraps a {@link FilterEmbeddings} operator.
 */
public class FilterEmbeddingsNode extends UnaryNode implements FilterNode {
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
    FilterEmbeddings op =
      new FilterEmbeddings(getChildNode().execute(), filterPredicate, getEmbeddingMetaData());
    op.setName(toString());
    return op.evaluate();
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
    return String.format("FilterEmbeddingsNode{filterPredicate=%s}", filterPredicate);
  }
}
