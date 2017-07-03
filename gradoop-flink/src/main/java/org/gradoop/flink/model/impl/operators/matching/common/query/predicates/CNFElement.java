
package org.gradoop.flink.model.impl.operators.matching.common.query.predicates;

import org.gradoop.common.model.impl.pojo.GraphElement;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.expressions.ComparisonExpression;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.EmbeddingMetaData;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Represents a collection disjunct predicates
 * This can be used to represent a CNF
 */
public class CNFElement extends PredicateCollection<ComparisonExpression> {

  /**
   * Creates a new CNFElement with empty predicate list
   */
  public CNFElement() {
    this.predicates = new ArrayList<>();
  }

  /**
   * Creats a new CNFElement with preset predicate list
   *
   * @param predicates predicates
   */
  public CNFElement(List<ComparisonExpression> predicates) {
    this.predicates = predicates;
  }

  @Override
  public boolean evaluate(Embedding embedding, EmbeddingMetaData metaData) {
    for (ComparisonExpression comparisonExpression : predicates) {
      if (comparisonExpression.evaluate(embedding, metaData)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean evaluate(GraphElement element) {
    for (ComparisonExpression comparisonExpression : predicates) {
      if (comparisonExpression.evaluate(element)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public Set<String> getVariables() {
    Set<String> variables = new HashSet<>();
    for (ComparisonExpression comparisonExpression : predicates) {
      variables.addAll(comparisonExpression.getVariables());
    }
    return variables;
  }

  @Override
  public Set<String> getPropertyKeys(String variable) {
    Set<String> properties = new HashSet<>();
    for (ComparisonExpression comparisonExpression : predicates) {
      properties.addAll(comparisonExpression.getPropertyKeys(variable));
    }
    return properties;
  }

  @Override
  public String operatorName() {
    return "OR";
  }
}
