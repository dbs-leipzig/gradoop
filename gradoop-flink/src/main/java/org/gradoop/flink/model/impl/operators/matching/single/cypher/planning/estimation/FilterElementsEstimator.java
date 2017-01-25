package org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.estimation;

import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNFElement;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.comparables.LiteralComparable;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.comparables.PropertySelectorComparable;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.expressions.ComparisonExpression;
import org.gradoop.flink.model.impl.operators.matching.common.statistics.GraphStatistics;

import java.util.Optional;

public class FilterElementsEstimator implements Estimator {

  public enum ElementType {
    VERTEX, EDGE
  }

  private final GraphStatistics graphStatistics;

  private final String variable;

  private final boolean isVertex;

  private final CNF predicate;

  public FilterElementsEstimator(GraphStatistics graphStatistics, String variable, ElementType elementType, CNF predicate) {
    this.graphStatistics = graphStatistics;
    this.variable = variable;
    this.isVertex = elementType == ElementType.VERTEX;
    this.predicate = predicate;
  }

  @Override
  public long getCardinality() {
    long cardinality = isVertex ? graphStatistics.getVertexCount() : graphStatistics.getEdgeCount();
    Optional<String> label = getLabel();

    if (label.isPresent()) {
      cardinality = isVertex ? graphStatistics.getVertexCountByLabel(label.get()) :
        graphStatistics.getEdgeCountByLabel(label.get());
    }

    return cardinality;
  }

  private Optional<String> getLabel() {
    String label = null;
    for (CNFElement cnfElement : predicate.getPredicates()) {
      for (ComparisonExpression comparisonExpression : cnfElement.getPredicates()) {
        if (comparisonExpression.getLhs() instanceof PropertySelectorComparable) {
          PropertySelectorComparable propertySelector = (PropertySelectorComparable) comparisonExpression.getLhs();
          if (propertySelector.getVariable().equals(variable) && propertySelector.getPropertyKey().equals("__label__")) {
            if (comparisonExpression.getRhs() instanceof LiteralComparable) {
              label = ((LiteralComparable) comparisonExpression.getRhs()).getValue().toString();
            }
          }
        }
      }
    }
    return label != null ? Optional.of(label) : Optional.empty();
  }
}