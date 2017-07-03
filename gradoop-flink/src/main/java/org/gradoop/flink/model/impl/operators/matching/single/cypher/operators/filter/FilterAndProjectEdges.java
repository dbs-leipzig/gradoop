
package org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.filter;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.PhysicalOperator;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.filter.functions.FilterAndProjectEdge;

import java.util.List;

/**
 * Filters a set of EPGM {@link Edge} objects based on a specified predicate. Additionally, the
 * operator projects all property values to the output {@link Embedding} that are specified in the
 * given {@code projectionPropertyKeys}.
 *
 * Edge -> Embedding(
 *  [IdEntry(SourceId),IdEntry(EdgeId),IdEntry(TargetId)],
 *  [PropertyEntry(v1),PropertyEntry(v2)]
 * )
 *
 * Example:
 *
 * Given an Edge(0, 1, 2, "friendOf", {since:2017, weight:23}), a predicate "weight = 23" and
 * list of projection property keys [since,isValid] the operator creates
 * an {@link Embedding}:
 *
 * ([IdEntry(1),IdEntry(0),IdEntry(2)],[PropertyEntry(2017),PropertyEntry(NULL)])
 */
public class FilterAndProjectEdges implements PhysicalOperator {
  /**
   * Input graph elements
   */
  private final DataSet<Edge> input;
  /**
   * Predicates in conjunctive normal form
   */
  private final CNF predicates;
  /**
   * Property keys used for projection
   */
  private final List<String> projectionPropertyKeys;
  /**
   * Signals that the edge is a loop
   */
  private boolean isLoop;

  /**
   * Operator name used for Flink operator description
   */
  private String name;

  /**
   * New edge filter operator
   *
   * @param input Candidate edges
   * @param predicates Predicates used to filter edges
   * @param projectionPropertyKeys Property keys used for projection
   * @param isLoop is the edge a loop
   */
  public FilterAndProjectEdges(DataSet<Edge> input, CNF predicates,
    List<String> projectionPropertyKeys, boolean isLoop) {
    this.input = input;
    this.predicates = predicates;
    this.projectionPropertyKeys = projectionPropertyKeys;
    this.isLoop = isLoop;
    this.setName("FilterAndProjectEdges");
  }

  @Override
  public DataSet<Embedding> evaluate() {
    return input
      .flatMap(new FilterAndProjectEdge(predicates, projectionPropertyKeys, isLoop))
      .name(getName());
  }

  @Override
  public void setName(String newName) {
    this.name = newName;
  }

  @Override
  public String getName() {
    return this.name;
  }
}
