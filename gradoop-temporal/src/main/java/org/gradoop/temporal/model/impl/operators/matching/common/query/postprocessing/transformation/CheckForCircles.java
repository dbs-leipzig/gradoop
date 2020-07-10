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
package org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.transformation;

import org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.QueryTransformation;
import org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.exceptions.QueryContradictoryException;
import org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.relationgraph.RelationGraph;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.TemporalCNF;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.expressions.ComparisonExpressionTPGM;
import org.s1ck.gdl.utils.Comparator;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.s1ck.gdl.utils.Comparator.EQ;
import static org.s1ck.gdl.utils.Comparator.LT;
import static org.s1ck.gdl.utils.Comparator.LTE;
import static org.s1ck.gdl.utils.Comparator.NEQ;

/**
 * Builds a relation graph (cf. {@link RelationGraph}
 * from the necessary temporal comparisons in a CNF and checks
 * if it contains forbidden cyclic paths.
 * For example, a graph a<b=a would be forbidden, as it implies a contradiction
 * in the CNF.
 */
public class CheckForCircles implements QueryTransformation {

  @Override
  public TemporalCNF transformCNF(TemporalCNF cnf) throws QueryContradictoryException {
    List<List<Comparator>> cyclicPaths =
      new RelationGraph(
        getNecessaryTemporalComparisons(cnf)
      ).findAllCircles();
    for (List<Comparator> cyclicPath : cyclicPaths) {
      if (illegalCircle(cyclicPath)) {
        throw new QueryContradictoryException();
      }
    }
    return cnf;
  }

  /**
   * Checks whether a cyclic path in the relations graph implies a contradictory query
   *
   * @param cyclicPath cyclic path to check
   * @return true iff the cyclic path implies a contradictory query
   */
  private boolean illegalCircle(List<Comparator> cyclicPath) {
    int countEQ = Collections.frequency(cyclicPath, EQ);
    int countNEQ = Collections.frequency(cyclicPath, NEQ);
    int countLTE = Collections.frequency(cyclicPath, LTE);
    int countLT = Collections.frequency(cyclicPath, LT);

    return ((countNEQ == 0 && countLT >= 1) ||
      (countEQ >= 1 && countNEQ == 1 && countLTE == 0 && countLT == 0));
  }

  /**
   * Returns the set of necessary comparisons in the CNF, i.e. all comparisons that
   * form a singleton clause
   *
   * @param cnf CNF
   * @return set of necessary comparisons
   */
  private Set<ComparisonExpressionTPGM> getNecessaryTemporalComparisons(TemporalCNF cnf) {
    return cnf.getPredicates().stream()
      .filter(clause -> clause.size() == 1)
      .map(clause -> clause.getPredicates().get(0))
      .filter(comparison -> comparison.isTemporal())
      .collect(Collectors.toSet());
  }
}
