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

import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNFElement;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.expressions.ComparisonExpression;
import org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.QueryTransformation;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Abstract base class for subsumptions of clauses.
 * A simplifying assumption is made: smaller clauses are not subsumed by larger ones.
 * It is not guaranteed that all subsumptions or the optimal sequence of subsumptions are applied
 */
public abstract class Subsumption implements QueryTransformation {
  @Override
  public CNF transformCNF(CNF cnf) {
    // separate singleton clauses from disjunctive clauses
    ArrayList<CNFElement> necessaryClauses = cnf.getPredicates().stream()
      .filter(clause -> clause.size() == 1).collect(Collectors.toCollection(ArrayList::new));
    List<CNFElement> disjunctiveClauses = cnf.getPredicates().stream()
      .filter(clause -> clause.size() > 1)
      .collect(Collectors.toList());
    // apply subsumption within disjunctive clauses while keeping the singletons and ...
    cnf = new CNF(necessaryClauses).and(
      subsumeDisjunctiveClauses(disjunctiveClauses));
    List<CNFElement> oldClauses = cnf.getPredicates();
    // ... apply subsumption to the result
    return new CNF(subsumeClauses(oldClauses));
  }

  /**
   * Sort clauses: clauses that subsume most other clauses are sorted at the beginning of
   * the list
   *
   * @param clauses clauses to sort
   * @return sorted clauses
   */
  protected ArrayList<CNFElement> sortClauses(List<CNFElement> clauses) {
    // count for every clause how much clauses it subsumes
    HashMap<CNFElement, Integer> countSubsumes = new HashMap<>();
    for (int i = 0; i < clauses.size(); i++) {
      CNFElement clause1 = clauses.get(i);
      countSubsumes.putIfAbsent(clause1, 0);
      for (int j = i; j < clauses.size(); j++) {
        CNFElement clause2 = clauses.get(j);
        if (subsumes(clause1, clause2)) {
          countSubsumes.put(clause1, countSubsumes.get(clause1) + 1);
        }
        if (subsumes(clause2, clause1)) {
          countSubsumes.putIfAbsent(clause2, 0);
          countSubsumes.put(clause2, countSubsumes.get(clause2) + 1);
        }
      }
    }
    // sort by these counts. If two clauses subsume the same number of clauses,
    // the shorter clause is sorted first
    ArrayList<CNFElement> clausesSorted = new ArrayList<>(clauses);
    clausesSorted.sort(new Comparator<CNFElement>() {
      @Override
      public int compare(CNFElement t1, CNFElement t2) {
        int subsumedDifference = countSubsumes.get(t2) -
          countSubsumes.get(t1);
        return subsumedDifference != 0 ? subsumedDifference :
          t1.size() - t2.size();
      }
    });
    return clausesSorted;
  }

  /**
   * Performs the subsumption of a set of clauses
   *
   * @param clauses clauses for subsumption
   * @return sublist of the clauses after subsumption
   */
  protected ArrayList<CNFElement> subsumeClauses(List<CNFElement> clauses) {
    ArrayList<CNFElement> oldClauses = sortClauses(clauses);
    // points to clauses that are already subsumed (their index in the clauses list)
    HashSet<Integer> subsumed = new HashSet<>();

    // check for every clause if it subsumes other clauses
    for (int i = 0; i < oldClauses.size(); i++) {
      // clause c_i is already subsumed, i.e. actually not there anymore
      if (subsumed.contains(i)) {
        continue;
      }
      // clause c_i is not subsumed yet, i.e. may subsume other clauses
      CNFElement c1 = oldClauses.get(i);
      for (int j = i + 1; j < oldClauses.size(); j++) {
        // clause c_j is already subsumed, i.e. actually not there anymore
        if (subsumed.contains(j)) {
          continue;
        }
        // clause c_j is not subsumed yet, i.e. may be subsumed
        CNFElement c2 = oldClauses.get(j);
        if (subsumes(c1, c2)) {
          subsumed.add(j);
        }
      }
    }
    // only clauses that are not subsumed by other clauses are contained in the new CNF
    ArrayList<CNFElement> newClauses = new ArrayList<>();
    for (int i = 0; i < oldClauses.size(); i++) {
      if (subsumed.contains(i)) {
        continue;
      }
      newClauses.add(oldClauses.get(i));
    }
    return newClauses;
  }

  /**
   * Applies {@link this::subsumeDisjunctiveClause} to all disjunctive clauses
   * (CNF clauses of size > 1)
   *
   * @param disjClauses clauses
   * @return CNF conjoining the subsumed clauses
   */
  protected CNF subsumeDisjunctiveClauses(List<CNFElement> disjClauses) {
    List<CNFElement> newClauses = disjClauses.stream()
      .map(this::subsumeDisjunctiveClause)
      .collect(Collectors.toList());

    return new CNF(newClauses);
  }

  /**
   * Applies subsumption to a disjunctive clause (might simplify the clause)
   *
   * @param clause clause to apply subsumption to.
   * @return subsumed clause
   */
  protected CNFElement subsumeDisjunctiveClause(CNFElement clause) {
    return new CNFElement(subsumeClauses(
      clause.getPredicates().stream()
        .map(comparison -> new CNFElement(Collections.singletonList(comparison)))
        .collect(Collectors.toList()))
      .stream()
      .map(c -> c.getPredicates().get(0))
      .collect(Collectors.toList()));
  }

  /**
   * Checks for two disjunctive clauses c1 and c2 whether c1 subsumes c2.
   * Here, c1 subsumes c2 iff c1's comparisons form a subset of c2's comparisons
   *
   * @param c1 first clause
   * @param c2 second clause
   * @return true iff c1 subsumes c2
   */
  protected boolean subsumes(CNFElement c1, CNFElement c2) {
    for (ComparisonExpression comp1 : c1) {
      boolean foundMatch = false;
      for (ComparisonExpression comp2 : c2) {
        if (subsumes(comp1, comp2)) {
          foundMatch = true;
          break;
        }
      }
      if (!foundMatch) {
        return false;
      }
    }
    return true;
  }

  /**
   * Checks whether a ComparisonExpression c1 subsumes c2
   *
   * @param c1 potentially subsuming comparison
   * @param c2 potentially subsumed comparison
   * @return true iff c1 subsumes c2
   */
  public abstract boolean subsumes(ComparisonExpression c1,
                                   ComparisonExpression c2);
}
