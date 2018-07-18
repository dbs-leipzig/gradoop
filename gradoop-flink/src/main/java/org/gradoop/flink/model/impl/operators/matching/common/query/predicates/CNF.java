/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.impl.operators.matching.common.query.predicates;

import org.gradoop.common.model.impl.pojo.GraphElement;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.EmbeddingMetaData;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Represents a collection conjunct OrPredicates
 * This can be used to represent a CNF
 */
public class CNF extends PredicateCollection<CNFElement> {

  /**
   * Creates a new conjunctive normal form with empty predicate list
   */
  public CNF() {
    this.predicates = new ArrayList<>();
  }

  /**
   * Creates a new conjunctive normal form with given predicate list
   *
   * @param predicates predicates
   */
  public CNF(List<CNFElement> predicates) {
    this.predicates = predicates;
  }

  /**
   * Copy constructor for CNF
   * @param copyValue CNF to copy
   */
  public CNF(CNF copyValue) {
    this(new ArrayList<>(copyValue.getPredicates()));
  }

  /**
   * Connects another CNF via AND. Duplicate predicates are removed.
   *
   * @param other a predicate in cnf
   * @return this
   */
  public CNF and(CNF other) {
    other.getPredicates().stream()
      .filter(predicate -> !this.predicates.contains(predicate))
      .forEach(this::addPredicate);
    return this;
  }

  /**
   * Connects another CNF via OR.
   *
   * @param other a predicate in cnf
   * @return this
   */
  public CNF or(CNF other) {
    ArrayList<CNFElement> newPredicates = new ArrayList<>();

    for (CNFElement p : predicates) {
      for (CNFElement q : other.getPredicates()) {
        CNFElement newCNFElement = new CNFElement();
        newCNFElement.addPredicates(p.getPredicates());
        newCNFElement.addPredicates(q.getPredicates());

        newPredicates.add(newCNFElement);
      }
    }
    predicates = newPredicates;
    return this;
  }

  @Override
  public boolean evaluate(Embedding embedding, EmbeddingMetaData metaData) {
    for (CNFElement element : predicates) {
      if (!element.evaluate(embedding, metaData)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean evaluate(GraphElement graphElement) {
    for (CNFElement element : predicates) {
      if (!element.evaluate(graphElement)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public String operatorName() {
    return "AND";
  }

  /**
   * Filters all disjunctions that could be evaluated with the given variable and returns
   * them in a new CNF
   *
   * Example:
   * Given myFilter = CNF((a = b) And (b > 5 OR a > 10) AND (c = false) AND (a = c))
   * myFilter.getSubCNF(a,b) => CNF((a = b) And (b > 5 OR a > 10))
   *
   * @param variable variable that must be included in the disjunction
   * @return CNF containing only the specified variable
   */
  public CNF getSubCNF(String variable) {
    Set<String> variables = new HashSet<>(1);
    variables.add(variable);
    return getSubCNF(variables);
  }

  /**
   * Filters all disjunctions that could be evaluated with the given set of variables and returns
   * them in a new CNF
   *
   * Example:
   * Given myFilter = CNF((a = b) And (b > 5 OR a > 10) AND (c = false) AND (a = c))
   * myFilter.getSubCNF(a,b) => CNF((a = b) And (b > 5 OR a > 10))
   *
   * @param variables set of variables that must be included in the disjunction
   * @return CNF containing only variables covered by the input list
   */
  public CNF getSubCNF(Set<String> variables) {
    List<CNFElement> filtered = predicates
      .stream()
      .filter(p -> variables.containsAll(p.getVariables()))
      .collect(Collectors.toList());

    return new CNF(filtered);
  }

  /**
   * Filters all disjunctions that could be evaluated with the given variable and removes
   * them from the CNF. The filtered predicates will be returned in a new CNF
   *
   * Example:
   * Given myFilter = CNF((a = 10) AND (b > 5 OR a > 10) AND (c = false) AND (a = c))
   * myFilter.removeSubCNF(a) => CNF(a = 10)
   * and myFilter == CNF((b > 5 OR a > 10) AND (c = false) AND (a = c))
   *
   * @param variable variable that must be included in the disjunction
   * @return CNF containing only variables covered by the input list
   */
  public CNF removeSubCNF(String variable) {
    Set<String> variables = new HashSet<>(1);
    variables.add(variable);
    return removeSubCNF(variables);
  }

  /**
   * Filters all disjunctions that could be evaluated with the given set of variables and removes
   * them from the CNF. The filtered predicates will be returned in a new CNF
   *
   * Example:
   * Given myFilter = CNF((a = b) AND (b > 5 OR a > 10) AND (c = false) AND (a = c))
   * myFilter.removeSubCNF(a,b) => CNF((a = b) And (b > 5 OR a > 10))
   * and myFilter == CNF((c = false) AND (a = c))
   *
   * @param variables set of variables that must be included in the disjunction
   * @return CNF containing only variables covered by the input list
   */
  public CNF removeSubCNF(Set<String> variables) {
    Map<Boolean, List<CNFElement>> filtered = predicates
      .stream()
      .collect(Collectors.partitioningBy(p -> variables.containsAll(p.getVariables())));

    this.predicates = filtered.get(false);

    return new CNF(filtered.get(true));
  }

  @Override
  public Set<String> getVariables() {
    Set<String> variables = new HashSet<>();
    for (CNFElement cnfElement : predicates) {
      variables.addAll(cnfElement.getVariables());
    }
    return variables;
  }

  @Override
  public Set<String> getPropertyKeys(String variable) {
    Set<String> properties = new HashSet<>();
    for (CNFElement cnfElement : predicates) {
      properties.addAll(cnfElement.getPropertyKeys(variable));
    }
    return properties;
  }
}
