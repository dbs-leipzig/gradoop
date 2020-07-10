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
package org.gradoop.temporal.model.impl.operators.matching.common.query.predicates;

import org.gradoop.common.model.api.entities.GraphElement;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.PredicateCollection;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.EmbeddingMetaData;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Represents a collection conjunct OrPredicates. This can be used to represent a CNF.
 */
public class TemporalCNF extends PredicateCollection<CNFElementTPGM> {

  /**
   * Creates a new conjunctive normal form with empty predicate list.
   */
  public TemporalCNF() {
    this.predicates = new ArrayList<>();
  }

  /**
   * Creates a new conjunctive normal form with given predicate list.
   *
   * @param predicates predicates
   */
  public TemporalCNF(List<CNFElementTPGM> predicates) {
    this.predicates = predicates;
  }

  /**
   * Copy constructor for CNF.
   *
   * @param copyValue CNF to copy
   */
  public TemporalCNF(TemporalCNF copyValue) {
    this(new ArrayList<>(copyValue.getPredicates()));
  }

  /**
   * Connects another CNF via AND. Duplicate predicates are removed.
   *
   * @param other a predicate in cnf
   * @return this
   */
  public TemporalCNF and(TemporalCNF other) {
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
  public TemporalCNF or(TemporalCNF other) {
    ArrayList<CNFElementTPGM> newPredicates = new ArrayList<>();

    for (CNFElementTPGM p : predicates) {
      for (CNFElementTPGM q : other.getPredicates()) {
        CNFElementTPGM newCNFElementTPGM = new CNFElementTPGM();
        newCNFElementTPGM.addPredicates(p.getPredicates());
        newCNFElementTPGM.addPredicates(q.getPredicates());

        newPredicates.add(newCNFElementTPGM);
      }
    }
    predicates = newPredicates;
    return this;
  }

  @Override
  public boolean evaluate(Embedding embedding, EmbeddingMetaData metaData) {
    for (CNFElementTPGM element : predicates) {
      if (!element.evaluate(embedding, metaData)) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean evaluate(GraphElement graphElement) {
    for (CNFElementTPGM element : predicates) {
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
   * Filters all disjunctions that could be evaluated with the given variable and returns them in a new CNF.
   * <p>
   * Example:
   * <br>
   * Given {@code myFilter = CNF((a = b) And (b > 5 OR a > 10) AND (c = false) AND (a = c))}<br>
   * {@code myFilter.getSubCNF(a,b) => CNF((a = b) And (b > 5 OR a > 10))}
   *
   * @param variable variable that must be included in the disjunction
   * @return CNF containing only the specified variable
   */
  public TemporalCNF getSubCNF(String variable) {
    Set<String> variables = new HashSet<>(1);
    variables.add(variable);
    return getSubCNF(variables);
  }

  /**
   * Filters all disjunctions that could be evaluated with the given set of variables and returns
   * them in a new CNF.
   * <p>
   * Example:
   * <br>
   * Given {@code myFilter = CNF((a = b) And (b > 5 OR a > 10) AND (c = false) AND (a = c))}<br>
   * {@code myFilter.getSubCNF(a,b) => CNF((a = b) And (b > 5 OR a > 10))}
   *
   * @param variables set of variables that must be included in the disjunction
   * @return CNF containing only variables covered by the input list
   */
  public TemporalCNF getSubCNF(Set<String> variables) {
    List<CNFElementTPGM> filtered = predicates
      .stream()
      .filter(p -> variables.containsAll(p.getVariables()))
      .collect(Collectors.toList());

    return new TemporalCNF(filtered);
  }

  /**
   * Filters all disjunctions that could be evaluated with the given variable and removes
   * them from the CNF. The filtered predicates will be returned in a new CNF.
   * <p>
   * Example:
   * <br>
   * Given {@code myFilter = CNF((a = 10) AND (b > 5 OR a > 10) AND (c = false) AND (a = c))}<br>
   * {@code myFilter.removeSubCNF(a) => CNF(a = 10)}<br>
   * and {@code myFilter == CNF((b > 5 OR a > 10) AND (c = false) AND (a = c))}
   *
   * @param variable variable that must be included in the disjunction
   * @return CNF containing only variables covered by the input list
   */
  public TemporalCNF removeSubCNF(String variable) {
    Set<String> variables = new HashSet<>(1);
    variables.add(variable);
    return removeSubCNF(variables);
  }

  /**
   * Filters all disjunctions that could be evaluated with the given set of variables and removes
   * them from the CNF. The filtered predicates will be returned in a new CNF.
   * <p>
   * Example:
   * <br>
   * Given {@code myFilter = CNF((a = b) AND (b > 5 OR a > 10) AND (c = false) AND (a = c))}<br>
   * {@code myFilter.removeSubCNF(a,b) => CNF((a = b) And (b > 5 OR a > 10))}<br>
   * and {@code myFilter == CNF((c = false) AND (a = c))}
   *
   * @param variables set of variables that must be included in the disjunction
   * @return CNF containing only variables covered by the input list
   */
  public TemporalCNF removeSubCNF(Set<String> variables) {
    Map<Boolean, List<CNFElementTPGM>> filtered = predicates
      .stream()
      .collect(Collectors.partitioningBy(p -> variables.containsAll(p.getVariables())));

    this.predicates = filtered.get(false);

    return new TemporalCNF(filtered.get(true));
  }

  @Override
  public Set<String> getVariables() {
    Set<String> variables = new HashSet<>();
    for (CNFElementTPGM cnfElement : predicates) {
      variables.addAll(cnfElement.getVariables());
    }
    return variables;
  }

  @Override
  public Set<String> getPropertyKeys(String variable) {
    Set<String> properties = new HashSet<>();
    for (CNFElementTPGM cnfElement : predicates) {
      properties.addAll(cnfElement.getPropertyKeys(variable));
    }
    return properties;
  }
}
