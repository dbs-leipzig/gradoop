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
package org.gradoop.temporal.model.impl.operators.matching.common.query;

import com.google.common.collect.Sets;
import org.apache.commons.lang3.tuple.Pair;
import org.gradoop.common.util.GradoopConstants;
import org.gradoop.flink.model.impl.operators.matching.common.query.QueryHandler;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNFElement;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.QueryPredicate;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.expressions.ComparisonExpression;
import org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.CNFPostProcessing;
import org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.exceptions.QueryContradictoryException;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.ComparableTPGMFactory;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.comparables.TemporalComparable;
import org.s1ck.gdl.GDLHandler;
import org.s1ck.gdl.exceptions.BailSyntaxErrorStrategy;
import org.s1ck.gdl.model.Edge;
import org.s1ck.gdl.model.Vertex;
import org.s1ck.gdl.model.comparables.time.TimeLiteral;
import org.s1ck.gdl.model.comparables.time.TimeSelector;
import org.s1ck.gdl.model.predicates.Predicate;
import org.s1ck.gdl.model.predicates.booleans.And;
import org.s1ck.gdl.model.predicates.expressions.Comparison;
import org.s1ck.gdl.utils.Comparator;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Wraps a {@link GDLHandler} and adds functionality needed for query
 * processing during graph pattern matching.
 * Extension for temporal queries
 */
public class TemporalQueryHandler extends QueryHandler {
  /**
   * Time Literal representing the systime at the start of query processing
   */
  private final TimeLiteral now;
  /**
   * Transformations to apply to the query cnf
   */
  private final CNFPostProcessing cnfPostProcessing;

  /**
   * The query CNF
   */
  private CNF cnf;

  /**
   * Creates a new query handler that postprocesses the CNF with a default
   * {@link CNFPostProcessing}
   *
   * @param gdlString GDL query string
   */
  public TemporalQueryHandler(String gdlString) throws QueryContradictoryException {
    this(gdlString, new CNFPostProcessing());
  }

  /**
   * Creates a new query handler.
   *
   * @param gdlString      GDL query string
   * @param postProcessing transformations to apply to the CNF
   * @throws QueryContradictoryException if a contradiction in the query is encountered
   */
  public TemporalQueryHandler(String gdlString, CNFPostProcessing postProcessing)
    throws QueryContradictoryException {
    super(gdlString);
    this.cnfPostProcessing = postProcessing;
    now = new TimeLiteral("now");
    initCNF();
  }

  /**
   * Returns all available predicates in Conjunctive Normal Form {@link CNF}. If there are no
   * predicated defined in the query, a CNF containing zero predicates is returned.
   *
   * @return predicates
   */
  @Override
  public CNF getPredicates() {
    return cnf;
  }

  /**
   * Initiates the query CNF from the query, including postprocessing
   *
   * @throws QueryContradictoryException if query is found to be contradictory
   */
  private void initCNF() throws QueryContradictoryException {
    if (getGdlHandler().getPredicates().isPresent()) {
      Predicate predicate = getGdlHandler().getPredicates().get();
      //predicate = preprocessPredicate(predicate);
      CNF rawCNF = QueryPredicate.createFrom(predicate, new ComparableTPGMFactory()).asCNF();
      System.out.println("Initial CNF: "+rawCNF);
      cnf = cnfPostProcessing.postprocess(rawCNF);
      // TODO add edge joins
    } else {
      cnf = new CNF();
    }
    System.out.println("Effective CNF: "+cnf);
  }

  /**
   * Pipeline for preprocessing query predicates. Currently, only a {@link #defaultAsOfExpansion}
   * is done.
   *
   * @param predicate hte predicate to preprocess
   * @return preprocessed predicate
   */
  private Predicate preprocessPredicate(Predicate predicate) {
    return defaultAsOfExpansion(predicate);
  }

  /**
   * Preprocessing function for predicates. Adds v.asOf(now) constraints for every
   * query graph element v iff no constraint on any tx_to value is contained in the
   * predicate.
   *
   * @param predicate predicate to augment with asOf(now) predicates
   * @return predicate with v.asOf(now) constraints for every
   * query graph element v iff no constraint on any tx_to value is contained in the
   * predicate (else input predicate is returned).
   */
  private Predicate defaultAsOfExpansion(Predicate predicate) {
    GDLHandler gdlHandler = getGdlHandler();
    if (predicate != null && predicate.containsSelectorType(TimeSelector.TimeField.TX_TO)) {
      // no default asOfs if a constraint on any tx_to value is contained
      return predicate;
    } else {
      // add v.asOf(now) for every element in the query
      ArrayList<String> vars = new ArrayList<>(gdlHandler.getEdgeCache(true, true).keySet());
      vars.addAll(gdlHandler.getVertexCache(true, true).keySet());
      And asOf0 = new And(
        new Comparison(
          new TimeSelector(vars.get(0), TimeSelector.TimeField.TX_FROM),
          Comparator.LTE, now),
        new Comparison(
          new TimeSelector(vars.get(0), TimeSelector.TimeField.TX_TO),
          Comparator.GTE, now)
      );
      if (predicate == null) {
        predicate = asOf0;
      } else {
        predicate = new And(predicate, asOf0);
      }
      for (int i = 1; i < vars.size(); i++) {
        String v = vars.get(i);
        And asOf = new And(
          new Comparison(
            new TimeSelector(v, TimeSelector.TimeField.TX_FROM),
            Comparator.LTE, now),
          new Comparison(
            new TimeSelector(v, TimeSelector.TimeField.TX_TO),
            Comparator.GTE, now)
        );
        predicate = new And(predicate, asOf);
      }
      return predicate;
    }
  }

  /**
   * Returns the TimeLiteral representing the systime at the start of query processing
   *
   * @return TimeLiteral representing the systime at the start of query processing
   */
  public TimeLiteral getNow() {
    return now;
  }

  /**
   * Checks if the graph returns a single vertex and no edges (no loops).
   *
   * @return true, if single vertex graph
   */
  public boolean isSingleVertexGraph() {
    return getVertexCount() == 1 && getEdgeCount() == 0;
  }

  /**
   * Returns the number of vertices in the query graph.
   *
   * @return vertex count
   */
  public int getVertexCount() {
    return getVertices().size();
  }

  /**
   * Returns the number of edges in the query graph.
   *
   * @return edge count
   */
  public int getEdgeCount() {
    return getEdges().size();
  }

}
