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

import org.gradoop.flink.model.impl.operators.matching.common.query.QueryHandler;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.QueryPredicate;
import org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.CNFPostProcessing;
import org.gradoop.temporal.model.impl.operators.matching.common.query.postprocessing.exceptions.QueryContradictoryException;
import org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.ComparableTPGMFactory;
import org.s1ck.gdl.GDLHandler;
import org.s1ck.gdl.model.Edge;
import org.s1ck.gdl.model.Vertex;
import org.s1ck.gdl.model.comparables.time.MaxTimePoint;
import org.s1ck.gdl.model.comparables.time.MinTimePoint;
import org.s1ck.gdl.model.comparables.time.TimeLiteral;
import org.s1ck.gdl.model.comparables.time.TimeSelector;
import org.s1ck.gdl.model.predicates.Predicate;
import org.s1ck.gdl.model.predicates.booleans.And;
import org.s1ck.gdl.model.predicates.expressions.Comparison;

import java.util.HashMap;
import java.util.Map;

import static org.s1ck.gdl.utils.Comparator.LTE;

/**
 * Wraps a {@link GDLHandler} and adds functionality needed for query
 * processing during graph pattern matching.
 * Extension of the flink QueryHandler for temporal queries
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
   * @throws QueryContradictoryException if a contradiction in the query is encountered
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
   * predicates defined in the query, a CNF containing zero predicates is returned.
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
      // some transformations on the GDL query before conversion to CNF
      predicate = preprocessPredicate(predicate);
      CNF rawCNF = QueryPredicate.createFrom(predicate, new ComparableTPGMFactory(now)).asCNF();
      cnf = cnfPostProcessing.postprocess(rawCNF);
    } else {
      // empty CNF
      cnf = new CNF();
    }
  }

  /**
   * Pipeline for preprocessing GDL query predicates. Currently, only predicates for
   * valid interval overlap of edges and their vertices are added.
   *
   * @param predicate the GDL predicate to preprocess
   * @return preprocessed GDL predicate
   */
  private Predicate preprocessPredicate(Predicate predicate) {
    return addEdgeOverlapPredicates(predicate);
  }

  /**
   * Augments a predicate by constraints ensuring that valid times of
   * edges and their adjacent nodes overlap
   * @param predicate the predicate to augment
   * @return predicate augmented by edge overlaps constraints for every edge
   */
  private Predicate addEdgeOverlapPredicates(Predicate predicate) {
    GDLHandler handler = getGdlHandler();
    Map<String, Edge> edgeCache = handler.getEdgeCache();

    // maps vertex ids to vertex variables
    Map<Long, String> vertexMap = new HashMap<>();
    for (Vertex vertex : handler.getVertexCache(true, true).values()) {
      vertexMap.put(vertex.getId(), vertex.getVariable());
    }

    // determine all (vertex, edge, vertex) triples
    for (String edgeVar: handler.getEdgeCache().keySet()) {
      Edge edge = edgeCache.get(edgeVar);
      // no temporal predicates for paths
      if (edge.hasVariableLength()) {
        continue;
      }
      String sourceVar = vertexMap.get(edge.getSourceVertexId());
      String targetVar = vertexMap.get(edge.getTargetVertexId());


      // all relevant val-selectors
      TimeSelector eFrom = new TimeSelector(edgeVar, TimeSelector.TimeField.VAL_FROM);
      TimeSelector eTo = new TimeSelector(edgeVar, TimeSelector.TimeField.VAL_TO);

      TimeSelector sFrom = new TimeSelector(sourceVar, TimeSelector.TimeField.VAL_FROM);
      TimeSelector sTo = new TimeSelector(sourceVar, TimeSelector.TimeField.VAL_TO);

      TimeSelector tFrom = new TimeSelector(targetVar, TimeSelector.TimeField.VAL_FROM);
      TimeSelector tTo = new TimeSelector(targetVar, TimeSelector.TimeField.VAL_TO);

      // create predicates edgeVar.val.overlaps(sourceVar.val) AND
      // edgeVar.val.overlaps(targetVar.val)...
      Predicate overlaps = new Comparison(
          new MaxTimePoint(eFrom, sFrom, tFrom), LTE, new MinTimePoint(eTo, sTo, tTo)
        );
      // ...and add them to the resulting predicate
      predicate = new And(predicate, overlaps);
    }
    return predicate;
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
