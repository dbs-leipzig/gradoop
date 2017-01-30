/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.planner.greedy;

import com.google.common.collect.Sets;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.operators.matching.common.MatchStrategy;
import org.gradoop.flink.model.impl.operators.matching.common.query.QueryHandler;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.common.statistics.GraphStatistics;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.estimation.QueryPlanEstimator;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.plantable.PlanTable;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.plantable.PlanTableEntry;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.QueryPlan;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.binary.JoinEmbeddingsNode;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.leaf.FilterAndProjectEdgesNode;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.leaf.FilterAndProjectVerticesNode;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.unary.FilterEmbeddingsNode;
import org.s1ck.gdl.model.Edge;
import org.s1ck.gdl.model.Vertex;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.plantable.PlanTableEntry.Type.EDGE;
import static org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.plantable.PlanTableEntry.Type.PATH;
import static org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.plantable.PlanTableEntry.Type.VERTEX;

public class GreedyPlanner {
  /**
   * The search graph to be queried
   */
  private final LogicalGraph graph;
  /**
   * The query handler represents the query.
   */
  private final QueryHandler queryHandler;
  /**
   * Statistics about the search graph.
   */
  private final GraphStatistics graphStatistics;
  /**
   * The morphism type for vertex mappings.
   */
  private final MatchStrategy vertexStrategy;
  /**
   * The morphism type for edge mappings.
   */
  private final MatchStrategy edgeStrategy;

  /**
   * Creates a new greedy planner.
   *
   * @param graph search graph
   * @param queryHandler query handler
   * @param graphStatistics search graph statistics
   * @param vertexStrategy morphism type for vertex mappings
   * @param edgeStrategy morphism type for edge mappings
   */
  public GreedyPlanner(LogicalGraph graph, QueryHandler queryHandler,
    GraphStatistics graphStatistics, MatchStrategy vertexStrategy, MatchStrategy edgeStrategy) {
    this.graph = graph;
    this.queryHandler = queryHandler;
    this.graphStatistics = graphStatistics;
    this.vertexStrategy = vertexStrategy;
    this.edgeStrategy = edgeStrategy;
  }

  public PlanTableEntry plan() {
    // create initial plan table covering all possible leaf nodes
    PlanTable planTable = initPlanTable();

    planTable.forEach(System.out::println);

    int i = 0;
    while(planTable.size() > 1) {
      PlanTable newPlans = evaluateJoinEmbeddings(planTable);
      newPlans = evaluateFilterEmbeddings(newPlans);
      PlanTableEntry bestEntry = newPlans.min();
      planTable.removeProcessedBy(bestEntry);
      planTable.add(bestEntry);

      System.out.println("iteration " + ++i);
      planTable.forEach(System.out::println);
    }

    return planTable.get(0);
  }

  //------------------------------------------------------------------------------------------------
  // Initialization
  //------------------------------------------------------------------------------------------------

  /**
   * Creates the initial plan table entries according to the specified vertices and edges.
   *
   * @return initial plan table
   */
  private PlanTable initPlanTable() {
    PlanTable planTable = new PlanTable();
    planTable = initVertices(planTable);
    planTable = initEdges(planTable);
    return planTable;
  }

  private PlanTable initVertices(PlanTable planTable) {
    for (Vertex vertex : queryHandler.getVertices()) {

      String variable = vertex.getVariable();
      CNF predicates = queryHandler.getPredicates();

      FilterAndProjectVerticesNode node = new FilterAndProjectVerticesNode(graph.getVertices(),
        vertex.getVariable(), predicates.removeSubCNF(variable), queryHandler.getPredicates().getPropertyKeys(vertex.getVariable()));

      planTable.add(new PlanTableEntry(VERTEX, Sets.newHashSet(variable), predicates,
        new QueryPlanEstimator(new QueryPlan(node), queryHandler, graphStatistics)));
    }
    return planTable;
  }

  private PlanTable initEdges(PlanTable planTable) {
    for (Edge edge : queryHandler.getEdges()) {
      String edgeVariable = edge.getVariable();
      String sourceVariable = queryHandler.getVertexById(edge.getSourceVertexId()).getVariable();
      String targetVariable = queryHandler.getVertexById(edge.getTargetVertexId()).getVariable();
      CNF predicates = queryHandler.getPredicates();

      FilterAndProjectEdgesNode node = new FilterAndProjectEdgesNode(graph.getEdges(),
        sourceVariable, edgeVariable, targetVariable, predicates.removeSubCNF(edgeVariable),
        queryHandler.getPredicates().getPropertyKeys(edgeVariable));

      planTable.add(new PlanTableEntry(EDGE, Sets.newHashSet(edgeVariable), predicates,
        new QueryPlanEstimator(new QueryPlan(node), queryHandler, graphStatistics)));
    }
    return planTable;
  }

  //----------------------------------------------------------------------------------------------
  // Join evaluation
  //----------------------------------------------------------------------------------------------

  private PlanTable evaluateJoinEmbeddings(PlanTable currentTable) {
    PlanTable newTable = new PlanTable();

    for (int i = 0; i < currentTable.size(); i++) {
      PlanTableEntry leftEntry = currentTable.get(i);
      if (mayExtend(leftEntry)) {
        for (int j = 0; j < currentTable.size(); j++) {
          PlanTableEntry rightEntry = currentTable.get(j);
          if (i != j) {
            List<String> overlap = getOverlap(leftEntry, rightEntry);
            if (overlap.size() > 0) {
              newTable.add(joinEntries(leftEntry, rightEntry, overlap));
            }
          }
        }
      }
    }
    return newTable;
  }

  private boolean mayExtend(PlanTableEntry entry) {
    return entry.getType() == VERTEX || entry.getType() == PATH;
  }

  private List<String> getOverlap(PlanTableEntry leftEntry, PlanTableEntry rightEntry) {
    return leftEntry.getAllVariables().stream()
      .filter(var -> rightEntry.getAllVariables().contains(var))
      .collect(Collectors.toList());
  }

  private PlanTableEntry joinEntries(PlanTableEntry leftEntry, PlanTableEntry rightEntry, List<String> joinVariables) {
    JoinEmbeddingsNode node = new JoinEmbeddingsNode(leftEntry.getQueryPlan().getRoot(),
      rightEntry.getQueryPlan().getRoot(), joinVariables, vertexStrategy, edgeStrategy);

    HashSet<String> evaluatedVars = Sets.newHashSet(leftEntry.getProcessedVariables());
    evaluatedVars.addAll(rightEntry.getProcessedVariables());
    leftEntry.getPredicates().removeSubCNF(rightEntry.getProcessedVariables());
    rightEntry.getPredicates().removeSubCNF(leftEntry.getProcessedVariables());
    CNF predicates = leftEntry.getPredicates().and(rightEntry.getPredicates());

    return new PlanTableEntry(PATH, evaluatedVars, predicates,
      new QueryPlanEstimator(new QueryPlan(node), queryHandler, graphStatistics));
  }

  //----------------------------------------------------------------------------------------------
  // Filter embedding evaluation
  //----------------------------------------------------------------------------------------------

  private PlanTable evaluateFilterEmbeddings(PlanTable currentTable) {
    PlanTable newTable = new PlanTable();

    for (PlanTableEntry entry : currentTable) {
      Set<String> variables = Sets.newHashSet(entry.getAttributedVariables());
      CNF predicates = entry.getPredicates();
      CNF subCNF = predicates.removeSubCNF(variables);
      if (predicates.size() > 0) {
        FilterEmbeddingsNode node = new FilterEmbeddingsNode(entry.getQueryPlan().getRoot(), subCNF);
        newTable.add(new PlanTableEntry(PATH, Sets.newHashSet(entry.getProcessedVariables()),
          predicates, new QueryPlanEstimator(new QueryPlan(node), queryHandler, graphStatistics)));
      }
      newTable.add(entry);
    }

    return newTable;
  }
}
