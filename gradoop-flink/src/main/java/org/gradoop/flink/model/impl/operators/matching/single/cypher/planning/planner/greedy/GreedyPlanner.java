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
import org.apache.commons.lang3.tuple.Pair;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.operators.matching.common.MatchStrategy;
import org.gradoop.flink.model.impl.operators.matching.common.query.QueryHandler;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.common.statistics.GraphStatistics;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.ExpandDirection;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.estimation.QueryPlanEstimator;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.plantable.PlanTable;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.plantable.PlanTableEntry;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.PlanNode;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.QueryPlan;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.binary.ExpandEmbeddingsNode;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.binary.JoinEmbeddingsNode;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.leaf.FilterAndProjectEdgesNode;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.leaf.FilterAndProjectVerticesNode;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.unary.FilterEmbeddingsNode;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.unary.ProjectEmbeddingsNode;
import org.s1ck.gdl.model.Edge;
import org.s1ck.gdl.model.Vertex;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.plantable.PlanTableEntry.Type.*;

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

  /**
   * Computes the {@link PlanTableEntry} that wraps the {@link QueryPlan} with the minimum costs
   * according to the greedy optimization algorithm.
   *
   * @return entry with minimum execution costs
   */
  public PlanTableEntry plan() {
    PlanTable planTable = initPlanTable();

    while(planTable.size() > 1) {
      PlanTable newPlans = evaluateJoins(planTable);
      newPlans = evaluateFilter(newPlans);
      newPlans = evaluateProjection(newPlans);
      // get plan with minimum costs and remove all plans covered by this plan
      PlanTableEntry bestEntry = newPlans.min();
      planTable.removeCoveredBy(bestEntry);
      planTable.add(bestEntry);

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
    planTable = createVertexPlans(planTable);
    planTable = createEdgePlans(planTable);
    return planTable;
  }

  //------------------------------------------------------------------------------------------------
  // Leaf nodes (i.e. vertices and (variable length) edges)
  //------------------------------------------------------------------------------------------------

  /**
   * Creates an initial {@link PlanTableEntry} for each vertex in the query graph and adds it to the
   * specified {@link PlanTable}. The entry wraps a query plan that filters vertices based on their
   * predicates and projects properties that are required for further query planning.
   *
   * @param planTable plan table
   * @return updated plan table with entries for vertices
   */
  private PlanTable createVertexPlans(PlanTable planTable) {
    for (Vertex vertex : queryHandler.getVertices()) {

      String vertexVariable = vertex.getVariable();
      CNF allPredicates = queryHandler.getPredicates();
      // TODO: this might be moved the the FilterAndProject node in issue #510
      CNF vertexPredicates = allPredicates.removeSubCNF(vertexVariable);
      Set<String> projectionKeys = allPredicates.getPropertyKeys(vertexVariable);

      FilterAndProjectVerticesNode node = new FilterAndProjectVerticesNode(graph.getVertices(),
        vertex.getVariable(), vertexPredicates, projectionKeys);

      planTable.add(new PlanTableEntry(VERTEX, Sets.newHashSet(vertexVariable), allPredicates,
        new QueryPlanEstimator(new QueryPlan(node), queryHandler, graphStatistics)));
    }
    return planTable;
  }

  /**
   * Creates an initial {@link PlanTableEntry} for each edge in the query graph and adds it to the
   * specified {@link PlanTable}. The entry wraps a {@link QueryPlan} that filters edges based on
   * their predicates and projects properties that are required for further query planning.
   *
   * @param planTable plan table
   * @return updated plan table with entries for edges
   */
  private PlanTable createEdgePlans(PlanTable planTable) {
    for (Edge edge : queryHandler.getEdges()) {
      String edgeVariable = edge.getVariable();
      String sourceVariable = queryHandler.getVertexById(edge.getSourceVertexId()).getVariable();
      String targetVariable = queryHandler.getVertexById(edge.getTargetVertexId()).getVariable();

      CNF allPredicates = queryHandler.getPredicates();
      // TODO: this might be moved the the FilterAndProject node in issue #510
      CNF edgePredicates = allPredicates.removeSubCNF(edgeVariable);
      Set<String> projectionKeys = allPredicates.getPropertyKeys(edgeVariable);

      FilterAndProjectEdgesNode node = new FilterAndProjectEdgesNode(graph.getEdges(),
        sourceVariable, edgeVariable, targetVariable, edgePredicates, projectionKeys);

      PlanTableEntry.Type type = edge.hasVariableLength() ? PATH : EDGE;

      planTable.add(new PlanTableEntry(type, Sets.newHashSet(edgeVariable), allPredicates,
        new QueryPlanEstimator(new QueryPlan(node), queryHandler, graphStatistics)));
    }
    return planTable;
  }

  //------------------------------------------------------------------------------------------------
  // Join and Expand
  //------------------------------------------------------------------------------------------------

  /**
   * Evaluates which entries in the specified plan table can be joined. The joined entries
   * are added to a new table which is returned.
   *
   * @param currentTable query plan table
   * @return new table containing solely joined plans from the input table
   */
  private PlanTable evaluateJoins(PlanTable currentTable) {
    PlanTable newTable = new PlanTable();

    for (int i = 0; i < currentTable.size(); i++) {
      PlanTableEntry leftEntry = currentTable.get(i);
      if (mayExtend(leftEntry)) {
        for (int j = 0; j < currentTable.size(); j++) {
          PlanTableEntry rightEntry = currentTable.get(j);
          if (i != j) {
            List<String> joinVariables = getOverlap(leftEntry, rightEntry);
            if (joinVariables.size() > 0) {
              if (rightEntry.getType() == PATH && joinVariables.size() == 2) {
                // evaluate join with variable length path on source and target vertex
                newTable.add(joinEntries(leftEntry, rightEntry, joinVariables.subList(0, 1)));
                newTable.add(joinEntries(leftEntry, rightEntry, joinVariables.subList(1, 2)));
              } else {
                // regular join or join with variable length path on source or target vertex
                newTable.add(joinEntries(leftEntry, rightEntry, joinVariables));
              }
            }
          }
        }
      }
    }
    return newTable;
  }

  /**
   * Checks if the given entry may be extended. This is only the case for entries that represents
   * either a vertex or a partial match graph.
   *
   * @param entry plan table entry
   * @return true, iff the specified entry may be extended
   */
  private boolean mayExtend(PlanTableEntry entry) {
    return entry.getType() == VERTEX || entry.getType() == GRAPH;
  }

  /**
   * Computes the overlapping query variables of the specified entries.
   *
   * @param firstEntry first entry
   * @param secondEntry second entry
   * @return variables that are available in both input entries
   */
  private List<String> getOverlap(PlanTableEntry firstEntry, PlanTableEntry secondEntry) {
    return firstEntry.getAllVariables().stream()
      .filter(var -> secondEntry.getAllVariables().contains(var))
      .collect(Collectors.toList());
  }

  /**
   * Joins the query plans represented by the specified plan table entries.
   *
   * The method considers if the right entry is a variable length path and in that case
   * creates an {@link ExpandEmbeddingsNode}. In any other case, a regular
   * {@link JoinEmbeddingsNode} is used to join the query plans.
   *
   * @param leftEntry left entry
   * @param rightEntry right entry
   * @param joinVariables join variables
   * @return an entry that represents the join of both input entries
   */
  private PlanTableEntry joinEntries(PlanTableEntry leftEntry, PlanTableEntry rightEntry,
    List<String> joinVariables) {

    PlanNode node;
    if (rightEntry.getType() == PATH) {
      assert(joinVariables.size() == 1);
      node = createExpandNode(leftEntry, rightEntry, joinVariables.get(0));
    } else {
      node = new JoinEmbeddingsNode(leftEntry.getQueryPlan().getRoot(),
        rightEntry.getQueryPlan().getRoot(), joinVariables, vertexStrategy, edgeStrategy);
    }

    // update processed variables
    HashSet<String> processedVariables = Sets.newHashSet(leftEntry.getProcessedVariables());
    processedVariables.addAll(rightEntry.getProcessedVariables());
    // create resulting predicates
    // TODO: this might be moved the the join/expand node in issue #510
    CNF leftPredicates = new CNF(leftEntry.getPredicates());
    CNF rightPredicates = new CNF(rightEntry.getPredicates());
    leftPredicates.removeSubCNF(rightEntry.getProcessedVariables());
    rightPredicates.removeSubCNF(leftEntry.getProcessedVariables());
    CNF predicates = leftPredicates.and(rightPredicates);

    return new PlanTableEntry(GRAPH, processedVariables, predicates,
      new QueryPlanEstimator(new QueryPlan(node), queryHandler, graphStatistics));
  }

  /**
   * Creates an {@link ExpandEmbeddingsNode} from the specified arguments.
   *
   * @param leftEntry left entry
   * @param rightEntry right entry
   * @param startVariable vertex variable to expand from
   *
   * @return new expand node
   */
  private ExpandEmbeddingsNode createExpandNode(PlanTableEntry leftEntry, PlanTableEntry rightEntry,
    String startVariable) {

    String pathVariable = rightEntry.getQueryPlan().getRoot()
      .getEmbeddingMetaData().getEdgeVariables().get(0);

    Edge queryEdge = queryHandler.getEdgeByVariable(pathVariable);
    Vertex sourceVertex = queryHandler.getVertexById(queryEdge.getSourceVertexId());
    Vertex targetVertex = queryHandler.getVertexById(queryEdge.getTargetVertexId());

    int lowerBound = queryEdge.getLowerBound();
    int upperBound = queryEdge.getUpperBound();
    ExpandDirection direction = sourceVertex.getVariable().equals(startVariable) ?
      ExpandDirection.OUT : ExpandDirection.IN;
    String endVariable = direction == ExpandDirection.OUT ?
      targetVertex.getVariable() : sourceVertex.getVariable();

    return new ExpandEmbeddingsNode(leftEntry.getQueryPlan().getRoot(),
      rightEntry.getQueryPlan().getRoot(),
      startVariable, pathVariable, endVariable, lowerBound, upperBound, direction,
      vertexStrategy, edgeStrategy);
  }

  //------------------------------------------------------------------------------------------------
  // Filter embedding evaluation
  //------------------------------------------------------------------------------------------------

  /**
   * The method checks if a filter can be applied on any of the entries in the specified table. If
   * this is the case, a {@link FilterEmbeddingsNode} is added to the query plan represented by the
   * affected entries.
   *
   * @param currentTable query plan table
   * @return input table with possibly updated entries
   */
  private PlanTable evaluateFilter(PlanTable currentTable) {
    PlanTable newTable = new PlanTable();

    for (PlanTableEntry entry : currentTable) {
      Set<String> variables = Sets.newHashSet(entry.getProcessedVariables());
      CNF predicates = entry.getPredicates();
      CNF subCNF = predicates.removeSubCNF(variables);
      if (subCNF.size() > 0) {
        FilterEmbeddingsNode node = new FilterEmbeddingsNode(entry.getQueryPlan().getRoot(), subCNF);
        newTable.add(new PlanTableEntry(GRAPH, Sets.newHashSet(entry.getProcessedVariables()),
          predicates, new QueryPlanEstimator(new QueryPlan(node), queryHandler, graphStatistics)));
      }
      newTable.add(entry);
    }

    return newTable;
  }

  //------------------------------------------------------------------------------------------------
  // Filter embedding evaluation
  //------------------------------------------------------------------------------------------------

  /**
   * The method checks if a filter can be applied on any of the entries in the specified table. If
   * this is the case, a {@link ProjectEmbeddingsNode} is added to the query plan represented by the
   * affected entries.
   *
   * @param currentTable query plan table
   * @return input table with possibly updated entries
   */
  private PlanTable evaluateProjection(PlanTable currentTable) {
    PlanTable newTable = new PlanTable();

    for (PlanTableEntry entry : currentTable) {
      Set<Pair<String, String>> propertyPairs = entry.getPropertyPairs();
      Set<Pair<String, String>> projectionPairs = entry.getProjectionPairs();

      Set<Pair<String, String>> updatedPropertyPairs = propertyPairs.stream()
        .filter(projectionPairs::contains)
        .collect(Collectors.toSet());

      if (updatedPropertyPairs.size() < propertyPairs.size()) {
        ProjectEmbeddingsNode node = new ProjectEmbeddingsNode(entry.getQueryPlan().getRoot(),
          updatedPropertyPairs.stream().collect(Collectors.toList()));
        newTable.add(new PlanTableEntry(GRAPH,
          Sets.newHashSet(entry.getProcessedVariables()), entry.getPredicates(),
          new QueryPlanEstimator(new QueryPlan(node), queryHandler, graphStatistics)));
      } else {
        newTable.add(entry);
      }
    }
    return newTable;
  }
}
