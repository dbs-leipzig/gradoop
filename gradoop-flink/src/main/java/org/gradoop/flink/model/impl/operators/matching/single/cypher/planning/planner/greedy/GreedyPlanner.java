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
package org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.planner.greedy;

import com.google.common.collect.Sets;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.util.GradoopConstants;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.matching.common.MatchStrategy;
import org.gradoop.flink.model.impl.operators.matching.common.query.QueryHandler;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNFElement;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.QueryComparable;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.comparables.PropertySelectorComparable;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.expressions.ComparisonExpression;
import org.gradoop.flink.model.impl.operators.matching.common.statistics.GraphStatistics;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.utils.ExpandDirection;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.estimation.QueryPlanEstimator;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.plantable.PlanTable;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.plantable.PlanTableEntry;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.PlanNode;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.QueryPlan;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.binary.CartesianProductNode;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.binary.ExpandEmbeddingsNode;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.binary.JoinEmbeddingsNode;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.binary.ValueJoinNode;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.leaf.FilterAndProjectEdgesNode;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.leaf.FilterAndProjectVerticesNode;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.unary.FilterEmbeddingsNode;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.queryplan.unary.ProjectEmbeddingsNode;
import org.s1ck.gdl.model.Edge;
import org.s1ck.gdl.model.Vertex;
import org.s1ck.gdl.utils.Comparator;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.plantable.PlanTableEntry.Type.EDGE;
import static org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.plantable.PlanTableEntry.Type.GRAPH;
import static org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.plantable.PlanTableEntry.Type.PATH;
import static org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.plantable.PlanTableEntry.Type.VERTEX;

/**
 * A greedy query planner that builds a query plan by iteratively picking the cheapest partial query
 * plan and extending it.
 */
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

    while (planTable.size() > 1) {
      PlanTable newPlans = evaluateJoins(planTable);

      if (newPlans.size() == 0) {
        // No new plans where generated by joining but there are still multiple disconnected
        // query graph components. In this case we need to do a cartesian product.
        newPlans = evaluateCartesianProducts(planTable);
      }
      newPlans = evaluateFilter(newPlans);
      newPlans = evaluateProjection(newPlans);

      // get plan with minimum costs and remove all plans covered by this plan
      PlanTableEntry bestEntry = newPlans.min();
      planTable.removeCoveredBy(bestEntry);
      planTable.add(bestEntry);
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
    createVertexPlans(planTable);
    createEdgePlans(planTable);
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
   */
  private void createVertexPlans(PlanTable planTable) {
    for (Vertex vertex : queryHandler.getVertices()) {
      String vertexVariable = vertex.getVariable();
      CNF allPredicates = queryHandler.getPredicates();
      // TODO: this might be moved to the FilterAndProject node in issue #510
      CNF vertexPredicates = allPredicates.removeSubCNF(vertexVariable);
      Set<String> projectionKeys = allPredicates.getPropertyKeys(vertexVariable);

      DataSet<org.gradoop.common.model.impl.pojo.Vertex> vertices =
        vertex.getLabel().equals(GradoopConstants.DEFAULT_VERTEX_LABEL) ?
          graph.getVertices() : graph.getVerticesByLabel(vertex.getLabel());

      FilterAndProjectVerticesNode node = new FilterAndProjectVerticesNode(vertices,
        vertex.getVariable(), vertexPredicates, projectionKeys);

      planTable.add(new PlanTableEntry(VERTEX, Sets.newHashSet(vertexVariable), allPredicates,
        new QueryPlanEstimator(new QueryPlan(node), queryHandler, graphStatistics)));
    }
  }

  /**
   * Creates an initial {@link PlanTableEntry} for each edge in the query graph and adds it to the
   * specified {@link PlanTable}. The entry wraps a {@link QueryPlan} that filters edges based on
   * their predicates and projects properties that are required for further query planning.
   *
   * @param planTable plan table
   */
  private void createEdgePlans(PlanTable planTable) {
    for (Edge edge : queryHandler.getEdges()) {
      String edgeVariable = edge.getVariable();
      String sourceVariable = queryHandler.getVertexById(edge.getSourceVertexId()).getVariable();
      String targetVariable = queryHandler.getVertexById(edge.getTargetVertexId()).getVariable();

      CNF allPredicates = queryHandler.getPredicates();
      // TODO: this might be moved the the FilterAndProject node in issue #510
      CNF edgePredicates = allPredicates.removeSubCNF(edgeVariable);
      Set<String> projectionKeys = allPredicates.getPropertyKeys(edgeVariable);

      boolean isPath = edge.getUpperBound() != 1;

      DataSet<org.gradoop.common.model.impl.pojo.Edge> edges =
        edge.getLabel().equals(GradoopConstants.DEFAULT_EDGE_LABEL) ?
          graph.getEdges() : graph.getEdgesByLabel(edge.getLabel());

      FilterAndProjectEdgesNode node = new FilterAndProjectEdgesNode(edges,
        sourceVariable, edgeVariable, targetVariable, edgePredicates, projectionKeys, isPath);

      PlanTableEntry.Type type = edge.hasVariableLength() ? PATH : EDGE;

      planTable.add(new PlanTableEntry(type, Sets.newHashSet(edgeVariable), allPredicates,
        new QueryPlanEstimator(new QueryPlan(node), queryHandler, graphStatistics)));
    }
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
    Set<String> overlap = firstEntry.getAllVariables();
    overlap.retainAll(secondEntry.getAllVariables());
    return new ArrayList<>(overlap);
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
      assert joinVariables.size() == 1;
      node = createExpandNode(leftEntry, rightEntry, joinVariables.get(0));
    } else {
      node = new JoinEmbeddingsNode(leftEntry.getQueryPlan().getRoot(),
        rightEntry.getQueryPlan().getRoot(), joinVariables, vertexStrategy, edgeStrategy);
    }

    // update processed variables
    HashSet<String> processedVariables = Sets.newHashSet(leftEntry.getProcessedVariables());
    processedVariables.addAll(rightEntry.getProcessedVariables());
    // create resulting predicates
    // TODO: this might be moved to the join/expand node in issue #510
    CNF predicates = mergePredicates(leftEntry, rightEntry);

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
        FilterEmbeddingsNode node = new FilterEmbeddingsNode(entry.getQueryPlan().getRoot(),
          subCNF);
        newTable.add(new PlanTableEntry(GRAPH, Sets.newHashSet(entry.getProcessedVariables()),
          predicates, new QueryPlanEstimator(new QueryPlan(node), queryHandler, graphStatistics)));
      } else {
        newTable.add(entry);
      }
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
          new ArrayList<>(updatedPropertyPairs));
        newTable.add(new PlanTableEntry(GRAPH,
          Sets.newHashSet(entry.getProcessedVariables()), entry.getPredicates(),
          new QueryPlanEstimator(new QueryPlan(node), queryHandler, graphStatistics)));
      } else {
        newTable.add(entry);
      }
    }
    return newTable;
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
  private PlanTable evaluateCartesianProducts(PlanTable currentTable) {
    PlanTable newTable = new PlanTable();
    for (int i = 0; i < currentTable.size(); i++) {
      PlanTableEntry leftEntry = currentTable.get(i);
      for (int j = i + 1; j < currentTable.size(); j++) {
        PlanTableEntry rightEntry = currentTable.get(j);
        CNF joinPredicate = getJoinPredicate(leftEntry, rightEntry);
        if (joinPredicate.size() > 0) {
          newTable.add(createValueJoinEntry(leftEntry, rightEntry, joinPredicate));
        } else {
          // regular join or join with variable length path on source or target vertex
          newTable.add(createCartesianProductEntry(leftEntry, rightEntry));
        }
      }
    }
    return newTable;
  }


  /**
   * Computes the overlapping query variables of the specified entries.
   *
   * @param leftEntry first entry
   * @param rightEntry second entry
   * @return variables that are available in both input entries
   */
  private CNF getJoinPredicate(PlanTableEntry leftEntry, PlanTableEntry rightEntry) {
    Set<String> allVariables = leftEntry.getAllVariables();
    allVariables.addAll(rightEntry.getAllVariables());

    CNF leftPredicates = new CNF(leftEntry.getPredicates());
    CNF rightPredicates = new CNF(rightEntry.getPredicates());
    leftPredicates.removeSubCNF(rightEntry.getProcessedVariables());
    rightPredicates.removeSubCNF(leftEntry.getProcessedVariables());
    CNF predicates = leftPredicates.and(rightPredicates).getSubCNF(allVariables);

    return new CNF(
      predicates.getPredicates()
        .stream()
        .filter(p ->
          p.size() == 1 && p.getPredicates().get(0).getComparator().equals(Comparator.EQ)
        ).collect(Collectors.toList())
    );
  }

  /**
   * Creates an {@link CartesianProductNode} from the specified arguments.
   *
   * @param leftEntry left entry
   * @param rightEntry right entry
   *
   * @return new expand node
   */
  private PlanTableEntry createCartesianProductEntry(PlanTableEntry leftEntry,
    PlanTableEntry rightEntry) {
    CartesianProductNode node = new CartesianProductNode(
      leftEntry.getQueryPlan().getRoot(),
      rightEntry.getQueryPlan().getRoot(),
      vertexStrategy, edgeStrategy
    );

    Set<String> processedVariables = leftEntry.getProcessedVariables();
    processedVariables.addAll(rightEntry.getProcessedVariables());

    CNF predicates = mergePredicates(leftEntry, rightEntry);

    return new PlanTableEntry(
      GRAPH,
      processedVariables,
      predicates,
      new QueryPlanEstimator(new QueryPlan(node), queryHandler, graphStatistics)
    );
  }

  /**
   * Creates an {@link ValueJoinNode} from the specified arguments.
   *
   * @param leftEntry left entry
   * @param rightEntry right entry
   * @param joinPredicate join predicate
   *
   * @return new value join node
   */
  private PlanTableEntry createValueJoinEntry(PlanTableEntry leftEntry,
    PlanTableEntry rightEntry, CNF joinPredicate) {

    List<Pair<String, String>> leftProperties = new ArrayList<>();
    List<Pair<String, String>> rightProperties = new ArrayList<>();

    for (CNFElement e : joinPredicate.getPredicates()) {
      ComparisonExpression comparison = e.getPredicates().get(0);

      Pair<String, String> joinProperty = extractJoinProperty(comparison.getLhs());
      if (leftEntry.getAllVariables().contains(joinProperty.getKey())) {
        leftProperties.add(joinProperty);
      } else {
        rightProperties.add(joinProperty);
      }

      joinProperty = extractJoinProperty(comparison.getRhs());
      if (leftEntry.getAllVariables().contains(joinProperty.getKey())) {
        leftProperties.add(joinProperty);
      } else {
        rightProperties.add(joinProperty);
      }
    }

    ValueJoinNode node = new ValueJoinNode(
      leftEntry.getQueryPlan().getRoot(),
      rightEntry.getQueryPlan().getRoot(),
      leftProperties, rightProperties,
      vertexStrategy, edgeStrategy
    );

    Set<String> processedVariables = leftEntry.getProcessedVariables();
    processedVariables.addAll(rightEntry.getProcessedVariables());

    CNF predicates = mergePredicates(leftEntry, rightEntry);

    return new PlanTableEntry(
      GRAPH,
      processedVariables,
      predicates,
      new QueryPlanEstimator(new QueryPlan(node), queryHandler, graphStatistics)
    );
  }

  /**
   * Turns a QueryComparable into a {@code Pair<Variable, PropertyKey>}
   * @param comparable query comparable
   * @return join property
   */
  private Pair<String, String> extractJoinProperty(QueryComparable comparable) {
    if (comparable instanceof PropertySelectorComparable) {
      PropertySelectorComparable propertySelector = (PropertySelectorComparable) comparable;
      return Pair.of(propertySelector.getVariable(), propertySelector.getPropertyKey());
    } else {
      //TODO #580 Include ElementSelector -> ID needs to be projected as property
      throw new RuntimeException("Comparable " + comparable + "cant be used for ValueJoin");
    }
  }

  /**
   * Creates a new predicate that includes only elements that exist in both input predicates
   *
   * @param leftEntry left side plant table entry
   * @param rightEntry right side plan table entry
   * @return Merged predicates
   */
  private CNF mergePredicates(PlanTableEntry leftEntry, PlanTableEntry rightEntry) {
    CNF leftPredicates = new CNF(leftEntry.getPredicates());
    CNF rightPredicates = new CNF(rightEntry.getPredicates());
    leftPredicates.removeSubCNF(rightEntry.getProcessedVariables());
    rightPredicates.removeSubCNF(leftEntry.getProcessedVariables());
    return leftPredicates.and(rightPredicates);
  }
}
