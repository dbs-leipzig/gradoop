package org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.planner;

import com.google.common.collect.Sets;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.operators.matching.common.MatchStrategy;
import org.gradoop.flink.model.impl.operators.matching.common.query.QueryHandler;
import org.gradoop.flink.model.impl.operators.matching.common.query.predicates.CNF;
import org.gradoop.flink.model.impl.operators.matching.common.statistics.GraphStatistics;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.EmbeddingMetaData;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.estimation.Estimator;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.estimation.FilterElementsEstimator;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.estimation.FilterEmbeddingsEstimator;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.estimation.JoinEmbeddingsEstimator;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.plantable.PlanTable;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.plantable.PlanTableEntry;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.plantable.PlanTableEntryType;
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

public class GreedyPlanner {

  private final LogicalGraph graph;

  private final QueryHandler queryHandler;

  private final GraphStatistics graphStatistics;

  private final MatchStrategy vertexStrategy;

  private final MatchStrategy edgeStrategy;

  public GreedyPlanner(LogicalGraph graph, QueryHandler queryHandler,
    GraphStatistics graphStatistics, MatchStrategy vertexStrategy, MatchStrategy edgeStrategy) {
    this.graph = graph;
    this.queryHandler = queryHandler;
    this.graphStatistics = graphStatistics;
    this.vertexStrategy = vertexStrategy;
    this.edgeStrategy = edgeStrategy;
  }

  public PlanTableEntry plan() {
    PlanTable initialPlans = initPlanTable();

    while(initialPlans.size() > 1) {

      System.out.println("Initial plans");
      System.out.println();
      initialPlans.forEach(System.out::println);

      PlanTable newPlans = evaluateJoins(initialPlans);

      System.out.println("New plans (after JoinEmbeddings Evaluation)");
      System.out.println();
      newPlans.forEach(System.out::println);

      newPlans = evaluateFilter(newPlans);

      System.out.println("New plans (after FilterEmbeddings Evaluation)");
      System.out.println();
      newPlans.forEach(System.out::println);

      PlanTableEntry bestEntry = newPlans.min();

      System.out.println("Minimum plan table entry");
      System.out.println();
      System.out.println(bestEntry);

      initialPlans.removeProcessedBy(bestEntry);
      initialPlans.add(bestEntry);

      System.out.println("Updated initial plan table");
      System.out.println();
      initialPlans.forEach(System.out::println);
    }

    System.out.println("Final plan");
    System.out.println();
    System.out.println(initialPlans.get(0));

    return initialPlans.get(0);
  }

  //----------------------------------------------------------------------------------------------
  // Initialization
  //----------------------------------------------------------------------------------------------

  /**
   * Creates the initial plan table entries according to the specified vertices and edges.
   *
   * @return initial plan table
   */
  private PlanTable initPlanTable() {
    PlanTable planTable = new PlanTable();

    for (Vertex vertex : queryHandler.getVertices()) {
      String variable = vertex.getVariable();
      CNF subCNF = queryHandler.getPredicates().getSubCNF(variable);
      Estimator estimator = new FilterElementsEstimator(graphStatistics, variable, EmbeddingMetaData.EntryType.VERTEX, subCNF);
      FilterAndProjectVerticesNode node = new FilterAndProjectVerticesNode(graph.getVertices(),
        vertex.getVariable(), subCNF, queryHandler.getPredicates().getPropertyKeys(vertex.getVariable()));
      planTable.add(new PlanTableEntry(PlanTableEntryType.VERTEX, Sets
        .newHashSet(variable), new QueryPlan(node), estimator));
    }

    for (Edge edge : queryHandler.getEdges()) {
      String sourceVariable = queryHandler.getVertexById(edge.getSourceVertexId()).getVariable();
      String targetVariable = queryHandler.getVertexById(edge.getTargetVertexId()).getVariable();
      String edgeVariable = edge.getVariable();
      CNF subCNF = queryHandler.getPredicates().getSubCNF(edgeVariable);
      Estimator estimator = new FilterElementsEstimator(graphStatistics, edgeVariable, EmbeddingMetaData.EntryType.EDGE, subCNF);
      FilterAndProjectEdgesNode node = new FilterAndProjectEdgesNode(graph.getEdges(),
        sourceVariable, edgeVariable, targetVariable,
        subCNF, queryHandler.getPredicates().getPropertyKeys(edgeVariable));
      planTable.add(new PlanTableEntry(PlanTableEntryType.EDGE, Sets.newHashSet(edgeVariable), new QueryPlan(node), estimator));
    }

    return planTable;
  }

  //----------------------------------------------------------------------------------------------
  // Join evaluation
  //----------------------------------------------------------------------------------------------

  private PlanTable evaluateJoins(PlanTable currentTable) {
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
    return entry.getType() == PlanTableEntryType.VERTEX ||
      entry.getType() == PlanTableEntryType.PATH;
  }

  private List<String> getOverlap(PlanTableEntry leftEntry, PlanTableEntry rightEntry) {
    return leftEntry.getAllVariables().stream()
      .filter(var -> rightEntry.getAllVariables().contains(var))
      .collect(Collectors.toList());
  }

  private PlanTableEntry joinEntries(PlanTableEntry leftEntry, PlanTableEntry rightEntry, List<String> joinVariables) {
    JoinEmbeddingsEstimator estimator = new JoinEmbeddingsEstimator(graphStatistics,
      Sets.newHashSet(joinVariables),
      leftEntry.getEstimatedCardinality(),
      rightEntry.getEstimatedCardinality(),
      leftEntry.getQueryPlan().getRoot().getEmbeddingMetaData(),
      rightEntry.getQueryPlan().getRoot().getEmbeddingMetaData());

    JoinEmbeddingsNode node = new JoinEmbeddingsNode(leftEntry.getQueryPlan().getRoot(),
      rightEntry.getQueryPlan().getRoot(), joinVariables, vertexStrategy, edgeStrategy);

    HashSet<String> evaluatedVars = Sets.newHashSet(leftEntry.getProcessedVariables());
    evaluatedVars.addAll(rightEntry.getProcessedVariables());

    return new PlanTableEntry(PlanTableEntryType.PATH, evaluatedVars, new QueryPlan(node), estimator);
  }

  //----------------------------------------------------------------------------------------------
  // Filter embedding evaluation
  //----------------------------------------------------------------------------------------------

  private PlanTable evaluateFilter(PlanTable currentTable) {
    PlanTable newTable = new PlanTable();

    for (PlanTableEntry entry : currentTable) {
      Set<String> variables = Sets.newHashSet(entry.getAttributedVariables());
      CNF subCNF = queryHandler.getPredicates().getSubCNF(variables);
      if (subCNF.size() > 0) {
        Estimator estimator = new FilterEmbeddingsEstimator(graphStatistics, variables, subCNF,
          entry.getEstimatedCardinality(), entry.getQueryPlan().getRoot().getEmbeddingMetaData());
        FilterEmbeddingsNode
          node = new FilterEmbeddingsNode(entry.getQueryPlan().getRoot(), subCNF);
        newTable.add(new PlanTableEntry(PlanTableEntryType.PATH, Sets.newHashSet(entry.getProcessedVariables()), new QueryPlan(node), estimator));
      }
      newTable.add(entry);
    }

    return newTable;
  }

  //----------------------------------------------------------------------------------------------
  // Project embedding evaluation
  //----------------------------------------------------------------------------------------------
}
