package org.gradoop.flink.model.impl.operators.matching.single.cypher;

import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.operators.matching.common.MatchStrategy;
import org.gradoop.flink.model.impl.operators.matching.common.query.QueryHandler;
import org.gradoop.flink.model.impl.operators.matching.common.statistics.GraphStatistics;
import org.gradoop.flink.model.impl.operators.matching.common.statistics.GraphStatisticsLocalFSReader;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.planner.greedy.GreedyPlanner;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.planning.plantable.PlanTableEntry;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

public class CypherTest extends GradoopFlinkTestBase {

  @Test
  public void foo() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();
    LogicalGraph graph = loader.getDatabase().getDatabaseGraph();

    String path = CypherTest.class.getResource("/data/json/sna/statistics").getPath();
    GraphStatistics graphStatistics = GraphStatisticsLocalFSReader.read(path);

    String query1 = "MATCH (t:Tag)<-[:hasTag]-(f:Forum)-[:hasMember]->(p:Person) " +
      "WHERE t.name = \"Databases\"";
    String query2 = "MATCH (p1:Person)-[e:knows]->(p2:Person)<-[:hasMember]-(f:Forum)-[:hasMember]->(p3:Person) " +
      "WHERE p1.yob > e.since";

    String query = query2;

    System.out.println("query = " + query);

    QueryHandler queryHandler = new QueryHandler(query);

    GreedyPlanner planner = new GreedyPlanner(graph, queryHandler, graphStatistics,
      MatchStrategy.ISOMORPHISM, MatchStrategy.ISOMORPHISM);

    PlanTableEntry plan = planner.plan();

    System.out.println(plan);
  }
}
