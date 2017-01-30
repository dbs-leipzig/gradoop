package org.gradoop.flink.model.impl.operators.matching.single.cypher;

import org.apache.flink.api.java.DataSet;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.model.impl.operators.matching.common.MatchStrategy;
import org.gradoop.flink.model.impl.operators.matching.common.query.QueryHandler;
import org.gradoop.flink.model.impl.operators.matching.common.statistics.GraphStatistics;
import org.gradoop.flink.model.impl.operators.matching.common.statistics.GraphStatisticsLocalFSReader;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.Embedding;
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
    String query3 = "MATCH (p1:Person)-[:knows]->(p2:Person)-[:knows]->(p1)";
    String query4 = "MATCH (p1:Person)-[:knows]->(p2:Person)-[:knows]->(p1)<-[:knows]-(p3:Person)-[:knows]->(p2)";
    String query5 = "MATCH (p1:Person)-[e1:knows*1..2]->(p2:Person)<-[e2:hasMember]-(f:Forum)-[e3:hasModerator]->(p1)";
    String query6 = "MATCH (t:Tag)<-[:hasTag]-(f:Forum)-[:hasMember]->(p:Person) " +
      "WHERE (t.name = \"Databases\" OR p.name = \"Alice\") AND f.title = \"Graph Databases\"";

    String query = query6;

    System.out.printf("query = %s%n%n", query);

    QueryHandler queryHandler = new QueryHandler(query);

    GreedyPlanner planner = new GreedyPlanner(graph, queryHandler, graphStatistics,
      MatchStrategy.ISOMORPHISM, MatchStrategy.ISOMORPHISM);

    PlanTableEntry planTableEntry = planner.plan();

    System.out.println(planTableEntry);

//    DataSet<Embedding> result = planTableEntry.getQueryPlan().execute();
//    result.print();
  }
}
