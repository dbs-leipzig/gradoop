package org.gradoop.flink.model.impl.operators.matching.single.cypher;

import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.matching.common.query.QueryHandler;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;
import org.s1ck.gdl.model.Vertex;

public class CypherGraphConstructionTest extends GradoopFlinkTestBase {

  @Test
  public void parsingExample() {
    String pattern = "(b)<-[e0]-(a)-[:possible_friend]->(c)<-[e1]-(b)";

    QueryHandler queryHandler = new QueryHandler(pattern);

    Vertex a = queryHandler.getVertexByVariable("a");

    System.out.println(a);
  }

  @Test
  public void testEdgeConstruction() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    LogicalGraph dbGraph = loader.getDatabase().getDatabaseGraph();

    loader.appendToDatabaseFromString("expected0[" +
        "(alice)-[akb]->(bob)-[bkc]->(carol)," +
        "(alice)-[:possible_friend]->(carol)" +
      "]," +
      "expected1[" +
        "(bob)-[bkc]->(carol)-[ckd]->(dave)," +
        "(bob)-[:possible_friend]->(dave)" +
      "]");

    GraphCollection result = dbGraph.cypher(
      "MATCH (a:Person)-[e0:knows]->(b:Person)-[e1:knows]->(c:Person) " +
        "WHERE a.city = 'Leipzig' AND a <> c",
      "(b)<-[e0]-(a)-[e_new:possible_friend]->(c)<-[e1]-(b)");

    GraphCollection expectedCollection = loader
      .getGraphCollectionByVariables("expected0", "expected1");

    collectAndAssertTrue(result.equalsByGraphElementData(expectedCollection));
  }

  @Test
  public void testEdgeConstructionReducedPattern() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    LogicalGraph dbGraph = loader.getDatabase().getDatabaseGraph();

    loader.appendToDatabaseFromString("expected0[" +
        "(alice)-[:possible_friend]->(carol)" +
      "]," +
      "expected1[" +
        "(bob)-[:possible_friend]->(dave)" +
      "]");

    GraphCollection result = dbGraph.cypher(
      "MATCH (a:Person)-[:knows]->(b:Person)-[:knows]->(c:Person) " +
        "WHERE a.city = 'Leipzig' AND a <> c",
      "(a)-[e_new:possible_friend]->(c)");

    GraphCollection expectedCollection = loader
      .getGraphCollectionByVariables("expected0", "expected1");

    collectAndAssertTrue(result.equalsByGraphElementData(expectedCollection));
  }

  @Test
  public void testEdgeConstructionExtendedPattern() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    LogicalGraph dbGraph = loader.getDatabase().getDatabaseGraph();

    loader.appendToDatabaseFromString("expected0[" +
            "(alice)-[:possible_friend]->(:possible_person)-[:possible_friend]->(carol)" +
            "]," +
            "expected1[" +
            "(bob)-[:possible_friend]->(:possible_person)-[:possible_friend]->(dave)" +
            "]");

    GraphCollection result = dbGraph.cypher(
            "MATCH (a:Person)-[:knows]->(b:Person)-[:knows]->(c:Person) " +
                    "WHERE a.city = 'Leipzig' AND a <> c",
            "(a)-[e_new:possible_friend]->(v_new:possible_person)-[e_new2:possible_friend]->(c)");

    GraphCollection expectedCollection = loader
            .getGraphCollectionByVariables("expected0", "expected1");

    collectAndAssertTrue(result.equalsByGraphElementData(expectedCollection));
  }
}
