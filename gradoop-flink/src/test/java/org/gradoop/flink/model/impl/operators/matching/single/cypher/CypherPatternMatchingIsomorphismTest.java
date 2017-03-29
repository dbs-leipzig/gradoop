package org.gradoop.flink.model.impl.operators.matching.single.cypher;

import com.google.common.collect.Lists;
import org.gradoop.flink.model.impl.operators.matching.common.MatchStrategy;
import org.gradoop.flink.model.impl.operators.matching.common.statistics.GraphStatistics;
import org.gradoop.flink.model.impl.operators.matching.single.PatternMatching;
import org.gradoop.flink.model.impl.operators.matching.single.SubgraphIsomorphismTest;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.List;

import static org.gradoop.flink.model.impl.operators.matching.TestData.*;

/**
 * Runs test for vertex and edge isomorphism.
 */
public class CypherPatternMatchingIsomorphismTest extends SubgraphIsomorphismTest {

  @Parameterized.Parameters(name = "{index}: {0}")
  public static Iterable data() {
    List<String[]> data = Lists.newArrayList(SubgraphIsomorphismTest.data());
    data.add(new String[] {
      "Graph1_VarLength0",
      GRAPH_1, VAR_LENGTH_PATH_PATTERN_0,
      "expected1,expected2,expected3,expected4",
      "expected1[(v4)-[e4]->(v1)-[e2]->(v6)]" +
      "expected2[(v6)-[e8]->(v5)-[e6]->(v4)]" +
      "expected3[(v0)-[e0]->(v1)-[e2]->(v6)]" +
      "expected4[(v8)-[e10]->(v5)-[e6]->(v4)]"
    });
    data.add(new String[] {
      "Graph1_VarLength1",
      GRAPH_1, VAR_LENGTH_PATH_PATTERN_1,
      "expected1,expected2,expected3,expected4",
      "expected1[(v4)-[e4]->(v1)-[e2]->(v6)]" +
      "expected2[(v6)-[e8]->(v5)-[e6]->(v4)]" +
      "expected3[(v0)-[e0]->(v1)-[e2]->(v6)]" +
      "expected4[(v8)-[e10]->(v5)-[e6]->(v4)]"
    });
    data.add(new String[] {
      "Graph2_VarLength2",
      GRAPH_2, VAR_LENGTH_PATH_PATTERN_2,
      "expected1",
      "expected1[]"
    });
    data.add(new String[] {
      "Graph3_VarLength3",
      GRAPH_3, VAR_LENGTH_PATH_PATTERN_3,
      "expected1,expected2,expected3,expected4,expected5,expected6," +
        "expected7,expected8,expected9,expected10",
      "expected1[(v0)]" +
        "expected2[(v0)-[e0]->(v1)]" +
        "expected3[(v0)-[e0]->(v1)-[e1]->(v2)]" +
        "expected4[(v0)-[e0]->(v1)-[e1]->(v2)-[e2]->(v3)]" +
        "expected5[(v1)]" +
        "expected6[(v1)-[e1]->(v2)]" +
        "expected7[(v1)-[e1]->(v2)-[e2]->(v3)]" +
        "expected8[(v2)]" +
        "expected9[(v2)-[e2]->(v3)]" +
        "expected10[(v3)]"
    });
    data.add(new String[] {
      "Graph2_VarLength4",
      GRAPH_2, VAR_LENGTH_PATH_PATTERN_4,
      "expected1",
      "expected1[(v2)-[e8]->(v6)-[e7]->(v2)]"
    });
    return data;
  }

  public CypherPatternMatchingIsomorphismTest(String testName, String dataGraph, String queryGraph,
    String expectedGraphVariables, String expectedCollection) {
    super(testName, dataGraph, queryGraph, expectedGraphVariables, expectedCollection);
  }

  @Override
  public PatternMatching getImplementation(String queryGraph, boolean attachData) {
    int n = 42; // just used for testing
    return new CypherPatternMatching("MATCH " + queryGraph, attachData,
      MatchStrategy.ISOMORPHISM, MatchStrategy.ISOMORPHISM,
      new GraphStatistics(n, n, n, n));
  }
}
