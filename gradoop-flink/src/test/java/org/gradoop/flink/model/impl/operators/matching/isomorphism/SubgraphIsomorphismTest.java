package org.gradoop.flink.model.impl.operators.matching.isomorphism;

import org.gradoop.flink.model.impl.operators.matching.PatternMatchingTest;
import org.junit.runners.Parameterized;

import java.util.Arrays;

import static org.gradoop.flink.model.impl.operators.matching.TestData.*;

/**
 * Test data for pattern matching tests. The graphs are visualized in
 * dev-support/pattern_matching_testcases.pdf
 */
public abstract class SubgraphIsomorphismTest extends PatternMatchingTest {

  public SubgraphIsomorphismTest(String testName, String dataGraph,
    String queryGraph, String[] expectedGraphVariables,
    String expectedCollection) {
    super(testName, dataGraph, queryGraph, expectedGraphVariables,
      expectedCollection);
  }

  @Parameterized.Parameters(name = "{index}: {0}")
  public static Iterable data() {
    return Arrays.asList(new Object[][]{
      {
        "Graph1_Path0",
        GRAPH_1,
        PATH_PATTERN_0,
        new String[] {"expected1", "expected2", "expected3"},
        "expected1[" +
          "(v1)-[e2]->(v6)" +
        "]" +
        "expected2[" +
          "(v2)-[e3]->(v6)" +
        "]" +
        "expected3[" +
          "(v5)-[e6]->(v4)" +
        "]"
      },
      {
        "Graph1_Path2",
        GRAPH_1,
        PATH_PATTERN_2,
        new String[] {"expected1", "expected2", "expected3"},
        "expected1[" +
          "(v1)" +
        "]" +
        "expected2[" +
          "(v2)" +
        "]" +
        "expected3[" +
          "(v5)" +
        "]"
      },
      {
        "Graph2_Path3",
        GRAPH_2,
        PATH_PATTERN_3,
        new String[] {"expected1"},
        "expected1[ ]"
      },
      {
        "Graph2_Loop0",
        GRAPH_2,
        LOOP_PATTERN_0,
        new String[] {"expected1"},
        "expected1[" +
          "(v9)-[e15]->(v9)" +
        "]"
      },
      {
        "Graph1_Cycle2",
        GRAPH_1,
        CYCLE_PATTERN_2,
        new String[]{"expected1"},
        "expected1[" +
          "(v2)-[e3]->(v6)" +
          "(v6)-[e7]->(v2)" +
          "(v6)-[e9]->(v7)" +
        "]"
      },
      {
        "Graph1_Cycle4",
        GRAPH_1,
        CYCLE_PATTERN_4,
        new String[] {"expected1", "expected2"},
        "expected1[" +
          "(v1)-[e2]->(v6)-[e8]->(v5)-[e6]->(v4)-[e4]->(v1)" +
        "]" +
        "expected2[" +
          "(v5)-[e6]->(v4)-[e4]->(v1)-[e2]->(v6)-[e8]->(v5)" +
        "]"
      },
      {
        "Graph2_Cycle5",
        GRAPH_2,
        CYCLE_PATTERN_5,
        new String[]{"expected1", "expected2"},
        "expected1[" +
          "(v0)-[e1]->(v4)<-[e2]-(v0)" +
        "]" +
        "expected2[" +
          "(v5)-[e9]->(v4)<-[e10]-(v5)" +
        "]"
      },
      {
        "Graph4_Path4",
        GRAPH_4,
        PATH_PATTERN_4,
        new String[] {"expected1", "expected2", "expected3", "expected4"},
        "expected1[" +
          "(v0)-[e0]->(v1)-[e1]->(v2)" +
        "]" +
        "expected2[" +
          "(v0)-[e0]->(v1)-[e2]->(v2)" +
        "]" +
        "expected3[" +
          "(v1)-[e1]->(v2)-[e3]->(v3)" +
        "]" +
        "expected4[" +
          "(v1)-[e2]->(v2)-[e3]->(v3)" +
        "]"
      }
    });
  }
}