package org.gradoop.flink.model.impl.operators.matching.simulation.dual;

import org.gradoop.flink.model.impl.operators.matching.PatternMatchingTest;
import org.junit.runners.Parameterized;

import java.util.Arrays;

import static org.gradoop.flink.model.impl.operators.matching.TestData.*;

public abstract class DualSimulationTest extends PatternMatchingTest {

  public DualSimulationTest(String testName, String dataGraph,
    String queryGraph, String[] expectedGraphVariables,
    String expectedCollection) {
    super(testName, dataGraph, queryGraph, expectedGraphVariables,
      expectedCollection);
  }

  @Parameterized.Parameters(name = "{index}: {0}")
  public static Iterable data() {
    return Arrays.asList(new Object[][] {
      {
        "Graph1_Path0",
        GRAPH_1,
        PATH_PATTERN_0,
        new String[] {"expected"},
        "expected[" +
          "(v1)-[e2]->(v6)" +
          "(v5)-[e6]->(v4)" +
          "(v2)-[e3]->(v6)" +
        "]"
      },
      {
        "Graph1_Path2",
        GRAPH_1,
        PATH_PATTERN_2,
        new String[] {"expected"},
        "expected[" +
          "(v1);(v2);(v5)" +
        "]"
      },
      {
        "Graph2_Path3",
        GRAPH_2,
        PATH_PATTERN_3,
        new String[] {"expected"},
        "expected[" +
          "(v9)-[e15]->(v9)" +
        "]"
      },
      {
        "Graph2_Loop0",
        GRAPH_2,
        LOOP_PATTERN_0,
        new String[] {"expected"},
        "expected[" +
          "(v9)-[e15]->(v9)" +
        "]"
      },
      {
        "Graph1_Cycle0",
        GRAPH_1,
        CYCLE_PATTERN_0,
        new String[] {"expected"},
        "expected[" +
          "(v1)-[e2]->(v6)" +
          "(v6)-[e8]->(v5)" +
          "(v5)-[e6]->(v4)" +
          "(v4)-[e4]->(v1)" +
          "(v6)-[e7]->(v2)" +
          "(v2)-[e3]->(v6)" +
        "]"
      },
      {
        "Graph2_Cycle1",
        GRAPH_2,
        CYCLE_PATTERN_1,
        new String[] {"expected"},
        "expected[" +
          "(v9)-[e15]->(v9)" +
        "]"
      },
      {
        "Graph1_Cycle2",
        GRAPH_1,
        CYCLE_PATTERN_2,
        new String[] {"expected"},
        "expected[" +
          "(v1)-[e2]->(v6)" +
          "(v2)-[e3]->(v6)" +
          "(v4)-[e4]->(v1)" +
          "(v4)-[e5]->(v3)" +
          "(v5)-[e6]->(v4)" +
          "(v6)-[e7]->(v2)" +
          "(v6)-[e8]->(v5)" +
          "(v6)-[e9]->(v7)" +
        "]"
      },
      {
        "Graph2_Cycle3",
        GRAPH_2,
        CYCLE_PATTERN_3,
        new String[] {"expected"},
        "expected[" +
          "(v1)-[e0]->(v0)" +
          "(v0)-[e1]->(v4)" +
          "(v0)-[e2]->(v4)" +
          "(v0)-[e3]->(v3)" +
          "(v3)-[e4]->(v5)" +
          "(v5)-[e5]->(v1)" +
          "(v1)-[e6]->(v6)" +
          "(v6)-[e7]->(v2)" +
          "(v2)-[e8]->(v6)" +
          "(v5)-[e9]->(v4)" +
          "(v5)-[e10]->(v4)" +
          "(v6)-[e11]->(v7)" +
          "(v8)-[e12]->(v7)" +
          "(v10)-[e13]->(v5)" +
          "(v6)-[e14]->(v10)" +
        "]"
      },
      {
        "Graph1_Cycle4",
        GRAPH_1,
        CYCLE_PATTERN_4,
        new String[] {"expected"},
        "expected[" +
          "(v1)-[e2]->(v6)" +
          "(v6)-[e8]->(v5)" +
          "(v5)-[e6]->(v4)" +
          "(v4)-[e4]->(v1)" +
          "(v6)-[e7]->(v2)" +
          "(v2)-[e3]->(v6)" +
        "]"
      },
      {
        "Graph3_Path1",
        GRAPH_3,
        PATH_PATTERN_1,
        new String[] {"expected"},
        "expected[" +
          "(v0)-[e0]->(v1)" +
          "(v1)-[e1]->(v2)" +
          "(v2)-[e2]->(v3)" +
        "]"
      },
      {
        "Graph1_Tree0",
        GRAPH_1,
        TREE_PATTERN_0,
        new String[] {"expected"},
        "expected[]"
      }
    });
  }
}
