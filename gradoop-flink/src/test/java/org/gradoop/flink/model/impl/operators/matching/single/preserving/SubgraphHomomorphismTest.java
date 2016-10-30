package org.gradoop.flink.model.impl.operators.matching.single.preserving;

import org.gradoop.flink.model.impl.operators.matching.single.PatternMatchingTest;
import org.junit.runners.Parameterized;

import java.util.Arrays;

import static org.gradoop.flink.model.impl.operators.matching.single.TestData.*;

/**
 * Test data for pattern matching tests. The graphs are visualized in
 * dev-support/pattern_matching_testcases.pdf
 */
public abstract class SubgraphHomomorphismTest extends PatternMatchingTest {

  public SubgraphHomomorphismTest(String testName, String dataGraph,
    String queryGraph, String[] expectedGraphVariables,
    String expectedCollection) {
    super(testName, dataGraph, queryGraph, expectedGraphVariables,
      expectedCollection);
  }

  @Parameterized.Parameters(name = "{index}: {0}")
  public static Iterable data() {
    return Arrays.asList(new Object[][]{
      {
        "Graph1_Chain0",
        GRAPH_1, CHAIN_PATTERN_0,
        new String[] {"expected1", "expected2", "expected3"},
        "expected1[(v1)-[e2]->(v6)]" +
        "expected2[(v2)-[e3]->(v6)]" +
        "expected3[(v5)-[e6]->(v4)]"
      },
      {
        "Graph1_Chain2",
        GRAPH_1, CHAIN_PATTERN_2,
        new String[] {"expected1", "expected2", "expected3"},
        "expected1[(v1)]" +
        "expected2[(v2)]" +
        "expected3[(v5)]"
      },
      {
        "Graph2_Chain3",
        GRAPH_2, CHAIN_PATTERN_3,
        new String[] {"expected1"},
        "expected1[(v9)-[e15]->(v9)]"
      },
      {
        "Graph2_Loop0",
        GRAPH_2,
        LOOP_PATTERN_0,
        new String[] {"expected1"},
        "expected1[(v9)-[e15]->(v9)]"
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
        new String[] {"expected1", "expected2","expected3"},
        "expected1[(v1)-[e2]->(v6)-[e8]->(v5)-[e6]->(v4)-[e4]->(v1)]" +
        "expected2[(v5)-[e6]->(v4)-[e4]->(v1)-[e2]->(v6)-[e8]->(v5)]" +
        "expected3[(v2)-[e3]->(v6)-[e7]->(v2)-[e3]->(v6)-[e7]->(v2)]"
      },
      {
        "Graph2_Cycle5",
        GRAPH_2,
        CYCLE_PATTERN_5,
        new String[]{"expected1", "expected2"},
        "expected1[(v0)-[e1]->(v4)<-[e2]-(v0)]" +
        "expected2[(v5)-[e9]->(v4)<-[e10]-(v5)]"
      },
      {
        "Graph4_Chain4",
        GRAPH_4, CHAIN_PATTERN_4,
        new String[] {"expected1", "expected2", "expected3", "expected4"},
        "expected1[(v0)-[e0]->(v1)-[e1]->(v2)]" +
        "expected2[(v0)-[e0]->(v1)-[e2]->(v2)]" +
        "expected3[(v1)-[e1]->(v2)-[e3]->(v3)]" +
        "expected4[(v1)-[e2]->(v2)-[e3]->(v3)]"
      },
      {
        "Graph5_Chain6",
        GRAPH_5,
        CHAIN_PATTERN_6,
        new String[] {"expected1"},
        "expected1[(v0)-[e0]->(v1)]"
      },
      {
        "Graph5_Chain7",
        GRAPH_5,
        CHAIN_PATTERN_7,
        new String[] {"expected1"},
        "expected1[(v0)-[e0]->(v1)]"
      },
      {
        "Graph3_Unlabeled0",
        GRAPH_3,
        UNLABELED_PATTERN_0,
        new String[] {"expected1", "expected2", "expected3", "expected4"},
        "expected1[(v0)]" +
        "expected2[(v1)]" +
        "expected3[(v2)]" +
        "expected4[(v3)]"
      },
      {
        "Graph3_Unlabeled1",
        GRAPH_3,
        UNLABELED_PATTERN_1,
        new String[] {"expected1", "expected2", "expected3"},
        "expected1[(v0)-[e0]->(v1)]" +
        "expected2[(v1)-[e1]->(v2)]" +
        "expected3[(v2)-[e2]->(v3)]"
      },
      {
        "Graph1_Unlabeled2",
        GRAPH_1,
        UNLABELED_PATTERN_2,
        new String[] {"expected1", "expected2"},
        "expected1[(v4)-[e5]->(v3)]" +
        "expected2[(v6)-[e9]->(v7)]"
      },
      {
        "Graph1_Unlabeled3",
        GRAPH_1,
        UNLABELED_PATTERN_3,
        new String[] {"expected1", "expected2", "expected3"},
        "expected1[(v1)-[e2]->(v6)]" +
        "expected2[(v5)-[e6]->(v4)]" +
        "expected3[(v2)-[e3]->(v6)]"
      }
    });
  }
}