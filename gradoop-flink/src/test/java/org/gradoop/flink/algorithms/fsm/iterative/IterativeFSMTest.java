package org.gradoop.flink.algorithms.fsm.iterative;

import org.gradoop.flink.algorithms.fsm.FSMTestBase;
import org.junit.runners.Parameterized;

import java.util.Arrays;

import static org.gradoop.flink.algorithms.fsm.TestData.*;


public abstract class IterativeFSMTest extends FSMTestBase {

  IterativeFSMTest(String testName, String dataGraph, String searchSpace,
    String expectedCollection) {
    super(testName, dataGraph, searchSpace, expectedCollection);
  }

  @Parameterized.Parameters(name = "{index}: {0}")
  public static Iterable data() {
    return Arrays.asList(
      new String[] {
        "Single_Edge",
        FSM_SINGLE_EDGE,
        "g1,g2,g3,g4",
        "s1"
      },
      new String[] {
        "Simple_Graph",
        FSM_SIMPLE_GRAPH,
        "g1,g2,g3",
        "s1,s2,s3,s4,s5"
      },
      new String[] {
        "Parallel_Edges",
        FSM_PARALLEL_EDGES,
        "g1,g2,g3",
        "s1,s2"
      },
      new String[] {
        "Loop",
        FSM_LOOP,
        "g1,g2,g3,g4",
        "s1,s2,s3"
      },
      new String[] {
        "Diamond",
        FSM_DIAMOND,
        "g1,g2,g3",
        "s1,s2,s3,s4,s5,s6,s7"
      },
      new String[] {
        "Circle_with_Branch",
        FSM_CIRCLE_WITH_BRANCH,
        "g1,g2,g3",
        "s1,s2,s3,s4,s5,s6,s7,s8,s9,s10"
      }
    );
  }
}
