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
package org.gradoop.flink.algorithms.fsm.transactional.basic;

import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.operators.UnaryCollectionToCollectionOperator;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;

@RunWith(Parameterized.class)
public abstract class BasicPatternsTransactionalFSMTestBase extends GradoopFlinkTestBase {

  private final String testName;

  private final String asciiGraphs;

  private final String[] searchSpaceVariables;

  private final String[] expectedResultVariables;

  public BasicPatternsTransactionalFSMTestBase(String testName, String asciiGraphs,
    String searchSpaceVariables, String expectedResultVariables) {
    this.testName = testName;
    this.asciiGraphs = asciiGraphs;
    this.searchSpaceVariables = searchSpaceVariables.split(",");
    this.expectedResultVariables = expectedResultVariables.split(",");
  }

  public abstract UnaryCollectionToCollectionOperator getImplementation();

  @Parameterized.Parameters(name = "{index}: {0}")
  public static Iterable data() {
    return Arrays.asList(
      new String[] {
        "Single_Edge",
        BasicPatternsData.FSM_SINGLE_EDGE,
        "g1,g2,g3,g4",
        "s1"
      },
      new String[] {
        "Simple_Graph",
        BasicPatternsData.FSM_SIMPLE_GRAPH,
        "g1,g2,g3",
        "s1,s2,s3,s4,s5"
      },
      new String[] {
        "Parallel_Edges",
        BasicPatternsData.FSM_PARALLEL_EDGES,
        "g1,g2,g3",
        "s1,s2"
      },
      new String[] {
        "Loop",
        BasicPatternsData.FSM_LOOP,
        "g1,g2,g3,g4",
        "s1,s2,s3"
      },
      new String[] {
        "Diamond",
        BasicPatternsData.FSM_DIAMOND,
        "g1,g2,g3",
        "s1,s2,s3,s4,s5,s6,s7"
      },
      new String[] {
        "Circle_with_Branch",
        BasicPatternsData.FSM_CIRCLE_WITH_BRANCH,
        "g1,g2,g3",
        "s1,s2,s3,s4,s5,s6,s7,s8,s9,s10"
      },
      new String[] {
        "Colored Circle",
        BasicPatternsData.MULTI_LABELED_CIRCLE,
        "g1,g2",
        "s1,s2,s3,s4,s5,s6,s7"
      }
    );
  }

  @Test
  public void testGraphElementEquality() throws Exception {

    FlinkAsciiGraphLoader loader = getLoaderFromString(asciiGraphs);

    GraphCollection searchSpace =
      loader.getGraphCollectionByVariables(searchSpaceVariables);

    GraphCollection expectation =
      loader.getGraphCollectionByVariables(expectedResultVariables);

    GraphCollection result = getImplementation().execute(searchSpace);

    collectAndAssertTrue(result.equalsByGraphElementData(expectation));
  }
}
