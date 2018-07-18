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
package org.gradoop.flink.model.impl.operators.matching.single;

import org.junit.runners.Parameterized;

import java.util.Arrays;

import static org.gradoop.flink.model.impl.operators.matching.TestData.*;

public abstract class SubgraphIsomorphismTest extends PatternMatchingWithBindingTest {

  public SubgraphIsomorphismTest(String testName, String dataGraph,
    String queryGraph, String expectedGraphVariables,
    String expectedCollection) {
    super(testName, dataGraph, queryGraph, expectedGraphVariables,
      expectedCollection);
  }

  @Parameterized.Parameters(name = "{index}: {0}")
  public static Iterable<String[]> data() {
    return Arrays.asList(
      new String[] {
        "Graph1_Chain0",
        GRAPH_1, CHAIN_PATTERN_0,
        "expected1,expected2,expected3",
        "expected1[(v1)-[e2]->(v6)]" +
        "expected2[(v2)-[e3]->(v6)]" +
        "expected3[(v5)-[e6]->(v4)]"
      },
      new String[] {
        "Graph1_Chain2",
        GRAPH_1, CHAIN_PATTERN_2,
        "expected1,expected2,expected3",
        "expected1[(v1)]" +
        "expected2[(v2)]" +
        "expected3[(v5)]"
      },
      new String[] {
        "Graph2_Chain3",
        GRAPH_2, CHAIN_PATTERN_3,
        "expected1",
        "expected1[ ]"
      },
      new String[] {
        "Graph2_Loop0",
        GRAPH_2,
        LOOP_PATTERN_0,
        "expected1",
        "expected1[(v9)-[e15]->(v9)]"
      },
      new String[] {
        "Graph1_Cycle2",
        GRAPH_1,
        CYCLE_PATTERN_2,
        "expected1",
        "expected1[" +
          "(v2)-[e3]->(v6)" +
          "(v6)-[e7]->(v2)" +
          "(v6)-[e9]->(v7)" +
        "]"
      },
      new String[] {
        "Graph1_Cycle4",
        GRAPH_1,
        CYCLE_PATTERN_4,
        "expected1,expected2",
        "expected1[(v1)-[e2]->(v6)-[e8]->(v5)-[e6]->(v4)-[e4]->(v1)]" +
        "expected2[(v5)-[e6]->(v4)-[e4]->(v1)-[e2]->(v6)-[e8]->(v5)]"
      },
      new String[] {
        "Graph2_Cycle5",
        GRAPH_2,
        CYCLE_PATTERN_5,
        "expected1,expected2",
        "expected1[(v0)-[e1]->(v4)<-[e2]-(v0)]" +
        "expected2[(v5)-[e9]->(v4)<-[e10]-(v5)]"
      },
      new String[] {
        "Graph4_Cycle6",
        GRAPH_4, CYCLE_PATTERN_6,
        "expected1,expected2",
        "expected1[(v1)-[e1]->(v2)<-[e2]-(v1)]" +
        "expected2[(v1)-[e2]->(v2)<-[e1]-(v1)]"
      },
      new String[] {
        "Graph4_Chain4",
        GRAPH_4, CHAIN_PATTERN_4,
        "expected1,expected2,expected3,expected4",
        "expected1[(v0)-[e0]->(v1)-[e1]->(v2)]" +
        "expected2[(v0)-[e0]->(v1)-[e2]->(v2)]" +
        "expected3[(v1)-[e1]->(v2)-[e3]->(v3)]" +
        "expected4[(v1)-[e2]->(v2)-[e3]->(v3)]"
      },
      new String[] {
        "Graph5_Chain6",
        GRAPH_5,
        CHAIN_PATTERN_6,
        "expected1",
        "expected1[ ]"
      },
      new String[] {
        "Graph6_Chain7",
        GRAPH_5,
        CHAIN_PATTERN_7,
        "expected1",
        "expected1[ ]"
      },
      new String[] {
        "Graph3_Unlabeled0",
        GRAPH_3,
        UNLABELED_PATTERN_0,
        "expected1,expected2,expected3,expected4",
        "expected1[(v0)]" +
        "expected2[(v1)]" +
        "expected3[(v2)]" +
        "expected4[(v3)]"
      },
      new String[] {
        "Graph3_Unlabeled1",
        GRAPH_3,
        UNLABELED_PATTERN_1,
        "expected1,expected2,expected3",
        "expected1[(v0)-[e0]->(v1)]" +
        "expected2[(v1)-[e1]->(v2)]" +
        "expected3[(v2)-[e2]->(v3)]"
      },
      new String[] {
        "Graph1_Unlabeled2",
        GRAPH_1,
        UNLABELED_PATTERN_2,
        "expected1,expected2",
        "expected1[(v4)-[e5]->(v3)]" +
        "expected2[(v6)-[e9]->(v7)]"
      },
      new String[] {
        "Graph1_Unlabeled3",
        GRAPH_1,
        UNLABELED_PATTERN_3,
        "expected1,expected2,expected3",
        "expected1[(v1)-[e2]->(v6)]" +
        "expected2[(v5)-[e6]->(v4)]" +
        "expected3[(v2)-[e3]->(v6)]"
      }
    );
  }
}