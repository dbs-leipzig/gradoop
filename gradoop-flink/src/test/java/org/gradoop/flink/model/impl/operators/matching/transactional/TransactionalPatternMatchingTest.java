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
package org.gradoop.flink.model.impl.operators.matching.transactional;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.impl.operators.matching.TestData;
import org.gradoop.flink.model.impl.operators.matching.transactional.algorithm.DepthSearchMatching;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Rule;
import org.junit.Test;
import static org.junit.Assert.assertTrue;
import org.junit.rules.TemporaryFolder;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class TransactionalPatternMatchingTest extends GradoopFlinkTestBase {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void test() throws Exception {

    FlinkAsciiGraphLoader loader = getLoaderFromString(
      "g1:A[" +
      "(v0:B {id : 0})" +
      "(v1:A {id : 1})" +
      "(v2:A {id : 2})" +
      "(v3:C {id : 3})" +
      "(v4:B {id : 4})" +
      "(v5:A {id : 5})" +
      "(v6:B {id : 6})" +
      "(v7:C {id : 7})" +
      "(v8:B {id : 8})" +
      "(v9:C {id : 9})" +
      "(v10:D {id : 10})" +
      "(v0)-[e0:a {id : 0}]->(v1)" +
      "(v0)-[e1:a {id : 1}]->(v3)" +
      "(v1)-[e2:a {id : 2}]->(v6)" +
      "(v2)-[e3:a {id : 3}]->(v6)" +
      "(v4)-[e4:a {id : 4}]->(v1)" +
      "(v4)-[e5:b {id : 5}]->(v3)" +
      "(v5)-[e6:a {id : 6}]->(v4)" +
      "(v6)-[e7:a {id : 7}]->(v2)" +
      "(v6)-[e8:a {id : 8}]->(v5)" +
      "(v6)-[e9:b {id : 9}]->(v7)" +
      "(v8)-[e10:a {id : 10}]->(v5)" +
      "(v5)-[e11:a {id : 11}]->(v9)" +
      "(v9)-[e12:c {id : 12}]->(v10)" +
      "]" +
      "g2:B[" +
      "(v11:B {id : 0})" +
      "(v12:A {id : 1})" +
      "(v13:A {id : 2})" +
      "(v14:A {id : 3})" +
      "(v15:C {id : 4})" +
      "(v16:B {id : 5})" +
      "(v17:B {id : 6})" +
      "(v18:C {id : 7})" +
      "(v19:B {id : 8})" +
      "(v20:B {id : 9})" +
      "(v21:A {id : 10})" +
      "(v22:C {id : 11})" +
      "(v23:D {id : 12})" +
      "(v12)-[e13:a {id : 0}]->(v11)" +
      "(v11)-[e14:b {id : 1}]->(v15)" +
      "(v11)-[e15:a {id : 2}]->(v15)" +
      "(v11)-[e16:a {id : 3}]->(v14)" +
      "(v14)-[e17:a {id : 4}]->(v16)" +
      "(v16)-[e18:a {id : 5}]->(v12)" +
      "(v12)-[e19:a {id : 6}]->(v17)" +
      "(v17)-[e20:a {id : 7}]->(v13)" +
      "(v13)-[e21:a {id : 8}]->(v17)" +
      "(v16)-[e22:a {id : 9}]->(v15)" +
      "(v16)-[e23:b {id : 10}]->(v15)" +
      "(v17)-[e24:b {id : 11}]->(v18)" +
      "(v19)-[e25:a {id : 12}]->(v18)" +
      "(v21)-[e26:a {id : 13}]->(v16)" +
      "(v17)-[e27:a {id : 14}]->(v21)" +
      "(v20)-[e28:d {id : 15}]->(v20)" +
      "(v20)-[e29:a {id : 16}]->(v21)" +
      "(v21)-[e30:d {id : 17}]->(v22)" +
      "(v22)-[e31:a {id : 18}]->(v23)" +
      "]" +
      "g3:C[" +
      "(v24:A {id : 0})-[e32:a {id : 0}]->(v25:A {id : 1})" +
      "(v25)-[e33:a {id : 1}]->(v26:A {id : 2})" +
      "(v26)-[e34:a {id : 2}]->(v27:A {id : 3})" +
      "]" +
      "g4:D[" +
      "(v28:A {id : 0})-[e35:a {id : 0}]->(v29:A {id : 1})" +
      "(v29)-[e36:a {id : 1}]->(v30:A {id : 2})-[e37:a {id : 3}]->(v31:A {id : 3})" +
      "(v29)-[e38:a {id : 2}]->(v30)" +
      "]" +
      "g5:E[(v32 {id : 0})-[e39 {id : 0}]->(v33 {id : 1})]");

    GraphCollection coll = loader.getGraphCollectionByVariables("g1", "g2", "g3", "g4", "g5");

    for(int i=0;i<tests.length;i++) {
      String testPattern = tests[i];

      GraphCollection result = coll.match(testPattern, new DepthSearchMatching(), true);

      Collection<GraphHead> originalHeads  = Lists.newArrayList();
      Collection<GraphHead> resultHeads = Lists.newArrayList();

      coll.getGraphHeads().output(new LocalCollectionOutputFormat<>(
        originalHeads));
      result.getGraphHeads().output(new LocalCollectionOutputFormat<>(
        resultHeads
      ));

      getExecutionEnvironment().execute();

      Map<GradoopId, String> lineageIdMap = new HashMap<>();

      for(GraphHead original : originalHeads) {
        lineageIdMap.put(original.getId(), original.getLabel());
      }

      int aCount = 0;
      int bCount = 0;
      int cCount = 0;
      int dCount = 0;
      int eCount = 0;
      for(GraphHead head : resultHeads) {
        GradoopId id = head.getPropertyValue("lineage").getGradoopId();
        if (lineageIdMap.get(id).equals("A")) {
          aCount++;
        }
        if (lineageIdMap.get(id).equals("B")) {
          bCount++;
        }
        if (lineageIdMap.get(id).equals("C")) {
          cCount++;
        }
        if (lineageIdMap.get(id).equals("D")) {
          dCount++;
        }
        if (lineageIdMap.get(id).equals("E")) {
          eCount++;
        }
      }
      assertTrue(aCount == resultCounts[i][0]);
      assertTrue(bCount == resultCounts[i][1]);
      assertTrue(cCount == resultCounts[i][2]);
      assertTrue(dCount == resultCounts[i][3]);
      assertTrue(eCount == resultCounts[i][4]);
    }
  }

  private String[] tests = {
    TestData.CHAIN_PATTERN_0,
    TestData.CHAIN_PATTERN_1,
    TestData.CHAIN_PATTERN_2,
    TestData.CHAIN_PATTERN_3,
    TestData.CHAIN_PATTERN_4,
    TestData.CHAIN_PATTERN_5,
    TestData.CHAIN_PATTERN_6,
    TestData.CYCLE_PATTERN_0,
    TestData.CYCLE_PATTERN_1,
    TestData.CYCLE_PATTERN_2,
    TestData.CYCLE_PATTERN_3,
    TestData.CYCLE_PATTERN_4,
    TestData.CYCLE_PATTERN_5,
    TestData.LOOP_PATTERN_0,
    TestData.UNLABELED_PATTERN_0,
    TestData.UNLABELED_PATTERN_1,
    TestData.UNLABELED_PATTERN_2,
    TestData.UNLABELED_PATTERN_3,
  };

  private long[][] resultCounts = {
    {3, 5, 0, 0, 0},
    {0, 0, 3, 4, 0},
    {3, 4, 4, 4, 0},
    {0, 0, 0, 0, 0},
    {0, 0, 2, 4, 0},
    {0, 0, 0, 0, 0},
    {12, 18, 0, 0, 0},
    {1, 1, 0, 0, 0},
    {0, 0, 0, 0, 0},
    {1, 1, 0, 0, 0},
    {0, 1, 0, 0, 0},
    {2, 4, 0, 0, 0},
    {0, 2, 0, 0, 0},
    {0, 1, 0, 0, 0},
    {11, 13, 4, 4, 2},
    {13, 18, 3, 4, 1},
    {2, 3, 0, 0, 0},
    {3, 5, 0, 0, 0}
  };
}
