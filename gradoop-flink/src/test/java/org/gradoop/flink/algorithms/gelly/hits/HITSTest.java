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
package org.gradoop.flink.algorithms.gelly.hits;

import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.junit.Test;

public class HITSTest extends GradoopFlinkTestBase {

  @Test
  public void minimalHITSTest() throws Exception {

    String inputString = "input[(v0:A)-[:e]->(v1:B)<-[:e]-(v2:C)]";

    // Values are normalized sqrt(0.5) = .7071067811865475d
    String expectedResultString = "input[" +
      "(v0:A {aScore: 0.0d, hScore: .7071067811865475d})-[:e]->" +
      "(v1:B {aScore: 1.0d, hScore: 0.0d})<-[:e]-" +
      "(v2:C {aScore: 0.0d, hScore: .7071067811865475d})]";

    LogicalGraph input = getLoaderFromString(inputString)
      .getLogicalGraphByVariable("input");
    LogicalGraph expectedResult = getLoaderFromString(expectedResultString)
      .getLogicalGraphByVariable("input");
    LogicalGraph result = input.callForGraph(new HITS("aScore", "hScore", 1));

    collectAndAssertTrue(result.equalsByElementData(expectedResult));
  }
}
