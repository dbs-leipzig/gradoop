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

import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.matching.TestData;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertTrue;

/**
 * This is a test class used for all Patter Matching Algorithms that attach a variable mapping to
 * the resulting graph heads.
 * It contains an additional test that check the presents of these mappings
 */
public abstract class PatternMatchingWithBindingTest extends PatternMatchingTest {

  public PatternMatchingWithBindingTest(String testName, String dataGraph, String queryGraph,
    String expectedGraphVariables, String expectedCollection) {
    super(testName,dataGraph,queryGraph,expectedGraphVariables,expectedCollection);
  }

  @Test
  public void testVariableMappingExists() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(this.dataGraph);

    // initialize with data graph
    LogicalGraph db = loader.getLogicalGraphByVariable(TestData.DATA_GRAPH_VARIABLE);

    // append the expected result
    loader.appendToDatabaseFromString(expectedCollection);

    // execute and validate
    List<GraphHead> graphHeads = getImplementation(queryGraph, false).execute(db).
      getGraphHeads().collect();

    for(GraphHead graphHead : graphHeads) {
      assertTrue(graphHead.hasProperty(PatternMatching.VARIABLE_MAPPING_KEY));
    }

  }

}
