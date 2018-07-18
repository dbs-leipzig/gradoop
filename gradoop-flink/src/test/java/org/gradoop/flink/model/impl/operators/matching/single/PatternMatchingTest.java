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

import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.matching.TestData;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public abstract class PatternMatchingTest extends GradoopFlinkTestBase {

  protected final String testName;

  protected final String dataGraph;

  protected final String queryGraph;

  protected final String[] expectedGraphVariables;

  protected final String expectedCollection;

  public PatternMatchingTest(String testName, String dataGraph, String queryGraph,
    String expectedGraphVariables, String expectedCollection) {
    this.testName = testName;
    this.dataGraph = dataGraph;
    this.queryGraph = queryGraph;
    this.expectedGraphVariables = expectedGraphVariables.split(",");
    this.expectedCollection = expectedCollection;
  }

  public abstract PatternMatching getImplementation(String queryGraph, boolean attachData);

  @Test
  public void testGraphElementIdEquality() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(dataGraph);

    // initialize with data graph
    LogicalGraph db = loader.getLogicalGraphByVariable(TestData.DATA_GRAPH_VARIABLE);

    // append the expected result
    loader.appendToDatabaseFromString(expectedCollection);

    // execute and validate
    GraphCollection result = getImplementation(queryGraph, false).execute(db);
    GraphCollection expected = loader.getGraphCollectionByVariables(expectedGraphVariables);
    collectAndAssertTrue(result.equalsByGraphElementIds(expected));
  }

  @Test
  public void testGraphElementEquality() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString(dataGraph);

    // initialize with data graph
    LogicalGraph db = loader.getLogicalGraphByVariable(TestData.DATA_GRAPH_VARIABLE);

    // append the expected result
    loader.appendToDatabaseFromString(expectedCollection);

    // execute and validate
    GraphCollection result = getImplementation(queryGraph, true).execute(db);
    GraphCollection expected = loader.getGraphCollectionByVariables(expectedGraphVariables);
    collectAndAssertTrue(result.equalsByGraphElementData(expected));
  }
}
