/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.flink.model.impl.operators.matching.single;

import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.flink.model.impl.LogicalGraph;
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
