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
package org.gradoop.flink.model.impl.operators.limit;

import org.apache.flink.api.common.InvalidProgramException;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.jamon.annotations.Template;
import org.junit.Test;
import org.s1ck.gdl.model.Graph;

import static org.junit.Assert.assertEquals;

public class LimitTest extends GradoopFlinkTestBase {

  @Test
  public void testSameCollection() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    int limit = 2;

    GraphCollection input = loader.getGraphCollectionByVariables("g0", "g1");
    collectAndAssertTrue(input.equalsByGraphElementData(input.limit(limit)));
  }

  @Test
  public void testInBound() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    int limit = 2;

    GraphCollection inputCollection = loader.getGraphCollectionByVariables("g0", "g1", "g2", "g3");

    GraphCollection outputCollection = inputCollection.limit(limit);
    assertEquals(limit, outputCollection.getGraphHeads().count());
  }

  @Test
  public void testOutOfBound() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    GraphCollection inputCollection = loader.getGraphCollectionByVariables("g0", "g1");

    int limit = 4;
    int expectedCount = 2;

    GraphCollection outputCollection = inputCollection.limit(limit);
    assertEquals(expectedCount, outputCollection.getGraphHeads().count());
  }

  @Test
  public void testEmpty() throws Exception {
    GraphCollection inputCollection = getConfig().getGraphCollectionFactory()
      .createEmptyCollection();

    int limit = 4;
    int expectedCount = 0;

    GraphCollection outputCollection = inputCollection.limit(limit);
    assertEquals(expectedCount, outputCollection.getGraphHeads().count());
  }

  @Test(expected = InvalidProgramException.class)
  public void testNegativeLimit() throws Exception {
    GraphCollection inputCollection = getConfig().getGraphCollectionFactory()
      .createEmptyCollection();

    int limit = -1;
    int expectedCount = 0;

    GraphCollection outputCollection = inputCollection.limit(limit);
    assertEquals(expectedCount, outputCollection.getGraphHeads().count());
  }

  @Test
  public void testDistinctGraphs() throws Exception {
    FlinkAsciiGraphLoader loader = getLoaderFromString("(v1) (v2) g1[(v1)] g2[(v1) (v2)]");

    int limit = 2;

    GraphCollection inputCollection = loader.getGraphCollectionByVariables("g1", "g2");
    collectAndAssertTrue(inputCollection.equalsByGraphElementData(inputCollection.limit(limit)));
  }
}
