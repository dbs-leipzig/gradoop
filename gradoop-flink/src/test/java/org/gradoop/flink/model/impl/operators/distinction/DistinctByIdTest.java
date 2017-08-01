/**
 * Copyright © 2014 - 2017 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.impl.operators.distinction;

import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

public class DistinctByIdTest extends GradoopFlinkTestBase {

  @Test
  public void testNonDistinctCollection() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    GraphCollection inputCollection = loader
      .getGraphCollectionByVariables("g0", "g0", "g1");

    GraphCollection expectedCollection = loader
      .getGraphCollectionByVariables("g0", "g1");

    GraphCollection outputCollection = inputCollection.distinctById();

    collectAndAssertTrue(outputCollection
      .equalsByGraphElementIds(expectedCollection));
  }

  @Test
  public void testDistinctCollection() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    GraphCollection inputCollection = loader
      .getGraphCollectionByVariables("g0", "g1");

    GraphCollection expectedCollection = loader
      .getGraphCollectionByVariables("g0", "g1");

    GraphCollection outputCollection = inputCollection.distinctById();

    collectAndAssertTrue(outputCollection
      .equalsByGraphElementIds(expectedCollection));
  }
}
