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
package org.gradoop.flink.model.impl.operators.selection;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

public class SelectionTest extends GradoopFlinkTestBase {

  @Test
  public void testSelectionWithResult() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    GraphCollection inputCollection =
      loader.getGraphCollectionByVariables("g0", "g1", "g2");

    GraphCollection expectedOutputCollection =
      loader.getGraphCollectionByVariables("g0", "g1");

    FilterFunction<GraphHead> predicateFunc = (FilterFunction<GraphHead>) entity ->
      entity.hasProperty("vertexCount") && entity.getPropertyValue("vertexCount").getInt() == 3;

    GraphCollection outputCollection = inputCollection.select(predicateFunc);

    collectAndAssertTrue(
      expectedOutputCollection.equalsByGraphElementIds(outputCollection));
  }

  @Test
  public void testSelectionWithEmptyResult() throws Exception {
    FlinkAsciiGraphLoader loader = getSocialNetworkLoader();

    GraphCollection inputCollection =
      loader.getGraphCollectionByVariables("g0", "g1", "g2");

    FilterFunction<GraphHead> predicateFunc = (FilterFunction<GraphHead>) entity ->
      entity.hasProperty("vertexCount") && entity.getPropertyValue("vertexCount").getInt() > 5;

    GraphCollection outputCollection = inputCollection.select(predicateFunc);

    collectAndAssertTrue(outputCollection.isEmpty());
  }
}
