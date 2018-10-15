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
package org.gradoop.flink.algorithms.btgs;

import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

public class BusinessTransactionGraphsTest  extends GradoopFlinkTestBase {

  @Test
  public void testExecute() throws Exception {

    FlinkAsciiGraphLoader loader = new FlinkAsciiGraphLoader(getConfig());

    loader.initDatabaseFromFile(getFilePath("/data/gdl/iig_btgs.gdl"));

    LogicalGraph iig = loader.getLogicalGraphByVariable("iig");

    GraphCollection expectation = loader
      .getGraphCollectionByVariables("btg1", "btg2", "btg3", "btg4");

    GraphCollection result = iig
      .callForCollection(new BusinessTransactionGraphs());

    collectAndAssertTrue(expectation.equalsByGraphElementData(result));
  }
}