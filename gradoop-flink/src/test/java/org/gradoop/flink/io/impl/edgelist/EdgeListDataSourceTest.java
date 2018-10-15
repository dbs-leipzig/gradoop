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
package org.gradoop.flink.io.impl.edgelist;

import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

public class EdgeListDataSourceTest extends GradoopFlinkTestBase {

  @Test
  public void testRead() throws Exception {
    String edgeListFile = getFilePath("/data/edgelist/basic/input");
    String gdlFile = getFilePath("/data/edgelist/basic/expected.gdl");

    DataSource dataSource = new EdgeListDataSource(edgeListFile, ",", getConfig());
    LogicalGraph result = dataSource.getLogicalGraph();
    FlinkAsciiGraphLoader loader = getLoaderFromFile(gdlFile);
    LogicalGraph expected = loader.getLogicalGraphByVariable("expected");
    collectAndAssertTrue(expected.equalsByElementData(result));
  }
}
