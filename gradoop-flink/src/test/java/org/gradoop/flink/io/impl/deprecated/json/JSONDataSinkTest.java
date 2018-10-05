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
package org.gradoop.flink.io.impl.deprecated.json;

import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class JSONDataSinkTest extends GradoopFlinkTestBase {

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testWrite() throws Exception {
    String tmpDir = temporaryFolder.getRoot().toString();
    final String vertexFile = tmpDir + "/nodes.json";
    final String edgeFile   = tmpDir + "/edges.json";
    final String graphFile  = tmpDir + "/graphs.json";

    GraphCollection input = getSocialNetworkLoader().getGraphCollection();

    // write to JSON
    input.writeTo(new JSONDataSink(graphFile, vertexFile, edgeFile, getConfig()));

    getExecutionEnvironment().execute();

    // read from JSON
    GraphCollection output = new JSONDataSource(
      graphFile, vertexFile, edgeFile, getConfig()).getGraphCollection();

    collectAndAssertTrue(output.equalsByGraphElementData(input));
  }
}
