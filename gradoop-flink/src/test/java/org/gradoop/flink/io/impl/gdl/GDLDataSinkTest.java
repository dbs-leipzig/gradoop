/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
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
package org.gradoop.flink.io.impl.gdl;

import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class GDLDataSinkTest extends GradoopFlinkTestBase {
  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  @Test
  public void testWrite() throws Exception {
    FlinkAsciiGraphLoader testLoader = getSocialNetworkLoader();
    GraphCollection expectedCollection = testLoader.getGraphCollectionByVariables("g0", "g1", "g2", "g3");

    String path = temporaryFolder.getRoot().getPath() + "/graph.gdl";
    DataSink gdlsink = new GDLDataSink(path);
    gdlsink.write(expectedCollection, true);
    getExecutionEnvironment().execute();

    FlinkAsciiGraphLoader sinkLoader = getLoaderFromFile(path);
    GraphCollection sinkCollection = sinkLoader
      .getGraphCollectionByVariables("g0", "g1", "g2", "g3");

    collectAndAssertTrue(sinkCollection.equalsByGraphElementData(expectedCollection));
  }
}
