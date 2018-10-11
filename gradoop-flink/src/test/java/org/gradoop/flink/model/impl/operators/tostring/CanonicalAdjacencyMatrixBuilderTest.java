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
package org.gradoop.flink.model.impl.operators.tostring;

import org.apache.commons.io.FileUtils;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.api.epgm.GraphCollection;
import org.gradoop.flink.model.impl.operators.tostring.functions.EdgeToDataString;
import org.gradoop.flink.model.impl.operators.tostring.functions.GraphHeadToDataString;
import org.gradoop.flink.model.impl.operators.tostring.functions.VertexToDataString;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class CanonicalAdjacencyMatrixBuilderTest extends GradoopFlinkTestBase {

  @Test
  public void testDirected() throws Exception {
    FlinkAsciiGraphLoader loader = new FlinkAsciiGraphLoader(getConfig());

    loader.initDatabaseFromFile(getFilePath("/data/gdl/cam_test.gdl"));

    GraphCollection g = loader.getGraphCollection();

    CanonicalAdjacencyMatrixBuilder cam =
      new CanonicalAdjacencyMatrixBuilder(
        new GraphHeadToDataString(),
        new VertexToDataString(),
        new EdgeToDataString(), true);

    String result = cam.execute(g).collect().get(0);

    String expectation = FileUtils.readFileToString(
      FileUtils.getFile(getFilePath("/data/expected/cam_test_directed")));

    assertTrue(expectation.equals(result));
  }

  @Test
  public void testUndirected() throws Exception {
    FlinkAsciiGraphLoader loader = new FlinkAsciiGraphLoader(getConfig());

    loader.initDatabaseFromFile(getFilePath("/data/gdl/cam_test.gdl"));

    GraphCollection g = loader.getGraphCollection();

    CanonicalAdjacencyMatrixBuilder cam =
      new CanonicalAdjacencyMatrixBuilder(
        new GraphHeadToDataString(),
        new VertexToDataString(),
        new EdgeToDataString(), false);

    String result = cam.execute(g).collect().get(0);

    String expectation = FileUtils.readFileToString(
      FileUtils.getFile(getFilePath("/data/expected/cam_test_undirected")));

    assertTrue(expectation.equals(result));
  }

}