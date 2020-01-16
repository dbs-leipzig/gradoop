/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
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
package org.gradoop.temporal.model.impl.operators.tostring;

import org.apache.commons.io.FileUtils;
import org.gradoop.flink.model.impl.functions.epgm.RemoveProperties;
import org.gradoop.flink.model.impl.operators.tostring.CanonicalAdjacencyMatrixBuilder;
import org.gradoop.flink.model.impl.operators.transformation.ApplyTransformation;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.TemporalGraphCollection;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalGraphHead;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.gradoop.temporal.util.TemporalGradoopTestBase;
import org.testng.annotations.Test;

import static org.gradoop.temporal.util.TemporalGradoopTestUtils.PROPERTY_VALID_FROM;
import static org.gradoop.temporal.util.TemporalGradoopTestUtils.PROPERTY_VALID_TO;
import static org.testng.AssertJUnit.assertEquals;

public class TemporalCanonicalAdjacencyMatrixBuilderTest extends TemporalGradoopTestBase {

  @Test
  public void testDirected() throws Exception {
    FlinkAsciiGraphLoader loader = getTemporalSocialNetworkLoader();

    loader.initDatabaseFromFile(getFilePath("/data/gdl/cam_test.gdl"));

    TemporalGraphCollection g = toTemporalGraphCollectionWithDefaultExtractors(loader.getGraphCollection());
    g = g.apply(new ApplyTransformation<>(new RemoveProperties<>(PROPERTY_VALID_FROM, PROPERTY_VALID_TO),
      new RemoveProperties<>(PROPERTY_VALID_FROM, PROPERTY_VALID_TO),
      new RemoveProperties<>(PROPERTY_VALID_FROM, PROPERTY_VALID_TO)));

    CanonicalAdjacencyMatrixBuilder<TemporalGraphHead, TemporalVertex, TemporalEdge, TemporalGraph,
      TemporalGraphCollection> cam =
      new CanonicalAdjacencyMatrixBuilder<>(
        new TemporalGraphHeadToDataString<>(),
        new TemporalVertexToDataString<>(),
        new TemporalEdgeToDataString<>(), true);

    String result = cam.execute(g).collect().get(0);

    String expectation = FileUtils.readFileToString(
      FileUtils.getFile(getFilePath("/data/expected/cam_test_directed")));

    assertEquals(expectation, result);
  }

  @Test
  public void testUndirected() throws Exception {
    FlinkAsciiGraphLoader loader = new FlinkAsciiGraphLoader(getConfig());

    loader.initDatabaseFromFile(getFilePath("/data/gdl/cam_test.gdl"));

    TemporalGraphCollection g = toTemporalGraphCollectionWithDefaultExtractors(loader.getGraphCollection());
    g = g.apply(new ApplyTransformation<>(new RemoveProperties<>(PROPERTY_VALID_FROM, PROPERTY_VALID_TO),
      new RemoveProperties<>(PROPERTY_VALID_FROM, PROPERTY_VALID_TO),
      new RemoveProperties<>(PROPERTY_VALID_FROM, PROPERTY_VALID_TO)));

    CanonicalAdjacencyMatrixBuilder<TemporalGraphHead, TemporalVertex, TemporalEdge, TemporalGraph,
      TemporalGraphCollection> cam =
      new CanonicalAdjacencyMatrixBuilder<>(
        new TemporalGraphHeadToDataString<>(),
        new TemporalVertexToDataString<>(),
        new TemporalEdgeToDataString<>(), false);

    String result = cam.execute(g).collect().get(0);

    String expectation = FileUtils.readFileToString(
      FileUtils.getFile(getFilePath("/data/expected/cam_test_undirected")));

    assertEquals(expectation, result);
  }
}
