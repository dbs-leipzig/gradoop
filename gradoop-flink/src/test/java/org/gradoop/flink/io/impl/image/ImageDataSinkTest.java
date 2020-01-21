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
package org.gradoop.flink.io.impl.image;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.layouting.LayoutingAlgorithmTest;
import org.gradoop.flink.model.impl.operators.layouting.RandomLayouter;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

public class ImageDataSinkTest extends GradoopFlinkTestBase {


  private String getTestPath() {
    return "/tmp/testImage.png";
  }

  @Before
  public void setup() {
    File of = new File(getTestPath());
    if (of.exists()) {
      of.delete();
    }
    of.getParentFile().mkdirs();
  }

  @Test
  public void testImageSink() throws Exception {
    ExecutionEnvironment env = getExecutionEnvironment();
    GradoopFlinkConfig cfg = getConfig();

    FlinkAsciiGraphLoader loader = new FlinkAsciiGraphLoader(cfg);
    loader.initDatabaseFromString(LayoutingAlgorithmTest.graph);

    RandomLayouter rl = new RandomLayouter(0, 500, 0, 500);
    LogicalGraph g = rl.execute(loader.getLogicalGraph());

    DataSink p = new ImageDataSink(getTestPath(), 500, 500, 1000, 1000).vertexLabel("name").zoom(true, 100);
    g.writeTo(p);

    env.execute();

    File of = new File(getTestPath());
    Assert.assertTrue(of.exists());
  }
}
