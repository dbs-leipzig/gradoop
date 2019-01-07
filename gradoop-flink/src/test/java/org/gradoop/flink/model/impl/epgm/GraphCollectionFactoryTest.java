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
package org.gradoop.flink.model.impl.epgm;

import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.util.FlinkAsciiGraphLoader;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

/**
 * Tests for class {@link GraphCollectionFactory}
 */
public class GraphCollectionFactoryTest extends GradoopFlinkTestBase {

  private FlinkAsciiGraphLoader loader;
  private GraphCollectionFactory factory;

  @Before
  public void setUp() throws IOException {
    loader = getSocialNetworkLoader();
    factory = getConfig().getGraphCollectionFactory();
  }

  @Test
  public void testFromGraphMethod() throws Exception {

    GraphCollection expected = loader.getGraphCollectionByVariables("g0");
    GraphCollection result = factory.fromGraph(loader.getLogicalGraphByVariable("g0"));

    collectAndAssertTrue(expected.equalsByGraphElementData(result));
  }

  @Test
  public void testSingleFromGraphsMethod() throws Exception {

    GraphCollection expected = loader.getGraphCollectionByVariables("g0");
    GraphCollection result = factory.fromGraphs(loader.getLogicalGraphByVariable("g0"));

    collectAndAssertTrue(result.equalsByGraphElementData(expected));
  }

  @Test
  public void testEmptyFromGraphsMethod() throws Exception {

    GraphCollection expected = factory.createEmptyCollection();
    GraphCollection result = factory.fromGraphs();

    collectAndAssertTrue(result.equalsByGraphElementData(expected));
  }

  @Test
  public void testFromGraphsMethod() throws Exception {

    LogicalGraph graph1 = loader.getLogicalGraphByVariable("g1");
    LogicalGraph graph2 = loader.getLogicalGraphByVariable("g2");

    GraphCollection expected = loader.getGraphCollectionByVariables("g1", "g2");
    GraphCollection result = factory.fromGraphs(graph1, graph2);

    collectAndAssertTrue(result.equalsByGraphElementData(expected));
  }
}
