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
package org.gradoop.flink.model.impl.layouts.gve;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMGraphHead;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.epgm.LogicalGraphFactory;
import org.gradoop.flink.util.GradoopFlinkConfig;
import org.junit.BeforeClass;

/**
 * Base class for layout tests. Provides some predefined graph elements.
 */
public abstract class BaseGVELayoutTest extends GradoopFlinkTestBase {

  protected static EPGMGraphHead g0;
  protected static EPGMGraphHead g1;

  protected static EPGMVertex v0;
  protected static EPGMVertex v1;
  protected static EPGMVertex v2;

  protected static EPGMEdge e0;
  protected static EPGMEdge e1;

  /**
   * Create graph elements before tests.
   */
  @BeforeClass
  public static void setup() {
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    LogicalGraphFactory factory = GradoopFlinkConfig.createConfig(env).getLogicalGraphFactory();

    g0 = factory.getGraphHeadFactory().createGraphHead("A");
    g1 = factory.getGraphHeadFactory().createGraphHead("B");

    v0 = factory.getVertexFactory().createVertex("A");
    v1 = factory.getVertexFactory().createVertex("B");
    v2 = factory.getVertexFactory().createVertex("C");

    e0 = factory.getEdgeFactory().createEdge("a", v0.getId(), v1.getId());
    e1 = factory.getEdgeFactory().createEdge("b", v1.getId(), v2.getId());

    v0.addGraphId(g0.getId());
    v1.addGraphId(g0.getId());
    v1.addGraphId(g1.getId());
    v2.addGraphId(g1.getId());

    e0.addGraphId(g0.getId());
    e1.addGraphId(g1.getId());
  }
}
