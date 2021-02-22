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
package org.gradoop.temporal.model.impl.layout;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.common.model.api.entities.EdgeFactory;
import org.gradoop.common.model.api.entities.GraphHeadFactory;
import org.gradoop.common.model.api.entities.VertexFactory;
import org.gradoop.temporal.model.impl.TemporalGraphFactory;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalGraphHead;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.gradoop.temporal.util.TemporalGradoopConfig;
import org.gradoop.temporal.util.TemporalGradoopTestBase;
import org.junit.BeforeClass;

/**
 * Base class for layout tests. Provides some predefined graph elements.
 */
public abstract class BaseTemporalGVELayoutTest extends TemporalGradoopTestBase {

  protected static TemporalGraphHead g0;
  protected static TemporalGraphHead g1;

  protected static TemporalVertex v0;
  protected static TemporalVertex v1;
  protected static TemporalVertex v2;
  protected static TemporalVertex v3;
  protected static TemporalVertex v4;
  protected static TemporalVertex v5;

  protected static TemporalEdge e0;
  protected static TemporalEdge e1;
  protected static TemporalEdge e2;
  protected static TemporalEdge e3;
  protected static TemporalEdge e4;

  /**
   * Create temporal graph elements before tests.
   */
  @BeforeClass
  public static void setup() {
    ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    TemporalGraphFactory factory = TemporalGradoopConfig.createConfig(env).getTemporalGraphFactory();

    GraphHeadFactory<TemporalGraphHead> temporalGraphHeadFactory = factory.getGraphHeadFactory();
    VertexFactory<TemporalVertex> temporalVertexFactory = factory.getVertexFactory();
    EdgeFactory<TemporalEdge> temporalEdgeFactory = factory.getEdgeFactory();

    g0 = temporalGraphHeadFactory.createGraphHead("G");
    g1 = temporalGraphHeadFactory.createGraphHead("Q");

    v0 = temporalVertexFactory.createVertex("A");
    v1 = temporalVertexFactory.createVertex("A");
    v2 = temporalVertexFactory.createVertex("A");
    v3 = temporalVertexFactory.createVertex("B");
    v4 = temporalVertexFactory.createVertex("B");
    v5 = temporalVertexFactory.createVertex();

    e0 = temporalEdgeFactory.createEdge("X", v0.getId(), v1.getId());
    e1 = temporalEdgeFactory.createEdge("X", v1.getId(), v1.getId());
    e2 = temporalEdgeFactory.createEdge("Y", v3.getId(), v4.getId());
    e3 = temporalEdgeFactory.createEdge("Y", v3.getId(), v1.getId());

    v0.addGraphId(g0.getId());
    v1.addGraphId(g0.getId());
    v1.addGraphId(g1.getId());
    v2.addGraphId(g1.getId());
    v3.addGraphId(g1.getId());
    v4.addGraphId(g1.getId());
    v5.addGraphId(g1.getId());

    e0.addGraphId(g0.getId());
    e1.addGraphId(g0.getId());
    e2.addGraphId(g1.getId());
    e3.addGraphId(g1.getId());
  }
}
