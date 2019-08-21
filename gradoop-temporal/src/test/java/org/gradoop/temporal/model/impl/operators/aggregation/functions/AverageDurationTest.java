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
package org.gradoop.temporal.model.impl.operators.aggregation.functions;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.api.entities.EdgeFactory;
import org.gradoop.common.model.api.entities.VertexFactory;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.TemporalGraphFactory;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalElement;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.gradoop.temporal.util.TemporalGradoopTestBase;
import org.junit.Test;

import java.util.Arrays;

import static org.gradoop.common.model.impl.properties.PropertyValue.create;
import static org.gradoop.flink.model.impl.operators.aggregation.functions.average.Average.IGNORED_VALUE;
import static org.gradoop.temporal.model.api.functions.TimeDimension.TRANSACTION_TIME;
import static org.gradoop.temporal.model.api.functions.TimeDimension.VALID_TIME;
import static org.gradoop.temporal.model.impl.pojo.TemporalElement.DEFAULT_TIME_FROM;
import static org.gradoop.temporal.model.impl.pojo.TemporalElement.DEFAULT_TIME_TO;
import static org.junit.Assert.assertEquals;

/**
 * Test for the {@link AverageDuration} aggregate function.
 */
public class AverageDurationTest extends TemporalGradoopTestBase {

  /**
   * Test the implementation of {@link AverageDuration#getIncrement(TemporalElement)} for transaction time.
   */
  @Test
  public void testGetIncrementForTxTime() {
    AverageDuration function = new AverageDuration("", TRANSACTION_TIME);
    TemporalElement testElement = getVertexFactory().createVertex();
    testElement.setTransactionTime(Tuple2.of(DEFAULT_TIME_FROM, DEFAULT_TIME_TO));
    // This will produce an overflow, which is currently ignored.
    assertEquals(IGNORED_VALUE, function.getIncrement(testElement));
    testElement.setTransactionTime(Tuple2.of(DEFAULT_TIME_FROM, 7L));
    assertEquals(IGNORED_VALUE, function.getIncrement(testElement));
    testElement.setTransactionTime(Tuple2.of(4L, DEFAULT_TIME_TO));
    assertEquals(IGNORED_VALUE, function.getIncrement(testElement));
    testElement.setTransactionTime(Tuple2.of(1L, 11L));
    assertEquals(create(Arrays.asList(create(10L), create(1L))),
      function.getIncrement(testElement));
  }

  /**
   * Test the implementation of {@link AverageDuration#getIncrement(TemporalElement)} for valid time.
   */
  @Test
  public void testGetIncrementForValidTime() {
    AverageDuration function = new AverageDuration("", VALID_TIME);
    TemporalElement testElement = getVertexFactory().createVertex();
    testElement.setValidTime(Tuple2.of(DEFAULT_TIME_FROM, DEFAULT_TIME_TO));
    assertEquals(IGNORED_VALUE, function.getIncrement(testElement));
    testElement.setValidTime(Tuple2.of(DEFAULT_TIME_FROM, 6L));
    assertEquals(IGNORED_VALUE, function.getIncrement(testElement));
    testElement.setValidTime(Tuple2.of(3L, DEFAULT_TIME_TO));
    assertEquals(IGNORED_VALUE, function.getIncrement(testElement));
    testElement.setValidTime(Tuple2.of(2L, 7L));
    assertEquals(create(Arrays.asList(create(5L), create(1L))),
      function.getIncrement(testElement));
  }

  /**
   * Test the {@link AverageDuration} aggregate function and its subclasses in an aggregation.
   *
   * @throws Exception when the execution in Flink fails.
   */
  @Test
  public void testWithAggregation() throws Exception {
    TemporalGraphFactory graphFactory = getConfig().getTemporalGraphFactory();
    VertexFactory<TemporalVertex> vertexFactory = graphFactory.getVertexFactory();
    EdgeFactory<TemporalEdge> edgeFactory = graphFactory.getEdgeFactory();
    TemporalVertex v1 = vertexFactory.createVertex();
    v1.setTransactionTime(Tuple2.of(1L, 2L));
    v1.setValidTime(Tuple2.of(DEFAULT_TIME_FROM, DEFAULT_TIME_TO));
    TemporalVertex v2 = vertexFactory.createVertex();
    v2.setTransactionTime(Tuple2.of(DEFAULT_TIME_FROM, 5L));
    v2.setValidTime(Tuple2.of(-3L, DEFAULT_TIME_TO));
    TemporalVertex v3 = vertexFactory.createVertex();
    v3.setTransactionTime(Tuple2.of(0L, DEFAULT_TIME_TO));
    v3.setValidTime(Tuple2.of(DEFAULT_TIME_FROM, 0L));
    TemporalVertex v4 = vertexFactory.createVertex();
    v4.setTransactionTime(Tuple2.of(DEFAULT_TIME_FROM, DEFAULT_TIME_TO));
    v4.setValidTime(Tuple2.of(-5L, -2L));
    TemporalVertex v5 = vertexFactory.createVertex();
    v5.setTransactionTime(Tuple2.of(1L, 4L));
    v5.setValidTime(Tuple2.of(-10L, -3L));

    TemporalEdge e1 = edgeFactory.createEdge(v1.getId(), v2.getId());
    e1.setTransactionTime(Tuple2.of(0L, 30L));
    e1.setValidTime(Tuple2.of(DEFAULT_TIME_FROM, DEFAULT_TIME_TO));
    TemporalEdge e2 = edgeFactory.createEdge(v2.getId(), v3.getId());
    e2.setTransactionTime(Tuple2.of(DEFAULT_TIME_FROM, 7L));
    e2.setValidTime(Tuple2.of(-100L, DEFAULT_TIME_TO));
    TemporalEdge e3 = edgeFactory.createEdge(v3.getId(), v1.getId());
    e3.setTransactionTime(Tuple2.of(-1L, DEFAULT_TIME_TO));
    e3.setValidTime(Tuple2.of(DEFAULT_TIME_FROM, -80L));
    TemporalEdge e4 = edgeFactory.createEdge(v1.getId(), v3.getId());
    e4.setTransactionTime(Tuple2.of(DEFAULT_TIME_FROM, DEFAULT_TIME_TO));
    e4.setValidTime(Tuple2.of(-120L, -107L));
    TemporalEdge e5 = edgeFactory.createEdge(v2.getId(), v4.getId());
    e5.setTransactionTime(Tuple2.of(10L, 50L));
    e5.setValidTime(Tuple2.of(-301L, -276L));

    DataSet<TemporalVertex> vertices = getExecutionEnvironment().fromElements(v1, v2, v3, v4, v5);
    DataSet<TemporalEdge> edges = getExecutionEnvironment().fromElements(e1, e2, e3, e4, e5);
    TemporalGraph result = graphFactory.fromDataSets(vertices, edges)
      .aggregate(new AverageDuration("avgDurTx", TRANSACTION_TIME),
        new AverageDuration("avgDurValid", VALID_TIME),
        new AverageVertexDuration("avgVertexDurTx", TRANSACTION_TIME),
        new AverageVertexDuration("avgVertexDurValid", VALID_TIME),
        new AverageEdgeDuration("avgEdgeDurTx", TRANSACTION_TIME),
        new AverageEdgeDuration("avgEdgeDurValid", VALID_TIME));
    Properties headProperties = result.getGraphHead().collect().get(0).getProperties();
    assertEquals(18.5d, headProperties.get("avgDurTx").getDouble(), 1e-7d);
    assertEquals(12.d, headProperties.get("avgDurValid").getDouble(), 1e-7d);
    assertEquals(2.d, headProperties.get("avgVertexDurTx").getDouble(), 1e-7d);
    assertEquals(5.d, headProperties.get("avgVertexDurValid").getDouble(), 1e-7d);
    assertEquals(35.d, headProperties.get("avgEdgeDurTx").getDouble(), 1e-7d);
    assertEquals(19.d, headProperties.get("avgEdgeDurValid").getDouble(), 1e-7d);
  }
}
