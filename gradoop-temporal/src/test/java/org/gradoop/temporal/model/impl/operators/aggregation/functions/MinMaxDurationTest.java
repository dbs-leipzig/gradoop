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
package org.gradoop.temporal.model.impl.operators.aggregation.functions;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.api.entities.EdgeFactory;
import org.gradoop.common.model.api.entities.VertexFactory;
import org.gradoop.common.model.impl.pojo.EPGMGraphHead;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.TemporalGraphFactory;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalElement;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.gradoop.temporal.util.TemporalGradoopTestBase;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.gradoop.temporal.model.api.TimeDimension.TRANSACTION_TIME;
import static org.gradoop.temporal.model.api.TimeDimension.VALID_TIME;
import static org.gradoop.temporal.model.impl.pojo.TemporalElement.DEFAULT_TIME_FROM;
import static org.gradoop.temporal.model.impl.pojo.TemporalElement.DEFAULT_TIME_TO;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Test class for the following classes: {@link MaxDuration}, {@link MinDuration}
 * and the subclasses:
 * <ul>
 *   <li>{@link MaxVertexDuration}</li>
 *   <li>{@link MaxEdgeDuration}</li>
 *   <li>{@link MinVertexDuration}</li>
 *   <li>{@link MaxEdgeDuration}</li>
 * </ul>
 *
 */
public class MinMaxDurationTest extends TemporalGradoopTestBase {

  /**
   * Test the implementation of {@link MaxDuration#getIncrement(TemporalElement)} for valid time.
   */
  @Test
  public void testGetIncrementForValidTimeMaxDuration() {
    MaxDuration function = new MaxDuration("", VALID_TIME);
    TemporalElement testElement = getVertexFactory().createVertex();
    testElement.setValidTime(Tuple2.of(DEFAULT_TIME_FROM, DEFAULT_TIME_TO));
    assertEquals(Long.MIN_VALUE, function.getIncrement(testElement).getLong());
    testElement.setValidTime(Tuple2.of(DEFAULT_TIME_FROM, 6L));
    assertEquals(Long.MIN_VALUE, function.getIncrement(testElement).getLong());
    testElement.setValidTime(Tuple2.of(3L, DEFAULT_TIME_TO));
    assertEquals(Long.MIN_VALUE, function.getIncrement(testElement).getLong());
    testElement.setValidTime(Tuple2.of(2L, 7L));
    assertEquals(5L, function.getIncrement(testElement).getLong());
  }

  /**
   * Test the {@link MaxDuration} aggregate function and its subclasses in an aggregation.
   *
   * @throws Exception when the execution in Flink fails.
   */
  @Test
  public void testWithMaxAggregation() throws Exception {

    TemporalGraphFactory graphFactory = getConfig().getTemporalGraphFactory();
    VertexFactory<TemporalVertex> vertexFactory = graphFactory.getVertexFactory();
    EdgeFactory<TemporalEdge> edgeFactory = graphFactory.getEdgeFactory();
    TemporalVertex v1 = vertexFactory.createVertex();
    v1.setTransactionTime(Tuple2.of(0L, 2000L));
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
    v5.setValidTime(Tuple2.of(-100L, -3L));

    TemporalEdge e1 = edgeFactory.createEdge(v1.getId(), v2.getId());
    e1.setTransactionTime(Tuple2.of(0L, 300L));
    e1.setValidTime(Tuple2.of(DEFAULT_TIME_FROM, DEFAULT_TIME_TO));
    TemporalEdge e2 = edgeFactory.createEdge(v2.getId(), v3.getId());
    e2.setTransactionTime(Tuple2.of(DEFAULT_TIME_FROM, 7L));
    e2.setValidTime(Tuple2.of(-100L, DEFAULT_TIME_TO));
    TemporalEdge e3 = edgeFactory.createEdge(v3.getId(), v1.getId());
    e3.setTransactionTime(Tuple2.of(-1L, DEFAULT_TIME_TO));
    e3.setValidTime(Tuple2.of(DEFAULT_TIME_FROM, -80L));
    TemporalEdge e4 = edgeFactory.createEdge(v1.getId(), v3.getId());
    e4.setTransactionTime(Tuple2.of(DEFAULT_TIME_FROM, DEFAULT_TIME_TO));
    e4.setValidTime(Tuple2.of(-120L, 100L));
    TemporalEdge e5 = edgeFactory.createEdge(v2.getId(), v4.getId());
    e5.setTransactionTime(Tuple2.of(10L, 50L));
    e5.setValidTime(Tuple2.of(-301L, -276L));

    DataSet<TemporalVertex> vertices = getExecutionEnvironment().fromElements(v1, v2, v3, v4, v5);
    DataSet<TemporalEdge> edges = getExecutionEnvironment().fromElements(e1, e2, e3, e4, e5);
    TemporalGraph temporalResult = graphFactory.fromDataSets(vertices, edges)
      .aggregate(new MaxDuration("maxDurTx", TRANSACTION_TIME),
        new MaxDuration("maxDurValid", VALID_TIME),
        new MaxVertexDuration("maxVertexDurTx", TRANSACTION_TIME),
        new MaxVertexDuration("maxVertexDurValid", VALID_TIME),
        new MaxEdgeDuration("maxEdgeDurTx", TRANSACTION_TIME),
        new MaxEdgeDuration("maxEdgeDurValid", VALID_TIME));

    LogicalGraph result = temporalResult.toLogicalGraph();
    List<EPGMGraphHead> graphHead = new ArrayList<>();
    result.getGraphHead().output(new LocalCollectionOutputFormat<>(graphHead));
    getExecutionEnvironment().execute();

    Properties headProperties = graphHead.get(0).getProperties();
    assertEquals(2000L, headProperties.get("maxDurTx").getLong());
    assertEquals(220L, headProperties.get("maxDurValid").getLong());
    assertEquals(2000L, headProperties.get("maxVertexDurTx").getLong());
    assertEquals(97L, headProperties.get("maxVertexDurValid").getLong());
    assertEquals(300L, headProperties.get("maxEdgeDurTx").getLong());
    assertEquals(220L, headProperties.get("maxEdgeDurValid").getLong());
  }

  /**
   * Test the implementation of {@link MinDuration#getIncrement(TemporalElement)} for valid time.
   */
  @Test
  public void testGetIncrementForValidTimeMinDuration() {
    MinDuration function = new MinDuration("", VALID_TIME);
    TemporalElement testElement = getVertexFactory().createVertex();
    testElement.setValidTime(Tuple2.of(DEFAULT_TIME_FROM, DEFAULT_TIME_TO));
    assertEquals(DEFAULT_TIME_TO.longValue(), function.getIncrement(testElement).getLong());
    testElement.setValidTime(Tuple2.of(DEFAULT_TIME_FROM, 6L));
    assertEquals(DEFAULT_TIME_TO.longValue(), function.getIncrement(testElement).getLong());
    testElement.setValidTime(Tuple2.of(3L, DEFAULT_TIME_TO));
    assertEquals(DEFAULT_TIME_TO.longValue(), function.getIncrement(testElement).getLong());
    testElement.setValidTime(Tuple2.of(2L, 7L));
    assertEquals(5L, function.getIncrement(testElement).getLong());
  }

  /**
   * Test the {@link MinDuration} aggregate function and its subclasses in an aggregation.
   *
   * @throws Exception when the execution in Flink fails.
   */
  @Test
  public void testWithMinAggregation() throws Exception {
    TemporalGraphFactory graphFactory = getConfig().getTemporalGraphFactory();
    VertexFactory<TemporalVertex> vertexFactory = graphFactory.getVertexFactory();
    EdgeFactory<TemporalEdge> edgeFactory = graphFactory.getEdgeFactory();
    TemporalVertex v1 = vertexFactory.createVertex();
    v1.setTransactionTime(Tuple2.of(0L, 2L));
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
    v5.setValidTime(Tuple2.of(-100L, -3L));

    TemporalEdge e1 = edgeFactory.createEdge(v1.getId(), v2.getId());
    e1.setTransactionTime(Tuple2.of(0L, 10L));
    e1.setValidTime(Tuple2.of(DEFAULT_TIME_FROM, DEFAULT_TIME_TO));
    TemporalEdge e2 = edgeFactory.createEdge(v2.getId(), v3.getId());
    e2.setTransactionTime(Tuple2.of(DEFAULT_TIME_FROM, 7L));
    e2.setValidTime(Tuple2.of(-100L, DEFAULT_TIME_TO));
    TemporalEdge e3 = edgeFactory.createEdge(v3.getId(), v1.getId());
    e3.setTransactionTime(Tuple2.of(-1L, DEFAULT_TIME_TO));
    e3.setValidTime(Tuple2.of(DEFAULT_TIME_FROM, -80L));
    TemporalEdge e4 = edgeFactory.createEdge(v1.getId(), v3.getId());
    e4.setTransactionTime(Tuple2.of(DEFAULT_TIME_FROM, DEFAULT_TIME_TO));
    e4.setValidTime(Tuple2.of(-120L, 100L));
    TemporalEdge e5 = edgeFactory.createEdge(v2.getId(), v4.getId());
    e5.setTransactionTime(Tuple2.of(10L, 50L));
    e5.setValidTime(Tuple2.of(-302L, -298L));

    DataSet<TemporalVertex> vertices = getExecutionEnvironment().fromElements(v1, v2, v3, v4, v5);
    DataSet<TemporalEdge> edges = getExecutionEnvironment().fromElements(e1, e2, e3, e4, e5);
    TemporalGraph temporalResult = graphFactory.fromDataSets(vertices, edges)
      .aggregate(
        new MinDuration("minDurTx", TRANSACTION_TIME),
        new MinDuration("minDurValid", VALID_TIME),
        new MinVertexDuration("minVertexDurTx", TRANSACTION_TIME),
        new MinVertexDuration("minVertexDurValid", VALID_TIME),
        new MinEdgeDuration("minEdgeDurTx", TRANSACTION_TIME),
        new MinEdgeDuration("minEdgeDurValid", VALID_TIME));
    LogicalGraph result = temporalResult.toLogicalGraph();

    List<EPGMGraphHead> graphHead = new ArrayList<>();
    result.getGraphHead().output(new LocalCollectionOutputFormat<>(graphHead));
    getExecutionEnvironment().execute();

    Properties headProperties = graphHead.get(0).getProperties();
    assertEquals(2L, headProperties.get("minDurTx").getLong());
    assertEquals(3L, headProperties.get("minDurValid").getLong());
    assertEquals(2L, headProperties.get("minVertexDurTx").getLong());
    assertEquals(3L, headProperties.get("minVertexDurValid").getLong());
    assertEquals(10L, headProperties.get("minEdgeDurTx").getLong());
    assertEquals(4L, headProperties.get("minEdgeDurValid").getLong());
  }

  /**
   * Test the {@link MinDuration#postAggregate(PropertyValue)} and the
   * {@link MaxDuration#postAggregate(PropertyValue)} functions. If all the time data of temporal elements in
   * a graph is set to default, all the duration aggregate functions return null
   *
   * @throws Exception when the execution in Flink fails.
   */
  @Test
  public void testDurationWithDefaultValues() throws Exception {
    LogicalGraph logicalGraph  = getSocialNetworkLoader().getLogicalGraph();
    TemporalGraph temporalGraph = TemporalGraph.fromGraph(logicalGraph);
    temporalGraph = temporalGraph.aggregate(
      new MinDuration("minDur", VALID_TIME),
      new MaxDuration("maxDur", VALID_TIME));
    LogicalGraph result = temporalGraph.toLogicalGraph();
    List<EPGMGraphHead> graphHead = new ArrayList<>();
    result.getGraphHead().output(new LocalCollectionOutputFormat<>(graphHead));
    getExecutionEnvironment().execute();

    PropertyValue test = graphHead.get(0).getPropertyValue("minDur");
    assertNotNull(test);
    assertEquals(PropertyValue.NULL_VALUE, test);
    test = graphHead.get(0).getPropertyValue("maxDur");
    assertNotNull(test);
    assertEquals(PropertyValue.NULL_VALUE, test);
  }
}
