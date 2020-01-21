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
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.api.entities.EdgeFactory;
import org.gradoop.common.model.api.entities.VertexFactory;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.TemporalGraphFactory;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalElement;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.gradoop.temporal.model.impl.pojo.TemporalVertexFactory;
import org.gradoop.temporal.util.TemporalGradoopTestBase;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Arrays;

import static org.gradoop.common.model.impl.properties.PropertyValue.create;
import static org.gradoop.flink.model.impl.operators.aggregation.functions.average.Average.IGNORED_VALUE;
import static org.gradoop.temporal.model.api.TimeDimension.TRANSACTION_TIME;
import static org.gradoop.temporal.model.api.TimeDimension.VALID_TIME;
import static org.gradoop.temporal.model.impl.pojo.TemporalElement.DEFAULT_TIME_FROM;
import static org.gradoop.temporal.model.impl.pojo.TemporalElement.DEFAULT_TIME_TO;
import static org.testng.AssertJUnit.assertEquals;

/**
 * Test for the {@link AverageDuration} aggregate function.
 */
public class AverageDurationTest extends TemporalGradoopTestBase {

  /**
   * Factory used to instantiate {@link TemporalVertex} objects.
   */
  private TemporalVertexFactory factory;

  /**
   * Set up method initializes {@link TemporalVertexFactory}.
   */
  @BeforeClass
  public void setUp() {
    this.factory = new TemporalVertexFactory();
  }

  /**
   * Test the implementation of {@link AverageDuration#getIncrement(TemporalElement)} for transaction time.
   */
  @Test(dataProvider = "txTimeProvider")
  public void testGetIncrementForTxTime(TemporalElement actualElement, PropertyValue expected) {
    AverageDuration function = new AverageDuration("", TRANSACTION_TIME);

    assertEquals(function.getIncrement(actualElement), expected);
  }

  /**
   * Provides {@link TemporalVertex} objects with varying transaction times and the expected output of
   * {@link AverageDuration#getIncrement(TemporalElement)}.
   * <br>
   * Provided params:
   * <ol start="0">
   * <li>Temporal element</li>
   * <li>Expected output of {@link AverageDuration#getIncrement(TemporalElement)}</li>
   * </ol>
   *
   * @return Object[][]
   */
  @DataProvider(name = "txTimeProvider")
  public  Object[][] txTimeParameters() {
    TemporalVertex v0 = factory.createVertex();
    v0.setTransactionTime(Tuple2.of(DEFAULT_TIME_FROM, DEFAULT_TIME_TO));
    TemporalVertex v1 = factory.createVertex();
    v1.setTransactionTime(Tuple2.of(DEFAULT_TIME_FROM, 7L));
    TemporalVertex v2 = factory.createVertex();
    v2.setTransactionTime(Tuple2.of(4L, DEFAULT_TIME_TO));
    TemporalVertex v3 = factory.createVertex();
    v3.setTransactionTime(Tuple2.of(1L, 11L));

    return new Object[][]{
      {v0, IGNORED_VALUE},
      {v1, IGNORED_VALUE},
      {v2, IGNORED_VALUE},
      {v3, create(Arrays.asList(create(10L), create(1L)))}
    };
  }

  /**
   * Test the implementation of {@link AverageDuration#getIncrement(TemporalElement)} for valid time.
   */
  @Test(dataProvider = "validTimeProvider")
  public void testGetIncrementForValidTime(TemporalElement actualElement, PropertyValue expected) {
    AverageDuration function = new AverageDuration("", VALID_TIME);

    assertEquals(expected, function.getIncrement(actualElement));
  }

  /**
   * Provides {@link TemporalVertex} objects with varying valid times and the expected output of
   * {@link AverageDuration#getIncrement(TemporalElement)}.
   * <br>
   * Provided params:
   * <ol start="0">
   * <li>Temporal element</li>
   * <li>Expected output of {@link AverageDuration#getIncrement(TemporalElement)}</li>
   * </ol>
   *
   * @return Object[][]
   */
  @DataProvider(name = "validTimeProvider")
  public  Object[][] validTimeParameters() {
    TemporalVertex v0 = factory.createVertex();
    v0.setValidTime(Tuple2.of(DEFAULT_TIME_FROM, DEFAULT_TIME_TO));
    TemporalVertex v1 = factory.createVertex();
    v1.setValidTime(Tuple2.of(DEFAULT_TIME_FROM, 7L));
    TemporalVertex v2 = factory.createVertex();
    v2.setValidTime(Tuple2.of(4L, DEFAULT_TIME_TO));
    TemporalVertex v3 = factory.createVertex();
    v3.setValidTime(Tuple2.of(2L, 7L));

    return new Object[][]{
      {v0, IGNORED_VALUE},
      {v1, IGNORED_VALUE},
      {v2, IGNORED_VALUE},
      {v3, create(Arrays.asList(create(5L), create(1L)))}
    };
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
