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
package org.gradoop.temporal.model.impl.operators.matching.single.cypher.functions;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.api.entities.Element;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.EmbeddingMetaData;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.utils.ExpandDirection;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGM;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGMMetaData;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalEdgeFactory;
import org.gradoop.temporal.model.impl.pojo.TemporalElement;
import org.gradoop.temporal.model.impl.pojo.TemporalGraphHead;
import org.gradoop.temporal.model.impl.pojo.TemporalGraphHeadFactory;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.gradoop.temporal.model.impl.pojo.TemporalVertexFactory;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class ElementsFromEmbeddingTPGMTest {

  static final TemporalGraphHeadFactory ghFactory = new TemporalGraphHeadFactory();
  static final TemporalVertexFactory vFactory = new TemporalVertexFactory();
  static final TemporalEdgeFactory eFactory = new TemporalEdgeFactory();
  static Long[] defaultTime = new Long[] {TemporalElement.DEFAULT_TIME_FROM, TemporalElement.DEFAULT_TIME_TO,
    TemporalElement.DEFAULT_TIME_FROM, TemporalElement.DEFAULT_TIME_TO};

  @Test
  public void testSimpleElementFromEmbeddingTPGM() throws Exception {
    EmbeddingTPGM embedding = new EmbeddingTPGM();
    EmbeddingTPGMMetaData metaData = new EmbeddingTPGMMetaData();

    //------------------------------------
    // Vertices
    //------------------------------------
    //ids
    GradoopId v1 = GradoopId.get();
    GradoopId v2 = GradoopId.get();
    GradoopId v3 = GradoopId.get();
    //times
    Long[] v1Time = getRandomTime();
    Long[] v2Time = getRandomTime();
    Long[] v3Time = getRandomTime();

    //insert into embedding and metadata
    embedding.add(v1);
    embedding.add(v2);
    embedding.add(v3);

    for (Long[] time : new Long[][] {v1Time, v2Time, v3Time}) {
      embedding.addTimeData(time[0], time[1], time[2], time[3]);
    }

    metaData.setEntryColumn("v1", EmbeddingMetaData.EntryType.VERTEX, 0);
    metaData.setEntryColumn("v2", EmbeddingMetaData.EntryType.VERTEX, 1);
    metaData.setEntryColumn("v3", EmbeddingMetaData.EntryType.VERTEX, 2);
    metaData.setTimeColumn("v1", 0);
    metaData.setTimeColumn("v2", 1);
    metaData.setTimeColumn("v3", 2);


    //------------------------------------
    // Edges/Paths
    //------------------------------------
    //ids
    GradoopId e1 = GradoopId.get();
    //times
    Long[] e1Time = getRandomTime();

    //labels

    //insert into embedding and metadata
    embedding.add(e1);
    //path
    embedding.addTimeData(e1Time[0], e1Time[1], e1Time[2], e1Time[3]);


    metaData.setEntryColumn("e1", EmbeddingMetaData.EntryType.EDGE, 3);

    metaData.setTimeColumn("e1", 3);
    metaData.setDirection("e1", ExpandDirection.OUT);


    HashMap<String, Pair<String, String>> sourceTargetVariables = new HashMap<>();
    sourceTargetVariables.put("e1", Pair.of("v1", "v2"));

    HashMap<String, String> labelMapping = new HashMap<>();
    labelMapping.put("v1", "v1Label");
    labelMapping.put("v2", "v2Label");
    //labelMapping.put("v3", "v3Label");
    labelMapping.put("e1", "e1Label");

    DataSet<EmbeddingTPGM> input = ExecutionEnvironment.getExecutionEnvironment()
      .fromElements(embedding);

    List<Element> result = input.flatMap(
      new ElementsFromEmbeddingTPGM<TemporalGraphHead,
        TemporalVertex, TemporalEdge>(ghFactory, vFactory, eFactory,
        metaData, sourceTargetVariables, labelMapping)).collect();


    checkElement((TemporalVertex) result.get(0), v1, "v1Label", v1Time);
    checkElement((TemporalVertex) result.get(1), v2, "v2Label", v2Time);
    checkElement((TemporalVertex) result.get(2), v3, null, v3Time);


    checkElement((TemporalEdge) result.get(3), e1, "e1Label", e1Time);

    TemporalGraphHead head = (TemporalGraphHead) result.get(4);
    checkHead(head, v1Time, v2Time, v3Time, e1Time);
  }

  @Test
  public void testPathsFromEmbeddingTPGM() throws Exception {
    GradoopId source = GradoopId.get();
    GradoopId target = GradoopId.get();
    Long[] sourceTime = getRandomTime();
    Long[] targetTime = getRandomTime();

    GradoopId e1 = GradoopId.get();
    GradoopId v = GradoopId.get();
    GradoopId e2 = GradoopId.get();
    GradoopId w = GradoopId.get();
    GradoopId e3 = GradoopId.get();

    EmbeddingTPGM embedding = new EmbeddingTPGM();
    embedding.add(source);
    embedding.add(e1, v, e2, w, e3);
    embedding.add(target);

    embedding.addTimeData(sourceTime[0], sourceTime[1], sourceTime[2], sourceTime[3]);
    embedding.addTimeData(targetTime[0], targetTime[1], targetTime[2], targetTime[3]);


    EmbeddingTPGMMetaData metaData = new EmbeddingTPGMMetaData();
    metaData.setEntryColumn("source", EmbeddingMetaData.EntryType.VERTEX, 0);
    metaData.setEntryColumn("p", EmbeddingMetaData.EntryType.PATH, 1);
    metaData.setEntryColumn("target", EmbeddingMetaData.EntryType.VERTEX, 2);

    metaData.setTimeColumn("source", 0);
    metaData.setTimeColumn("target", 1);

    metaData.setDirection("p", ExpandDirection.OUT);

    HashMap<String, String> labelMapping = new HashMap<>();
    labelMapping.put("source", "sourceLabel");
    labelMapping.put("target", "targetLabel");

    HashMap<String, Pair<String, String>> sourceTargetVariables = new HashMap<>();
    sourceTargetVariables.put("p", Pair.of("source", "target"));

    DataSet<EmbeddingTPGM> input = ExecutionEnvironment.getExecutionEnvironment()
      .fromElements(embedding);

    List<Element> result = input.flatMap(
      new ElementsFromEmbeddingTPGM<TemporalGraphHead,
        TemporalVertex, TemporalEdge>(ghFactory, vFactory, eFactory,
        metaData, sourceTargetVariables, labelMapping)).collect();

    // 7 elements + graph head
    assertEquals(result.size(), 8);
    checkElement((TemporalVertex) result.get(0), source, "sourceLabel", sourceTime);
    checkElement((TemporalVertex) result.get(1), target, "targetLabel", targetTime);
    Map<PropertyValue, PropertyValue> variableMapping = result.get(7).
      getPropertyValue("__variable_mapping").getMap();
    List<PropertyValue> pValues = variableMapping.get(PropertyValue.create("p"))
      .getList();
    assertEquals(pValues.get(0).getGradoopId(), e1);
    assertEquals(pValues.get(1).getGradoopId(), v);
    assertEquals(pValues.get(2).getGradoopId(), e2);
    assertEquals(pValues.get(3).getGradoopId(), w);
    assertEquals(pValues.get(4).getGradoopId(), e3);

    TemporalGraphHead head = (TemporalGraphHead) result.get(7);
    checkHead(head, sourceTime, targetTime);

  }

  @Test
  public void testDeadPattern() throws Exception {
    EmbeddingTPGM embedding = new EmbeddingTPGM();
    EmbeddingTPGMMetaData metaData = new EmbeddingTPGMMetaData();

    //------------------------------------
    // Vertices
    //------------------------------------
    //ids
    GradoopId v1 = GradoopId.get();
    GradoopId v2 = GradoopId.get();
    //times
    Long[] v1Time = new Long[] {1L, 2L, 1L, 2L};
    Long[] v2Time = new Long[] {3L, 4L, 3L, 4L};

    //insert into embedding and metadata
    embedding.add(v1);
    embedding.add(v2);

    for (Long[] time : new Long[][] {v1Time, v2Time}) {
      embedding.addTimeData(time[0], time[1], time[2], time[3]);
    }

    metaData.setEntryColumn("v1", EmbeddingMetaData.EntryType.VERTEX, 0);
    metaData.setEntryColumn("v2", EmbeddingMetaData.EntryType.VERTEX, 1);
    metaData.setTimeColumn("v1", 0);
    metaData.setTimeColumn("v2", 1);

    DataSet<EmbeddingTPGM> input = ExecutionEnvironment.getExecutionEnvironment()
      .fromElements(embedding);

    List<Element> result = input.flatMap(
      new ElementsFromEmbeddingTPGM<TemporalGraphHead,
        TemporalVertex, TemporalEdge>(ghFactory, vFactory, eFactory,
        metaData, new HashMap<>(), new HashMap<>())).collect();

    assertEquals(result.size(), 3);
    TemporalGraphHead head = (TemporalGraphHead) result.get(2);
    checkHead(head, v1Time, v2Time);
  }

  private void checkHead(TemporalGraphHead head, Long[]... times) {

    Long[] txFroms = new Long[times.length];
    Long[] txTos = new Long[times.length];
    Long[] valFroms = new Long[times.length];
    Long[] valTos = new Long[times.length];
    for (int i = 0; i < times.length; i++) {
      txFroms[i] = times[i][0];
      txTos[i] = times[i][1];
      valFroms[i] = times[i][2];
      valTos[i] = times[i][3];
    }
    Long txFrom = getMax(txFroms);
    Long txTo = getMin(txTos);
    if (txFrom > txTo) {
      txFrom = Long.MIN_VALUE;
      txTo = Long.MIN_VALUE;
    }
    Long valFrom = getMax(valFroms);
    Long valTo = getMin(valTos);
    if (valFrom > valTo) {
      valFrom = Long.MIN_VALUE;
      valTo = Long.MIN_VALUE;
    }

    if (txTo > txFrom) {
      assertEquals(head.getPropertyValue("hasTxLifetime"), PropertyValue.create(true));
    } else {
      assertEquals(head.getPropertyValue("hasTxLifetime"), PropertyValue.create(false));
    }

    if (valTo > valFrom) {
      assertEquals(head.getPropertyValue("hasValidLifetime"), PropertyValue.create(true));
    } else {
      assertEquals(head.getPropertyValue("hasValidLifetime"), PropertyValue.create(false));
    }

    Long txDuration = (txTo - txFrom > 0) ? txTo - txFrom : 0L;

    Long validDuration = (valTo - valFrom > 0) ? valTo - valFrom : 0L;
    assertEquals(head.getValidFrom(), valFrom);
    assertEquals(head.getValidTo(), valTo);
    assertEquals(head.getTxFrom(), txFrom);
    assertEquals(head.getTxTo(), txTo);
    assertEquals((Long) head.getPropertyValue("txLifetime").getLong(), txDuration);
    assertEquals((Long) head.getPropertyValue("validLifetime").getLong(), validDuration);
  }

  private long getMin(Long... args) {
    long min = Long.MAX_VALUE;
    for (long arg : args) {
      if (arg < min) {
        min = arg;
      }
    }
    return min;
  }

  private long getMax(Long... args) {
    long max = Long.MIN_VALUE;
    for (long arg : args) {
      if (arg > max) {
        max = arg;
      }
    }
    return max;
  }

  private void checkElement(TemporalElement v, GradoopId id, String label, Long[] time) {
    assertEquals(v.getLabel(), label);
    assertEquals(v.getTransactionTime(), new Tuple2<>(time[0], time[1]));
    assertEquals(v.getValidTime(), new Tuple2<>(time[2], time[3]));
    assertEquals(v.getId(), id);
  }

  private Long[] getRandomTime() {
    int minFrom = 0;
    int maxFrom = 100;
    int minTo = 500;
    int maxTo = 1000;
    Long txFrom = (long) (Math.random() * maxFrom);
    Long txTo = minTo + ((long) (Math.random() * maxTo));
    Long validFrom = (long) (Math.random() * maxFrom);
    Long validTo = minTo + ((long) (Math.random() * maxTo));
    return new Long[] {txFrom, txTo, validFrom, validTo};
  }
}
