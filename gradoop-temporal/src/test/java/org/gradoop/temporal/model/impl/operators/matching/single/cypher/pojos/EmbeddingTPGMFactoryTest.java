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

package org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalEdgeFactory;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.gradoop.temporal.model.impl.pojo.TemporalVertexFactory;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.s1ck.gdl.model.comparables.time.TimeSelector.TimeField.TX_FROM;
import static org.s1ck.gdl.model.comparables.time.TimeSelector.TimeField.TX_TO;
import static org.s1ck.gdl.model.comparables.time.TimeSelector.TimeField.VAL_FROM;
import static org.s1ck.gdl.model.comparables.time.TimeSelector.TimeField.VAL_TO;

public class EmbeddingTPGMFactoryTest {

  @Test
  public void testFromVertex() throws Exception {
    Properties properties = new Properties();
    properties.set("foo", 1);
    properties.set("bar", "42");
    properties.set("baz", false);
    TemporalVertex vertex = new TemporalVertexFactory().createVertex("TestVertex", properties);
    Long valFrom = 123456L;
    Long txTo = 124L;
    Tuple2<Long, Long> tx = new Tuple2<>(123L, txTo);
    vertex.setTransactionTime(tx);
    Tuple2<Long, Long> val = new Tuple2<>(valFrom, 12456787L);
    vertex.setValidTime(val);

    Embedding embedding =
      EmbeddingTPGMFactory.fromVertex(vertex, Lists.newArrayList("foo", "bar", VAL_FROM.toString(),
        TX_TO.toString()));

    assertEquals(1, embedding.size());
    assertEquals(vertex.getId(), embedding.getId(0));
    assertEquals(PropertyValue.create(1), embedding.getProperty(0));
    assertEquals(PropertyValue.create("42"), embedding.getProperty(1));
    assertEquals(PropertyValue.create(valFrom), embedding.getProperty(2));
    assertEquals(PropertyValue.create(txTo), embedding.getProperty(3));
  }

  @Test
  public void testFromEdge() throws Exception {
    Properties properties = new Properties();
    properties.set("foo", 1);
    properties.set("bar", "42");
    properties.set("baz", false);
    TemporalEdge edge = new TemporalEdgeFactory().createEdge(
      "TestVertex", GradoopId.get(), GradoopId.get(), properties
    );
    Long txFrom = 1253453L;
    Long valTo = 127834487L;
    Tuple2<Long, Long> tx = new Tuple2<>(txFrom, 124346557L);
    edge.setTransactionTime(tx);
    Tuple2<Long, Long> val = new Tuple2<>(13456L, valTo);
    edge.setValidTime(val);

    Embedding embedding =
      EmbeddingTPGMFactory.fromEdge(edge, Lists.newArrayList(
        TX_FROM.toString(), "foo", VAL_TO.toString(), "bar"), false);

    assertEquals(3, embedding.size());
    assertEquals(edge.getSourceId(), embedding.getId(0));
    assertEquals(edge.getId(), embedding.getId(1));
    assertEquals(edge.getTargetId(), embedding.getId(2));
    assertEquals(PropertyValue.create(txFrom), embedding.getProperty(0));
    assertEquals(PropertyValue.create(1), embedding.getProperty(1));
    assertEquals(PropertyValue.create(valTo), embedding.getProperty(2));
    assertEquals(PropertyValue.create("42"), embedding.getProperty(3));

  }

}

