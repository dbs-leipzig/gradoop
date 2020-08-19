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
package org.gradoop.temporal.model.impl.operators.matching.common.query.predicates.comparables;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.EmbeddingMetaData;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.gradoop.temporal.model.impl.pojo.TemporalVertexFactory;
import org.junit.Test;
import org.s1ck.gdl.model.comparables.time.MaxTimePoint;
import org.s1ck.gdl.model.comparables.time.TimeLiteral;
import org.s1ck.gdl.model.comparables.time.TimeSelector;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

import static org.junit.Assert.assertEquals;
import static org.s1ck.gdl.model.comparables.time.TimeSelector.TimeField.TX_TO;
import static org.s1ck.gdl.model.comparables.time.TimeSelector.TimeField.VAL_TO;

public class MaxTimePointComparableTest {

  @Test
  public void testMaxTimePoint() {
    //-----------------------------------------------
    // test data
    //-----------------------------------------------
    String timeString1 = "1972-02-12T13:25:03";
    Long t1 = dateStringToLong(timeString1);
    TimeSelector s1 = new TimeSelector("a", TX_TO);
    PropertyValue t1prop = PropertyValue.create(t1);

    String timeString2 = "2020-05-05T00:00:00";
    Long t2 = dateStringToLong(timeString2);
    TimeSelector s2 = new TimeSelector("b", TX_TO);
    PropertyValue t2prop = PropertyValue.create(t2);

    String timeString3 = "1970-01-03T03:04:05";
    Long t3 = dateStringToLong(timeString3);
    TimeLiteral l3 = new TimeLiteral(timeString3);

    MaxTimePoint mtp = new MaxTimePoint(s1, s2, l3);
    MaxTimePointComparable maxTimePoint = new MaxTimePointComparable(mtp);
    PropertyValue reference = PropertyValue.create(t2);
    //-----------------------------------------
    // evaluate on an embedding
    //-----------------------------------------
    Embedding embedding = new Embedding();
    GradoopId aId = GradoopId.get();
    GradoopId bId = GradoopId.get();
    embedding.add(aId, new PropertyValue[] {t1prop});
    embedding.add(bId, new PropertyValue[] {t2prop});

    EmbeddingMetaData metaData = new EmbeddingMetaData();
    metaData.setEntryColumn("a", EmbeddingMetaData.EntryType.VERTEX, 0);
    metaData.setPropertyColumn("a", TX_TO.toString(), 0);
    metaData.setEntryColumn("b", EmbeddingMetaData.EntryType.VERTEX, 1);
    metaData.setPropertyColumn("b", TX_TO.toString(), 1);

    assertEquals(reference, maxTimePoint.evaluate(embedding, metaData));
    //----------------------------------------------------
    // evaluate on a GraphElement
    //----------------------------------------------------
    TemporalVertex vertex = new TemporalVertexFactory().createVertex();
    vertex.setTransactionTime(new Tuple2<>(1L, t1));
    vertex.setValidTime(new Tuple2<>(1L, t2));

    TimeSelector selector1 = new TimeSelector("a", VAL_TO);
    TimeSelector selector2 = new TimeSelector("a", TX_TO);
    mtp = new MaxTimePoint(selector1, selector2, l3);
    maxTimePoint = new MaxTimePointComparable(mtp);

    assertEquals(reference, maxTimePoint.evaluate(vertex));
  }

  private Long dateStringToLong(String date) {
    return LocalDateTime.parse(date).toInstant(ZoneOffset.ofHours(0)).toEpochMilli();
  }
}
