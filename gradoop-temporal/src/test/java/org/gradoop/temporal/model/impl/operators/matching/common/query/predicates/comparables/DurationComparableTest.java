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
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.EmbeddingMetaData;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.gradoop.temporal.model.impl.pojo.TemporalVertexFactory;
import org.junit.Test;
import org.s1ck.gdl.model.comparables.time.Duration;
import org.s1ck.gdl.model.comparables.time.MaxTimePoint;
import org.s1ck.gdl.model.comparables.time.MinTimePoint;
import org.s1ck.gdl.model.comparables.time.TimeLiteral;
import org.s1ck.gdl.model.comparables.time.TimeSelector;

import static org.junit.Assert.assertEquals;
import static org.s1ck.gdl.model.comparables.time.TimeSelector.TimeField.TX_FROM;
import static org.s1ck.gdl.model.comparables.time.TimeSelector.TimeField.TX_TO;
import static org.s1ck.gdl.model.comparables.time.TimeSelector.TimeField.VAL_FROM;
import static org.s1ck.gdl.model.comparables.time.TimeSelector.TimeField.VAL_TO;

public class DurationComparableTest {

  @Test
  public void testOnEmbedding() {
    Embedding embedding = new Embedding();
    EmbeddingMetaData metaData = new EmbeddingMetaData();

    embedding.addPropertyValues(PropertyValue.create(5L), PropertyValue.create(50L),
      PropertyValue.create("foo"), PropertyValue.create(42L));
    metaData.setPropertyColumn("a", VAL_FROM.toString(), 0);
    metaData.setPropertyColumn("a", VAL_TO.toString(), 1);
    metaData.setPropertyColumn("b", VAL_TO.toString(), 3);

    TimeSelector aValFrom = new TimeSelector("a", TimeSelector.TimeField.VAL_FROM);
    TimeSelector aValTo = new TimeSelector("a", VAL_TO);
    TimeSelector bValTo = new TimeSelector("b", VAL_TO);
    TimeLiteral l1 = new TimeLiteral("1970-01-01T00:00:00");

    Duration aVal = new Duration(aValFrom, aValTo);
    DurationComparable aValWrapper = new DurationComparable(aVal);

    assertEquals(aValWrapper.evaluate(embedding, metaData).getLong(), 45L);

    Duration d2 = new Duration(l1, bValTo);
    assertEquals(new DurationComparable(d2).evaluate(embedding, metaData).getLong(), 42L);

    TimeLiteral l2 = new TimeLiteral("1970-01-01T00:00:01");
    Duration d3 = new Duration(l1, l2);
    assertEquals(new DurationComparable(d3).evaluate(embedding, metaData).getLong(), 1000L);
  }

  @Test
  public void testOnElement() {
    TemporalVertex vertex = new TemporalVertexFactory().createVertex();
    vertex.setTransactionTime(new Tuple2<>(0L, 100L));
    vertex.setValidTime(new Tuple2<>(10L, 90L));

    TimeSelector txFrom = new TimeSelector("a", TX_FROM);
    TimeSelector txTo = new TimeSelector("a", TX_TO);
    Duration d1 = new Duration(txFrom, txTo);
    DurationComparable wrapper1 = new DurationComparable(d1);

    assertEquals(wrapper1.evaluate(vertex).getLong(), 100L);

    MinTimePoint mn = new MinTimePoint(txTo, new TimeSelector("a", VAL_TO));
    MaxTimePoint mx = new MaxTimePoint(new TimeLiteral("1970-01-01T00:00:01"), txFrom);

    Duration d2 = new Duration(mn, mx);
    DurationComparable wrapper2 = new DurationComparable(d2);
    assertEquals(wrapper2.evaluate(vertex).getLong(), 910L);
  }
}
