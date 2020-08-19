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

import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.EmbeddingMetaData;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.gradoop.temporal.model.impl.pojo.TemporalVertexFactory;
import org.junit.Test;
import org.s1ck.gdl.model.comparables.time.TimeLiteral;

import java.time.LocalDateTime;
import java.time.ZoneOffset;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class TimeLiteralComparableTest {

  @Test
  public void testEvaluationReturnsTimeLiteral() {
    //-----------------------------------------------
    // test data
    //-----------------------------------------------
    String timeString1 = "1972-02-12T13:25:03";
    TimeLiteral literal = new TimeLiteral(timeString1);
    TimeLiteralComparable wrapper = new TimeLiteralComparable(literal);
    PropertyValue reference = PropertyValue.create(dateStringToLong(timeString1));

    //------------------------------------------------
    // test on embeddings
    //------------------------------------------------
    assertEquals(reference, wrapper.evaluate(new Embedding(), new EmbeddingMetaData()));
    assertNotEquals(PropertyValue.create(timeString1),
      wrapper.evaluate(null, null));
    //---------------------------------------------------
    // test on GraphElement
    //---------------------------------------------------
    TemporalVertex vertex = new TemporalVertexFactory().createVertex();
    assertEquals(reference, wrapper.evaluate(vertex));
  }

  private Long dateStringToLong(String date) {
    return LocalDateTime.parse(date).toInstant(ZoneOffset.ofHours(0)).toEpochMilli();
  }
}
