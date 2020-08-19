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

import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.EmbeddingMetaData;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.gradoop.temporal.model.impl.pojo.TemporalVertexFactory;
import org.junit.Test;
import org.s1ck.gdl.model.comparables.time.TimeConstant;

import static org.junit.Assert.assertEquals;

public class TimeConstantComparableTest {

  @Test
  public void timeConstantTest() {
    TimeConstant c = new TimeConstant(1000L);
    TimeConstantComparable wrapper = new TimeConstantComparable(c);

    // evaluate on embedding
    Embedding embedding = new Embedding();
    EmbeddingMetaData metaData = new EmbeddingMetaData();

    assertEquals(1000L, wrapper.evaluate(embedding, metaData).getLong());

    // evaluate on temporal element
    TemporalVertex vertex = new TemporalVertexFactory().createVertex();
    assertEquals(1000L, wrapper.evaluate(vertex).getLong());
  }
}
