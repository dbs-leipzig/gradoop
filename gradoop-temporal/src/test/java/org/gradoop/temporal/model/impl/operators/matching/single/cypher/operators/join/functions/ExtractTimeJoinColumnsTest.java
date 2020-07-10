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
package org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.join.functions;


import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.PhysicalTPGMOperatorTest;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGM;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;


public class ExtractTimeJoinColumnsTest extends PhysicalTPGMOperatorTest {

  @Test
  public void testSingleSelector() throws Exception {
    PropertyValue a = PropertyValue.create("Foo");
    PropertyValue b = PropertyValue.create(42);

    EmbeddingTPGM embedding = new EmbeddingTPGM();
    embedding.add(
      GradoopId.get(), new PropertyValue[] {a, b}, 1L, 2L, 3L, 4L);

    ExtractTimeJoinColumns udf = new ExtractTimeJoinColumns(Collections.singletonList(new Tuple2<>(0, 1)));

    // separator is appended
    String expected = 2L + "|";

    Assert.assertEquals(expected, udf.getKey(embedding));
  }

  @Test
  public void testMultiSelectors() throws Exception {
    EmbeddingTPGM embedding = new EmbeddingTPGM();
    embedding.add(GradoopId.get(), new PropertyValue[] {},
      1234L, 12345L, 789L, 9876L);
    embedding.add(GradoopId.get(), new PropertyValue[] {},
      456L, 45678L, 4321L, 54321L);

    ExtractTimeJoinColumns udf = new ExtractTimeJoinColumns(Arrays.asList(
      new Tuple2<>(0, 3),
      new Tuple2<>(0, 1),
      new Tuple2<>(1, 2)
    ));

    String expected = "9876|12345|4321|";

    Assert.assertEquals(expected, udf.getKey(embedding));
  }

  @Test
  public void testMultiColumnReverse() throws Exception {
    EmbeddingTPGM embedding = new EmbeddingTPGM();
    embedding.add(GradoopId.get(), new PropertyValue[] {},
      1234L, 12345L, 789L, 9876L);

    ExtractTimeJoinColumns udf1 = new ExtractTimeJoinColumns(Arrays.asList(
      new Tuple2<>(0, 0),
      new Tuple2<>(0, 1)));
    ExtractTimeJoinColumns udf2 = new ExtractTimeJoinColumns(Arrays.asList(
      new Tuple2<>(0, 1),
      new Tuple2<>(0, 0)));

    Assert.assertNotEquals(udf1.getKey(embedding), udf2.getKey(embedding));
  }
}

