/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.join.functions;

import org.apache.commons.lang.ArrayUtils;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.PhysicalOperatorTest;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.EmbeddingTestUtils.createEmbedding;


public class ExtractPropertyJoinColumnsTest extends PhysicalOperatorTest {

  @Test
  public void testSingleColumn() throws Exception {
    PropertyValue a = PropertyValue.create("Foo");
    PropertyValue b = PropertyValue.create(42);

    Embedding embedding = new Embedding();
    embedding.add(GradoopId.get(), a, b);

    ExtractPropertyJoinColumns udf = new ExtractPropertyJoinColumns(Collections.singletonList(0));

    Assert.assertEquals(ArrayUtils.toString(embedding.getRawProperty(0)), udf.getKey(embedding));
  }

  @Test
  public void testMultiColumn() throws Exception {
    PropertyValue a = PropertyValue.create("Foo");
    PropertyValue b = PropertyValue.create(42);

    Embedding embedding = new Embedding();
    embedding.add(GradoopId.get(), a, b);


    ExtractPropertyJoinColumns udf = new ExtractPropertyJoinColumns(Arrays.asList(0, 1));

    Assert.assertEquals(
      ArrayUtils.toString(embedding.getRawProperty(0)) + ArrayUtils.toString(embedding.getRawProperty(1)),
      udf.getKey(embedding)
    );
  }

  @Test
  public void testMultiColumnReverse() throws Exception {
    PropertyValue a = PropertyValue.create("Foo");
    PropertyValue b = PropertyValue.create(42);

    Embedding embedding = new Embedding();
    embedding.add(GradoopId.get(), a, b);

    ExtractPropertyJoinColumns udf1 = new ExtractPropertyJoinColumns(Arrays.asList(0, 1));
    ExtractPropertyJoinColumns udf2 = new ExtractPropertyJoinColumns(Arrays.asList(1, 0));

    Assert.assertNotEquals(udf1.getKey(embedding), udf2.getKey(embedding));
  }
}
