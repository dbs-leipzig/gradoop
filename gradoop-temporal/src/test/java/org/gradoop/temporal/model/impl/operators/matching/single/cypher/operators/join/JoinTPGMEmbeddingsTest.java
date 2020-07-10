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
package org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.join;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.PhysicalTPGMOperator;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGM;
import org.junit.Test;

import java.util.Arrays;

import static org.apache.flink.api.java.ExecutionEnvironment.getExecutionEnvironment;
import static org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.join.JoinTestUtil.assertEmbeddingTPGMExists;
import static org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.join.JoinTestUtil.assertEveryEmbeddingTPGM;
import static org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.join.JoinTestUtil.createEmbeddingTPGM;
import static org.junit.Assert.assertEquals;

public class JoinTPGMEmbeddingsTest {
  private static final GradoopId v0 = GradoopId.get();
  private static final GradoopId v1 = GradoopId.get();
  private static final GradoopId e0 = GradoopId.get();
  private static final GradoopId e1 = GradoopId.get();
  private static final GradoopId e2 = GradoopId.get();
  private static final GradoopId e3 = GradoopId.get();

  @Test
  public void testJoin() throws Exception {
    ExecutionEnvironment env = getExecutionEnvironment();
    EmbeddingTPGM l = new EmbeddingTPGM();
    l.add(v0, new PropertyValue[] {PropertyValue.create("Foobar")},
      1L, 2L, 3L, 4L);
    l.add(e0, new PropertyValue[] {PropertyValue.create(42)},
      5L, 6L, 7L, 8L);
    l.add(v1, new PropertyValue[] {}, 9L, 10L, 11L, 12L);
    DataSet<EmbeddingTPGM> left = env.fromElements(l);

    EmbeddingTPGM r = new EmbeddingTPGM();
    r.add(v1, new PropertyValue[] {PropertyValue.create("Baz")},
      1L, 2L, 3L, 4L);
    DataSet<EmbeddingTPGM> right = env.fromElements(r);

    DataSet<EmbeddingTPGM> result =
      new JoinTPGMEmbeddings(left, right, 1, 2, 0)
        .evaluate();
    assertEquals(1, result.count());
    assertEveryEmbeddingTPGM(result, embedding ->
      embedding.getProperties().equals(Lists.newArrayList(
        PropertyValue.create("Foobar"),
        PropertyValue.create(42),
        PropertyValue.create("Baz")
        )
      ));
    assertEveryEmbeddingTPGM(result, embedding ->
      Arrays.equals(embedding.getTimes(0), new Long[] {1L, 2L, 3L, 4L}));
    assertEveryEmbeddingTPGM(result, embedding ->
      Arrays.equals(embedding.getTimes(1), new Long[] {5L, 6L, 7L, 8L}));
    assertEveryEmbeddingTPGM(result, embedding ->
      Arrays.equals(embedding.getTimes(2), new Long[] {9L, 10L, 11L, 12L}));
    assertEveryEmbeddingTPGM(result, embedding ->
      Arrays.equals(embedding.getTimes(3), new Long[] {1L, 2L, 3L, 4L}));
    assertEmbeddingTPGMExists(result, v0, e0, v1);
  }


  @Test
  public void testSingleJoinPartners() throws Exception {
    ExecutionEnvironment env = getExecutionEnvironment();

    DataSet<EmbeddingTPGM> left = env.fromElements(
      createEmbeddingTPGM(v0, e0, v1),
      createEmbeddingTPGM(v1, e2, v0)
    );

    DataSet<EmbeddingTPGM> right = env.fromElements(
      createEmbeddingTPGM(v0),
      createEmbeddingTPGM(v1)
    );

    PhysicalTPGMOperator join = new JoinTPGMEmbeddings(left, right, 1, 0, 0);

    DataSet<EmbeddingTPGM> result = join.evaluate();
    assertEquals(2, result.count());
    assertEmbeddingTPGMExists(result, v0, e0, v1);
    assertEmbeddingTPGMExists(result, v1, e2, v0);
  }

  @Test
  public void testMultipleJoinPartners() throws Exception {
    ExecutionEnvironment env = getExecutionEnvironment();
    //Single Column
    DataSet<EmbeddingTPGM> left = env.fromElements(
      createEmbeddingTPGM(v0, e0, v1),
      createEmbeddingTPGM(v1, e1, v0)
    );

    DataSet<EmbeddingTPGM> right = env.fromElements(
      createEmbeddingTPGM(v0, e2, v1),
      createEmbeddingTPGM(v1, e3, v0)
    );

    PhysicalTPGMOperator join =
      new JoinTPGMEmbeddings(left, right, 3, Lists.newArrayList(0, 2), Lists.newArrayList(2, 0));

    DataSet<EmbeddingTPGM> result = join.evaluate();
    assertEquals(2, result.count());
    assertEmbeddingTPGMExists(result, v0, e0, v1, e3);
    assertEmbeddingTPGMExists(result, v1, e1, v0, e2);
  }
}
