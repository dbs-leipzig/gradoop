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
import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.PhysicalTPGMOperator;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.PhysicalTPGMOperatorTest;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGM;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ValueJoinTest extends PhysicalTPGMOperatorTest {
  private static final GradoopId v0 = GradoopId.get();
  private static final GradoopId v1 = GradoopId.get();
  private static final GradoopId v2 = GradoopId.get();
  private static final GradoopId v3 = GradoopId.get();
  private static final GradoopId e0 = GradoopId.get();
  private static final GradoopId e1 = GradoopId.get();

  /**
   * Checks if the given data set contains at least one embedding that matches the given path.
   *
   * @param embeddings data set containing embedding
   * @param path       expected path
   * @throws Exception on failure
   */
  public static void assertEmbeddingTPGMExists(DataSet<EmbeddingTPGM> embeddings, GradoopId... path)
    throws Exception {
    List<GradoopId> pathList = Lists.newArrayList(path);
    assertTrue(embeddings.collect().stream()
      .anyMatch(embedding -> pathList.equals(embeddingToIdList(embedding)))
    );
  }

  /**
   * Applies a consumer (e.g. containing an assertion) to each embedding in the given data set.
   *
   * @param dataSet  data set containing embeddings
   * @param consumer consumer
   * @throws Exception on failure
   */
  public static void assertEveryEmbeddingTPGM(DataSet<EmbeddingTPGM> dataSet,
                                              Consumer<EmbeddingTPGM> consumer)
    throws Exception {
    dataSet.collect().forEach(consumer);
  }

  public static List<GradoopId> embeddingToIdList(EmbeddingTPGM embedding) {
    List<GradoopId> idList = new ArrayList<>();
    IntStream.range(0, embedding.size()).forEach(i -> idList.addAll(embedding.getIdAsList(i)));
    return idList;
  }

  @Test
  public void testJoin() throws Exception {
    EmbeddingTPGM l = new EmbeddingTPGM();
    l.add(v0, PropertyValue.create("Foobar"));
    l.add(e0, PropertyValue.create(42));
    l.add(v1, new PropertyValue[] {}, 1L, 2L, 3L, 4L);
    DataSet<EmbeddingTPGM> left = getExecutionEnvironment().fromElements(l);

    EmbeddingTPGM r1 = new EmbeddingTPGM();
    r1.add(v2, PropertyValue.create("Foobar"));
    r1.add(e1, PropertyValue.create(21));
    r1.add(v3, new PropertyValue[] {}, 0L, 1L, 1L, 2L);
    EmbeddingTPGM r2 = new EmbeddingTPGM();
    r2.add(v2, PropertyValue.create("Baz"));
    r2.add(e1, PropertyValue.create(42));
    r2.add(v3, new PropertyValue[] {}, 10L, 20L, 30L, 40L);

    DataSet<EmbeddingTPGM> right = getExecutionEnvironment().fromElements(r1, r2);

    List<Integer> emptyList = Lists.newArrayListWithCapacity(0);

    PhysicalTPGMOperator join = new ValueJoin(left, right,
      Lists.newArrayList(0), Lists.newArrayList(0),
      Lists.newArrayList(new Tuple2<>(0, 1)), Lists.newArrayList(new Tuple2<>(0, 3)),
      3,
      emptyList, emptyList, emptyList, emptyList,
      JoinOperatorBase.JoinHint.OPTIMIZER_CHOOSES
    );

    DataSet<EmbeddingTPGM> result = join.evaluate();
    assertEquals(1, result.count());
    assertEveryEmbeddingTPGM(result, embedding ->
      embedding.getProperties().equals(Lists.newArrayList(
        PropertyValue.create("Foobar"),
        PropertyValue.create(42),
        PropertyValue.create("Foobar"),
        PropertyValue.create(21)
        )
      ));
    assertEveryEmbeddingTPGM(result, embedding ->
      Arrays.equals(embedding.getTimes(0), new Long[] {1L, 2L, 3L, 4L}));
    assertEveryEmbeddingTPGM(result, embedding ->
      Arrays.equals(embedding.getTimes(1), new Long[] {0L, 1L, 1L, 2L}));
    assertEmbeddingTPGMExists(result, v0, e0, v1, v2, e1, v3);
  }

  @Test
  public void testSingleJoinPartners() throws Exception {
    EmbeddingTPGM l1 = new EmbeddingTPGM();
    l1.add(v0, new PropertyValue[] {PropertyValue.create("Foobar")},
      1L, 2L, 3L, 4L);
    EmbeddingTPGM l2 = new EmbeddingTPGM();
    l2.add(v1, new PropertyValue[] {PropertyValue.create("Bar")},
      2L, 4L, 8L, 16L);
    DataSet<EmbeddingTPGM> left = getExecutionEnvironment().fromElements(l1, l2);

    EmbeddingTPGM r1 = new EmbeddingTPGM();
    r1.add(v2, new PropertyValue[] {PropertyValue.create("Foobar")},
      1L, 2L, 3L, 4L);
    EmbeddingTPGM r2 = new EmbeddingTPGM();
    r2.add(v3, new PropertyValue[] {PropertyValue.create("Bar")},
      2L, 4L, 8L, 16L);
    DataSet<EmbeddingTPGM> right = getExecutionEnvironment().fromElements(r1, r2);

    PhysicalTPGMOperator join = new ValueJoin(left, right,
      Lists.newArrayList(0), Lists.newArrayList(0),
      Lists.newArrayList(new Tuple2<>(0, 1), new Tuple2<>(0, 2)),
      Lists.newArrayList(new Tuple2<>(0, 1), new Tuple2<>(0, 2)),
      1);

    DataSet<EmbeddingTPGM> result = join.evaluate();
    assertEquals(2, result.count());
    assertEmbeddingTPGMExists(result, v0, v2);
    assertEmbeddingTPGMExists(result, v1, v3);

    PhysicalTPGMOperator join2 = new ValueJoin(left, right,
      new ArrayList<>(), new ArrayList<>(),
      Lists.newArrayList(new Tuple2<>(0, 1)),
      Lists.newArrayList(new Tuple2<>(0, 0)),
      1);

    DataSet<EmbeddingTPGM> result2 = join2.evaluate();
    assertEquals(1, result2.count());
    assertEmbeddingTPGMExists(result2, v0, v3);
  }


  //-----------------------------------------------------------------
  // adapted from EmbeddingTestUtils
  //----------------------------------------------------------------

  @Test
  public void testMultipleJoinPartners() throws Exception {
    EmbeddingTPGM l1 = new EmbeddingTPGM();
    l1.add(v0, new PropertyValue[] {PropertyValue.create("Foobar"), PropertyValue.create(21)},
      1L, 2L, 3L, 4L);
    EmbeddingTPGM l2 = new EmbeddingTPGM();
    l2.add(v1, new PropertyValue[] {PropertyValue.create("Foobar"), PropertyValue.create(42)},
      10L, 20L, 30L, 40L);
    DataSet<EmbeddingTPGM> left = getExecutionEnvironment().fromElements(l1, l2);

    EmbeddingTPGM r1 = new EmbeddingTPGM();
    r1.add(v2, new PropertyValue[] {PropertyValue.create("Foobar"), PropertyValue.create(21)},
      1L, 5L, 3L, 8L);
    EmbeddingTPGM r2 = new EmbeddingTPGM();
    r2.add(v3, new PropertyValue[] {PropertyValue.create("Foobar"), PropertyValue.create(42)},
      10L, 50L, 30L, 100L);
    DataSet<EmbeddingTPGM> right = getExecutionEnvironment().fromElements(r1, r2);

    PhysicalTPGMOperator join =
      new ValueJoin(left, right,
        Lists.newArrayList(0, 1),
        Lists.newArrayList(0, 1),
        Lists.newArrayList(new Tuple2<>(0, 0), new Tuple2<>(0, 2)),
        Lists.newArrayList(new Tuple2<>(0, 0), new Tuple2<>(0, 2)),
        1);

    DataSet<EmbeddingTPGM> result = join.evaluate();
    assertEquals(2, result.count());
    assertEmbeddingTPGMExists(result, v0, v2);
    assertEmbeddingTPGMExists(result, v1, v3);
  }

  @Test
  public void testWithoutTime() throws Exception {
    EmbeddingTPGM l1 = new EmbeddingTPGM();
    l1.add(v0, new PropertyValue[] {PropertyValue.create("Foobar"), PropertyValue.create(21)},
      1L, 2L, 3L, 4L);
    EmbeddingTPGM l2 = new EmbeddingTPGM();
    l2.add(v1, new PropertyValue[] {PropertyValue.create("Foobar"), PropertyValue.create(42)},
      10L, 20L, 30L, 40L);
    DataSet<EmbeddingTPGM> left = getExecutionEnvironment().fromElements(l1, l2);

    EmbeddingTPGM r1 = new EmbeddingTPGM();
    r1.add(v2, new PropertyValue[] {PropertyValue.create("Foobar"), PropertyValue.create(21)},
      1L, 5L, 3L, 8L);
    EmbeddingTPGM r2 = new EmbeddingTPGM();
    r2.add(v3, new PropertyValue[] {PropertyValue.create("Foobar"), PropertyValue.create(42)},
      10L, 50L, 30L, 100L);
    DataSet<EmbeddingTPGM> right = getExecutionEnvironment().fromElements(r1, r2);

    PhysicalTPGMOperator join =
      new ValueJoin(left, right,
        Lists.newArrayList(0, 1),
        Lists.newArrayList(0, 1),
        new ArrayList<>(), new ArrayList<>(),
        1);

    DataSet<EmbeddingTPGM> result = join.evaluate();
    assertEquals(2, result.count());
    assertEmbeddingTPGMExists(result, v0, v2);
    assertEmbeddingTPGMExists(result, v1, v3);
  }

  @Test
  public void testWithoutProperties() throws Exception {
    EmbeddingTPGM l1 = new EmbeddingTPGM();
    l1.add(v0, new PropertyValue[] {PropertyValue.create("Foobar"), PropertyValue.create(21)},
      1L, 2L, 3L, 4L);
    EmbeddingTPGM l2 = new EmbeddingTPGM();
    l2.add(v1, new PropertyValue[] {PropertyValue.create("Foobar"), PropertyValue.create(42)},
      10L, 20L, 30L, 40L);
    DataSet<EmbeddingTPGM> left = getExecutionEnvironment().fromElements(l1, l2);

    EmbeddingTPGM r1 = new EmbeddingTPGM();
    r1.add(v2, new PropertyValue[] {PropertyValue.create("Foobar"), PropertyValue.create(21)},
      1L, 5L, 3L, 8L);
    EmbeddingTPGM r2 = new EmbeddingTPGM();
    r2.add(v3, new PropertyValue[] {PropertyValue.create("Foobar"), PropertyValue.create(42)},
      10L, 50L, 30L, 100L);
    DataSet<EmbeddingTPGM> right = getExecutionEnvironment().fromElements(r1, r2);

    PhysicalTPGMOperator join =
      new ValueJoin(left, right,
        new ArrayList<>(), new ArrayList<>(),
        Lists.newArrayList(new Tuple2<>(0, 0), new Tuple2<>(0, 2)),
        Lists.newArrayList(new Tuple2<>(0, 0), new Tuple2<>(0, 2)),
        1);

    DataSet<EmbeddingTPGM> result = join.evaluate();
    assertEquals(2, result.count());
    assertEmbeddingTPGMExists(result, v0, v2);
    assertEmbeddingTPGMExists(result, v1, v3);
  }
}
