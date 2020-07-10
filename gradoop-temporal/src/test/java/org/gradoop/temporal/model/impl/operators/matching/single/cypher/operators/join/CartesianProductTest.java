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
import org.apache.flink.api.common.operators.base.CrossOperatorBase;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.PhysicalTPGMOperator;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.PhysicalTPGMOperatorTest;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGM;
import org.junit.Test;

import java.util.Arrays;

import static org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.join.JoinTestUtil.assertEmbeddingTPGMExists;
import static org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.join.JoinTestUtil.assertEveryEmbeddingTPGM;
import static org.junit.Assert.assertEquals;

public class CartesianProductTest extends PhysicalTPGMOperatorTest {
  private static final GradoopId v0 = GradoopId.get();
  private static final GradoopId v1 = GradoopId.get();
  private static final GradoopId v2 = GradoopId.get();
  private static final GradoopId e0 = GradoopId.get();
  private static final GradoopId e1 = GradoopId.get();
  private static final GradoopId e2 = GradoopId.get();
  private static final GradoopId e3 = GradoopId.get();

  @Test
  public void testCross() throws Exception {
    EmbeddingTPGM l1 = new EmbeddingTPGM();
    l1.add(v0, PropertyValue.create("Foobar"));
    l1.add(e0, PropertyValue.create(42));
    l1.add(v1);
    l1.addTimeData(1L, 2L, 3L, 4L);
    EmbeddingTPGM l2 = new EmbeddingTPGM();
    l2.add(e1, PropertyValue.create("Foobar"));
    l2.add(e2, PropertyValue.create(42));
    l2.add(e3);
    l2.addTimeData(1L, 2L, 3L, 4L);
    DataSet<EmbeddingTPGM> left = getExecutionEnvironment().fromElements(l1, l2);

    EmbeddingTPGM r1 = new EmbeddingTPGM();
    r1.add(v2, PropertyValue.create("Baz"));
    r1.add(e1);
    r1.addTimeData(5L, 6L, 7L, 8L);
    EmbeddingTPGM r2 = new EmbeddingTPGM();
    r2.add(v0, PropertyValue.create("Baz"));
    r2.add(e2);
    r2.addTimeData(5L, 6L, 7L, 8L);
    DataSet<EmbeddingTPGM> right = getExecutionEnvironment().fromElements(r1, r2);

    PhysicalTPGMOperator join = new CartesianProduct(
      left, right, 2,
      Lists.newArrayList(), Lists.newArrayList(),
      Lists.newArrayList(), Lists.newArrayList(),
      CrossOperatorBase.CrossHint.OPTIMIZER_CHOOSES
    );

    DataSet<EmbeddingTPGM> result = join.evaluate();

    assertEquals(4, result.count());

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

    assertEmbeddingTPGMExists(result, v0, e0, v1, v2, e1);
    assertEmbeddingTPGMExists(result, v0, e0, v1, v0, e2);
    assertEmbeddingTPGMExists(result, e1, e2, e3, v2, e1);
    assertEmbeddingTPGMExists(result, e1, e2, e3, v0, e2);
  }

}
