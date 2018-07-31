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
package org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.join;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.operators.base.JoinOperatorBase;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.PhysicalOperator;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.PhysicalOperatorTest;
import org.junit.Test;

import java.util.List;

import static org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.EmbeddingTestUtils.assertEmbeddingExists;
import static org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.EmbeddingTestUtils.assertEveryEmbedding;

import static org.junit.Assert.assertEquals;

public class ValueJoinTest extends PhysicalOperatorTest {
  private static GradoopId v0 = GradoopId.get();
  private static GradoopId v1 = GradoopId.get();
  private static GradoopId v2 = GradoopId.get();
  private static GradoopId v3 = GradoopId.get();
  private static GradoopId e0 = GradoopId.get();
  private static GradoopId e1 = GradoopId.get();

  @Test
  public void testJoin() throws Exception {
    Embedding l = new Embedding();
    l.add(v0, PropertyValue.create("Foobar"));
    l.add(e0, PropertyValue.create(42));
    l.add(v1);
    DataSet<Embedding> left = getExecutionEnvironment().fromElements(l);

    Embedding r1 = new Embedding();
    r1.add(v2, PropertyValue.create("Foobar"));
    r1.add(e1, PropertyValue.create(21));
    r1.add(v3);
    Embedding r2 = new Embedding();
    r2.add(v2, PropertyValue.create("Baz"));
    r2.add(e1, PropertyValue.create(42));
    r2.add(v3);

    DataSet<Embedding> right = getExecutionEnvironment().fromElements(r1,r2);

    List<Integer> emptyList = Lists.newArrayListWithCapacity(0);

    PhysicalOperator join = new ValueJoin(left, right,
      Lists.newArrayList(0), Lists.newArrayList(0),
      3,
      emptyList, emptyList, emptyList, emptyList,
      JoinOperatorBase.JoinHint.OPTIMIZER_CHOOSES
    );

    DataSet<Embedding> result = join.evaluate();
    assertEquals(1, result.count());
    assertEveryEmbedding(result, embedding ->
      embedding.getProperties().equals(Lists.newArrayList(
        PropertyValue.create("Foobar"),
        PropertyValue.create(42),
        PropertyValue.create("Foobar"),
        PropertyValue.create(21)
        )
      ));
    assertEmbeddingExists(result, v0,e0,v1,v2,e1,v3);
  }

  @Test
  public void testSingleJoinPartners() throws Exception {
    Embedding l1 = new Embedding();
    l1.add(v0, PropertyValue.create("Foobar"));
    Embedding l2 = new Embedding();
    l2.add(v1, PropertyValue.create("Bar"));
    DataSet<Embedding> left = getExecutionEnvironment().fromElements(l1,l2);

    Embedding r1 = new Embedding();
    r1.add(v2, PropertyValue.create("Foobar"));
    Embedding r2 = new Embedding();
    r2.add(v3, PropertyValue.create("Bar"));
    DataSet<Embedding> right = getExecutionEnvironment().fromElements(r1,r2);

    PhysicalOperator join = new ValueJoin(left, right, Lists.newArrayList(0), Lists.newArrayList(0), 1);

    DataSet<Embedding> result = join.evaluate();
    assertEquals(2, result.count());
    assertEmbeddingExists(result, v0,v2);
    assertEmbeddingExists(result, v1,v3);
  }

  @Test
  public void testMultipleJoinPartners() throws Exception {
    Embedding l1 = new Embedding();
    l1.add(v0, PropertyValue.create("Foobar"), PropertyValue.create(21));
    Embedding l2 = new Embedding();
    l2.add(v1, PropertyValue.create("Foobar"), PropertyValue.create(42));
    DataSet<Embedding> left = getExecutionEnvironment().fromElements(l1,l2);

    Embedding r1 = new Embedding();
    r1.add(v2, PropertyValue.create("Foobar"), PropertyValue.create(21));
    Embedding r2 = new Embedding();
    r2.add(v3, PropertyValue.create("Foobar"), PropertyValue.create(42));
    DataSet<Embedding> right = getExecutionEnvironment().fromElements(r1,r2);

    PhysicalOperator join =
      new ValueJoin(left, right, Lists.newArrayList(0,1), Lists.newArrayList(0,1), 1);

    DataSet<Embedding> result = join.evaluate();
    assertEquals(2, result.count());
    assertEmbeddingExists(result, v0,v2);
    assertEmbeddingExists(result, v1,v3);
  }
}
