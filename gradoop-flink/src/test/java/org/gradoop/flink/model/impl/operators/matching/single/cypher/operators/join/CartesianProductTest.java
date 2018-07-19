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
import org.apache.flink.api.common.operators.base.CrossOperatorBase;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.PhysicalOperator;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.PhysicalOperatorTest;
import org.junit.Test;

import static org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.EmbeddingTestUtils.*;
import static org.junit.Assert.assertEquals;

public class CartesianProductTest extends PhysicalOperatorTest {
  private static GradoopId v0 = GradoopId.get();
  private static GradoopId v1 = GradoopId.get();
  private static GradoopId v2 = GradoopId.get();
  private static GradoopId e0 = GradoopId.get();
  private static GradoopId e1 = GradoopId.get();
  private static GradoopId e2 = GradoopId.get();
  private static GradoopId e3 = GradoopId.get();

  @Test
  public void testCross() throws Exception {
    Embedding l1 = new Embedding();
    l1.add(v0, PropertyValue.create("Foobar"));
    l1.add(e0, PropertyValue.create(42));
    l1.add(v1);
    Embedding l2 = new Embedding();
    l2.add(e1, PropertyValue.create("Foobar"));
    l2.add(e2, PropertyValue.create(42));
    l2.add(e3);
    DataSet<Embedding> left = getExecutionEnvironment().fromElements(l1,l2);

    Embedding r1 = new Embedding();
    r1.add(v2, PropertyValue.create("Baz"));
    r1.add(e1);
    Embedding r2 = new Embedding();
    r2.add(v0, PropertyValue.create("Baz"));
    r2.add(e2);
    DataSet<Embedding> right = getExecutionEnvironment().fromElements(r1,r2);

    PhysicalOperator join = new CartesianProduct(
      left, right, 2,
      Lists.newArrayList(), Lists.newArrayList(),
      Lists.newArrayList(), Lists.newArrayList(),
      CrossOperatorBase.CrossHint.OPTIMIZER_CHOOSES
    );

    DataSet<Embedding> result = join.evaluate();

    assertEquals(4, result.count());

    assertEveryEmbedding(result, embedding ->
      embedding.getProperties().equals(Lists.newArrayList(
        PropertyValue.create("Foobar"),
        PropertyValue.create(42),
        PropertyValue.create("Baz")
      )
    ));

    assertEmbeddingExists(result, v0,e0,v1,v2,e1);
    assertEmbeddingExists(result, v0,e0,v1,v0,e2);
    assertEmbeddingExists(result, e1,e2,e3,v2,e1);
    assertEmbeddingExists(result, e1,e2,e3,v0,e2);
  }
}
