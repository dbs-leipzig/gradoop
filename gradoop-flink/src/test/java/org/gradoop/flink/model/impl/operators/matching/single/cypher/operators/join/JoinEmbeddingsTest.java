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
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.PhysicalOperator;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.PhysicalOperatorTest;
import org.junit.Test;

import static org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.EmbeddingTestUtils.*;
import static org.junit.Assert.assertEquals;

public class JoinEmbeddingsTest extends PhysicalOperatorTest {
  private static GradoopId v0 = GradoopId.get();
  private static GradoopId v1 = GradoopId.get();
  private static GradoopId e0 = GradoopId.get();
  private static GradoopId e1 = GradoopId.get();
  private static GradoopId e2 = GradoopId.get();
  private static GradoopId e3 = GradoopId.get();

  @Test
  public void testJoin() throws Exception {
    Embedding l = new Embedding();
    l.add(v0, PropertyValue.create("Foobar"));
    l.add(e0, PropertyValue.create(42));
    l.add(v1);
    DataSet<Embedding> left = getExecutionEnvironment().fromElements(l);

    Embedding r = new Embedding();
    r.add(v1, PropertyValue.create("Baz"));
    DataSet<Embedding> right = getExecutionEnvironment().fromElements(r);

    PhysicalOperator join = new JoinEmbeddings(left, right, 1, 2, 0);

    DataSet<Embedding> result = join.evaluate();
    assertEquals(1, result.count());
    assertEveryEmbedding(result, embedding ->
      embedding.getProperties().equals(Lists.newArrayList(
        PropertyValue.create("Foobar"),
        PropertyValue.create(42),
        PropertyValue.create("Baz")
      )
    ));
    assertEmbeddingExists(result, v0,e0,v1);
  }

  @Test
  public void testSingleJoinPartners() throws Exception {
    DataSet<Embedding> left = getExecutionEnvironment().fromElements(
      createEmbedding(v0,e0,v1),
      createEmbedding(v1,e2,v0)
    );

    DataSet<Embedding> right = getExecutionEnvironment().fromElements(
      createEmbedding(v0),
      createEmbedding(v1)
    );

    PhysicalOperator join = new JoinEmbeddings(left, right, 1, 0, 0);

    DataSet<Embedding> result = join.evaluate();
    assertEquals(2, result.count());
    assertEmbeddingExists(result, v0,e0,v1);
    assertEmbeddingExists(result, v1,e2,v0);
  }

  @Test
  public void testMultipleJoinPartners() throws Exception {
    //Single Column
    DataSet<Embedding> left = getExecutionEnvironment().fromElements(
      createEmbedding(v0,e0,v1),
      createEmbedding(v1,e1,v0)
    );

    DataSet<Embedding> right = getExecutionEnvironment().fromElements(
      createEmbedding(v0,e2,v1),
      createEmbedding(v1,e3,v0)
    );

    PhysicalOperator join =
      new JoinEmbeddings(left, right, 3, Lists.newArrayList(0,2), Lists.newArrayList(2,0));

    DataSet<Embedding> result = join.evaluate();
    assertEquals(2, result.count());
    assertEmbeddingExists(result, v0,e0,v1,e3);
    assertEmbeddingExists(result, v1,e1,v0,e2);
  }
}
