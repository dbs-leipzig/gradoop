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
package org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.add;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.PhysicalTPGMOperatorTest;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGM;
import org.junit.Test;

import static org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.join.JoinTestUtil.createEmbeddingTPGM;
import static org.junit.Assert.assertEquals;

public class AddEmbeddingsTPGMElementsTest extends PhysicalTPGMOperatorTest {

  @Test
  public void testAddEmbeddings() throws Exception {
    EmbeddingTPGM e1 = createEmbeddingTPGM(GradoopId.get(), GradoopId.get());
    EmbeddingTPGM e2 = createEmbeddingTPGM(GradoopId.get(), GradoopId.get(), GradoopId.get());

    DataSet<EmbeddingTPGM> input = ExecutionEnvironment.getExecutionEnvironment().fromElements(e1, e2);

    DataSet<EmbeddingTPGM> result = new AddEmbeddingsTPGMElements(input, 3).evaluate();

    assertEquals(2, result.count());
    assertEquals(result.collect().get(0).size(), 5);
    assertEquals(result.collect().get(1).size(), 6);
  }
}
