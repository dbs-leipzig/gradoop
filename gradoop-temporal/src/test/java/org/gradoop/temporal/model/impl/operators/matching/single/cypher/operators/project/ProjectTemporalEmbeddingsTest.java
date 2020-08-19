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
 *//*

package org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.project;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.project.ProjectEmbeddings;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.PhysicalTPGMOperatorTest;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGM;
import org.junit.Test;

import java.util.List;

import static org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.join.JoinTestUtil.assertEveryEmbeddingTPGM;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

public class ProjectTemporalEmbeddingsTest extends PhysicalTPGMOperatorTest {
  @Test
  public void projectEmbedding() throws Exception {
    EmbeddingTPGM embedding = new EmbeddingTPGM();
    embedding.add(GradoopId.get());
    embedding.add(GradoopId.get(), getPropertyValues(Lists.newArrayList("m", "n", "o")),
      1L, 2L, 3L, 4L);

    DataSet<EmbeddingTPGM> embeddings =
      getExecutionEnvironment().fromElements(embedding, embedding);

    List<Integer> extractedPropertyKeys = Lists.newArrayList(0, 2);

    ProjectEmbeddings operator = new ProjectEmbeddings(embeddings, extractedPropertyKeys);

    DataSet<EmbeddingTPGM> results = operator.evaluate();
    assertEquals(2, results.count());

    assertEveryEmbeddingTPGM(results, (e) -> {
      assertEquals(2, e.size());
      assertEquals(PropertyValue.create("m"), e.getProperty(0));
      assertEquals(PropertyValue.create("o"), e.getProperty(1));
      assertArrayEquals(new Long[] {1L, 2L, 3L, 4L}, e.getTimes(0));
    });
  }
}
*/
