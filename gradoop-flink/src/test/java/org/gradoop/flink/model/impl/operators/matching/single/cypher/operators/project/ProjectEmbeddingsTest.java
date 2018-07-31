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
package org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.project;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.PhysicalOperatorTest;
import org.junit.Test;

import java.util.List;

import static org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.EmbeddingTestUtils.assertEveryEmbedding;
import static org.junit.Assert.assertEquals;

public class ProjectEmbeddingsTest extends PhysicalOperatorTest {

  @Test
  public void projectEmbedding() throws Exception{
    Embedding embedding = new Embedding();
    embedding.add(GradoopId.get());
    embedding.add(GradoopId.get(), getPropertyValues(Lists.newArrayList("m", "n", "o")));

    DataSet<Embedding> embeddings =
      getExecutionEnvironment().fromElements(embedding, embedding);

    List<Integer> extractedPropertyKeys = Lists.newArrayList(0,2);
    
    ProjectEmbeddings operator = new ProjectEmbeddings(embeddings, extractedPropertyKeys);

    DataSet<Embedding> results = operator.evaluate();
    assertEquals(2,results.count());
    
    assertEveryEmbedding(results, (e) -> {
      assertEquals(2,                        e.size());
      assertEquals(PropertyValue.create("m"), e.getProperty(0));
      assertEquals(PropertyValue.create("o"), e.getProperty(1));
    });
  }

}
