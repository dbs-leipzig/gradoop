/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.project;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.common.pojos.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.PhysicalOperatorTest;
import org.junit.Test;

import java.util.List;

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
