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
package org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.physical;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.common.model.impl.properties.PropertyList;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.IdEntry;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.ProjectionEntry;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.utils.PropertyProjector;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class ProjectEmbeddingsTest extends GradoopFlinkTestBase {

  @Test
  public void returnsEmbeddingWithOneProjection() throws Exception{
    ProjectionEntry entry = new ProjectionEntry(GradoopId.get(),
      getPropertyList(Lists.newArrayList("foo", "bar", "baz")));

    Embedding embedding = new Embedding();
    embedding.addEntry(new IdEntry(GradoopId.get()));
    embedding.addEntry(entry);
    embedding.addEntry(new IdEntry(GradoopId.get()));

    DataSet<Embedding> embeddingDataSet =
      getExecutionEnvironment().fromCollection(Lists.newArrayList(embedding));

    HashMap<Integer, List<String>> extractedPropertyKeys = new HashMap<>();
    extractedPropertyKeys.put(1, Lists.newArrayList("foo", "bar"));
    ProjectEmbeddings operator = new ProjectEmbeddings(embeddingDataSet, extractedPropertyKeys);

    List<Embedding> results = operator.evaluate().collect();

    assertEquals(1,results.size());
    assertEquals(3,results.get(0).size());
    assertEquals(IdEntry.class, results.get(0).getEntry(0).getClass());
    assertEquals(ProjectionEntry.class, results.get(0).getEntry(1).getClass());
    assertEquals(IdEntry.class, results.get(0).getEntry(2).getClass());
    assertEquals(extractedPropertyKeys.get(1),results.get(0).getEntry(1).getProperties().get().getKeys());
  }


  private PropertyList getPropertyList(List<String> property_names) {
    PropertyList properties = new PropertyList();

    for(String property_name : property_names) {
      properties.set(property_name, property_name);
    }

    return properties;
  }
}
