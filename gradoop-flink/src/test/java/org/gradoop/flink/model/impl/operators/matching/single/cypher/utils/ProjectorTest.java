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

package org.gradoop.flink.model.impl.operators.matching.single.cypher.utils;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.Embedding;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.IdEntry;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.embeddings.ProjectionEntry;
import org.junit.Test;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class ProjectorTest {
  @Test
  public void projectWithExistingPropertyKeysTest() throws Exception{
    Embedding embedding = new Embedding(Lists.newArrayList(
      new ProjectionEntry(GradoopId.get(), getProperties(Lists.newArrayList("m", "n", "o"))),
      new ProjectionEntry(GradoopId.get(), getProperties(Lists.newArrayList("a", "b", "c")))
    ));

    Map<Integer, List<String>> propertyKeyMap = new HashMap<>();
    propertyKeyMap.put(0, Lists.newArrayList("m", "o"));
    propertyKeyMap.put(1, Lists.newArrayList("a", "b"));

    Embedding result = Projector.project(embedding, propertyKeyMap);

    assertEquals(
      Sets.newHashSet("m", "o"),
      result.getEntry(0).getProperties().get().getKeys()
    );

    assertEquals(
      Sets.newHashSet("a", "b"),
      result.getEntry(1).getProperties().get().getKeys()
    );
  }

  @Test
  public void embeddingEntryProjectionHasCorrectId() throws Exception{
    ProjectionEntry projectionEntry =
      new ProjectionEntry(GradoopId.get(), getProperties(Lists.newArrayList("m", "n", "o")));

    Embedding embedding = new Embedding(Lists.newArrayList(
      projectionEntry
    ));

    Map<Integer, List<String>> propertyKeyMap = new HashMap<>();
    propertyKeyMap.put(0, Lists.newArrayList("m", "o"));

    Embedding result = Projector.project(embedding, propertyKeyMap);

    assertEquals(projectionEntry.getId(), result.getEntry(0).getId());
  }

  @Test
  public void returnEmptyProjectionEntryIfEntryHasNoProperties() throws Exception{
    IdEntry entry = new IdEntry(GradoopId.get());

    Embedding embedding = new Embedding(Lists.newArrayList(
      entry
    ));

    Map<Integer, List<String>> propertyKeyMap = new HashMap<>();
    propertyKeyMap.put(0, Lists.newArrayList("m", "o"));

    Embedding result = Projector.project(embedding, propertyKeyMap);

    assertEquals(ProjectionEntry.class,        result.getEntry(0).getClass());
    assertEquals(entry.getId(),                result.getEntry(0).getId());
    assertEquals(
      Sets.newHashSet("m", "o"),
      result.getEntry(0).getProperties().get().getKeys()
    );
  }

  private Properties getProperties(List<String> propertyNames) {
    Properties properties = new Properties();

    for(String property_name : propertyNames) {
      properties.set(property_name, property_name);
    }

    return properties;
  }
}
