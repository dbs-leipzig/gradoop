
package org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.expand.functions;

import org.apache.flink.api.common.functions.util.ListCollector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class AdoptEmptyPathsTest {
  @Test
  public void testFilterEmbeddingsOnClosingColumn() throws Exception {
    GradoopId a = GradoopId.get();
    GradoopId b = GradoopId.get();

    Embedding embedding = new Embedding();
    embedding.add(a);
    embedding.add(b);

    List<Embedding> result = new ArrayList<>();
    new AdoptEmptyPaths(1, 0).flatMap(embedding, new ListCollector<>(result));
    assertTrue(result.isEmpty());


    new AdoptEmptyPaths(1, 1).flatMap(embedding, new ListCollector<>(result));
    assertEquals(1, result.size());
  }

  @Test
  public void testEmbeddingFormat() throws Exception{
    GradoopId a = GradoopId.get();
    GradoopId b = GradoopId.get();

    Embedding embedding = new Embedding();
    embedding.add(a);
    embedding.add(b);

    List<Embedding> result = new ArrayList<>();
    new AdoptEmptyPaths(1, -1).flatMap(embedding, new ListCollector<>(result));

    assertTrue(result.get(0).getIdList(2).isEmpty());
    assertEquals(b, result.get(0).getId(3));
  }
}
