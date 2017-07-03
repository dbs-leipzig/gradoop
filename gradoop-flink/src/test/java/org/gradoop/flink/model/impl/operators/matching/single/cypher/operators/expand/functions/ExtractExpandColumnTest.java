package org.gradoop.flink.model.impl.operators.matching.single.cypher.operators.expand.functions;

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.matching.single.cypher.pojos.Embedding;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ExtractExpandColumnTest {

  @Test
  public void testSelectIdOfSpecifiedEmbeddingEntry() throws Exception {
    GradoopId a = GradoopId.get();
    GradoopId b = GradoopId.get();
    GradoopId c = GradoopId.get();

    Embedding embedding = new Embedding();
    embedding.add(a); 
    embedding.add(b); 
    embedding.add(c);

    ExtractExpandColumn selector = new ExtractExpandColumn(0);
    assertEquals(a, selector.getKey(embedding));

    selector = new ExtractExpandColumn(1);
    assertEquals(b, selector.getKey(embedding));

    selector = new ExtractExpandColumn(2);
    assertEquals(c, selector.getKey(embedding));
  }
}
