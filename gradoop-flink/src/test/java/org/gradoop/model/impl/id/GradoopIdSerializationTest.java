package org.gradoop.model.impl.id;

import org.gradoop.model.GradoopFlinkTestBase;
import org.junit.Test;

import static org.gradoop.model.impl.GradoopFlinkTestUtils.writeAndRead;
import static org.junit.Assert.assertEquals;

public class GradoopIdSerializationTest extends GradoopFlinkTestBase {

  @Test
  public void testGradoopIdSerialization() throws Exception {
    GradoopId idIn = GradoopId.get();
    assertEquals("GradoopIds were not equal", idIn, writeAndRead(idIn));
  }

  @Test
  public void testGradoopIdSetSerialization() throws Exception {
    GradoopIdSet idsIn = GradoopIdSet.fromExisting(
      GradoopId.get(), GradoopId.get());
    assertEquals("GradoopIdSets were not equal", idsIn, writeAndRead(idsIn));
  }
}
