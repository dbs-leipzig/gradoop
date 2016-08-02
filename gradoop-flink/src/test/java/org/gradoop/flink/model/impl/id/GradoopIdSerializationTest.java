package org.gradoop.flink.model.impl.id;

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.model.impl.GradoopFlinkTestUtils;
import org.junit.Test;

import static org.gradoop.flink.model.impl.GradoopFlinkTestUtils.writeAndRead;
import static org.junit.Assert.assertEquals;

public class GradoopIdSerializationTest extends GradoopFlinkTestBase {

  @Test
  public void testGradoopIdSerialization() throws Exception {
    GradoopId idIn = GradoopId.get();
    assertEquals("GradoopIds were not equal", idIn,
      GradoopFlinkTestUtils.writeAndRead(idIn));
  }

  @Test
  public void testGradoopIdSetSerialization() throws Exception {
    GradoopIdSet idsIn = GradoopIdSet.fromExisting(
      GradoopId.get(), GradoopId.get());
    assertEquals("GradoopIdSets were not equal", idsIn,
      GradoopFlinkTestUtils.writeAndRead(idsIn));
  }
}
