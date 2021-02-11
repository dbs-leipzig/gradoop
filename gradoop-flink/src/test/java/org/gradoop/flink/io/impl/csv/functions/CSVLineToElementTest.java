package org.gradoop.flink.io.impl.csv.functions;

import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Tests CSVLineToElement
 */
public class CSVLineToElementTest {

  /**
   * Test parsing of graph ids.
   */
  @Test
  public void testParseGradoopIds() {
    List<String> testData = Arrays.asList(
      "[]",
      "[000000000000000000000001]",
      "[000000000000000000000001,000000000000000000000002]",
      "[000000000000000000000003,000000000000000000000001,000000000000000000000002]"
    );
    GradoopId id1 = GradoopId.fromString("000000000000000000000001");
    GradoopId id2 = GradoopId.fromString("000000000000000000000002");
    GradoopId id3 = GradoopId.fromString("000000000000000000000003");

    List<GradoopIdSet> results = Arrays.asList(
      new GradoopIdSet(),
      GradoopIdSet.fromExisting(id1),
      GradoopIdSet.fromExisting(id1, id2),
      GradoopIdSet.fromExisting(id1, id2, id3)
    );

    CSVLineToElement mock = Mockito.mock(CSVLineToElement.class, Mockito.CALLS_REAL_METHODS);
    for (int i = 0; i < testData.size(); i++) {
      assertEquals(mock.parseGradoopIds(testData.get(i)), results.get(i));
    }
  }
}
