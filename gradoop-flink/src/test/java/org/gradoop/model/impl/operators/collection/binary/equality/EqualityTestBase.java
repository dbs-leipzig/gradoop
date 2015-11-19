package org.gradoop.model.impl.operators.collection.binary.equality;

import org.apache.flink.api.java.DataSet;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Created by peet on 19.11.15.
 */
public class EqualityTestBase {
  protected void collectAndCheckAssertions(DataSet<Boolean> result) {

    List<Boolean> collectedResult = null;
    try {
      collectedResult = result.collect();
    } catch (Exception e) {
      e.printStackTrace();
    }
    assertNotNull("result is null", collectedResult);
    assertEquals("only one boolean result expected", 1, collectedResult.size());
    assertTrue("expected equality", collectedResult.get(0));
  }

}
