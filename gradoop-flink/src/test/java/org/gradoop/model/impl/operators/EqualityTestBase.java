package org.gradoop.model.impl.operators;

import org.apache.flink.api.java.DataSet;

import java.util.List;

import static org.junit.Assert.*;

/**
 * Created by peet on 19.11.15.
 */
public class EqualityTestBase {
  protected void collectAndAssertEquals(DataSet<Boolean> result) {

    List<Boolean> collectedResult = collectAndAssertSizeOne(result);
    assertTrue("expected equality", collectedResult.get(0));
  }

  protected void collectAndAssertNotEquals(DataSet<Boolean> result) {

    List<Boolean> collectedResult = collectAndAssertSizeOne(result);
    assertFalse("expected inequality", collectedResult.get(0));
  }

  private List<Boolean> collectAndAssertSizeOne(DataSet<Boolean> result) {
    List<Boolean> collectedResult = null;
    try {
      collectedResult = result.collect();
    } catch (Exception e) {
      e.printStackTrace();
    }
    assertNotNull("result is null", collectedResult);
    assertEquals("only one boolean result expected", 1, collectedResult.size());
    return collectedResult;
  }

}
