package org.gradoop.model.impl.operators;

import org.apache.flink.api.java.DataSet;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.util.FlinkAsciiGraphLoader;
import org.gradoop.util.GradoopFlinkConfig;

import java.util.List;

import static org.junit.Assert.*;

public class EqualityTestBase extends org.gradoop.model.FlinkTestBase {

  public EqualityTestBase(TestExecutionMode mode) {
    super(mode);
  }

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

  protected FlinkAsciiGraphLoader<VertexPojo, EdgePojo, GraphHeadPojo>
  getLoaderFromString(
    String asciiGraphs) {
    FlinkAsciiGraphLoader<VertexPojo, EdgePojo, GraphHeadPojo> loader =
      new FlinkAsciiGraphLoader<>(
        GradoopFlinkConfig.createDefaultConfig(getExecutionEnvironment()));

    loader.readDatabaseFromString(asciiGraphs);
    return loader;
  }

}
