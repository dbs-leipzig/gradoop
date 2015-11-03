package org.gradoop.model.impl.operators.logicalgraph.unary.summarization;

import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class graphSummarizeGroupMapTest extends EPGMGraphSummarizeTestBase {

  public graphSummarizeGroupMapTest(TestExecutionMode mode) {
    super(mode);
  }

  @Override
  public Summarization<VertexPojo, EdgePojo, GraphHeadPojo>
  getSummarizationImpl(
    String vertexGroupingKey, boolean useVertexLabel, String edgeGroupingKey,
    boolean useEdgeLabel) {
    return new SummarizationGroupMap<>(vertexGroupingKey, edgeGroupingKey,
      useVertexLabel, useEdgeLabel);
  }
}
