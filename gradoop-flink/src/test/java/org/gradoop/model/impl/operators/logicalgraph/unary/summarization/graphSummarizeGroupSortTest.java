package org.gradoop.model.impl.operators.logicalgraph.unary.summarization;

import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class graphSummarizeGroupSortTest extends EPGMGraphSummarizeTestBase {

  public graphSummarizeGroupSortTest(TestExecutionMode mode) {
    super(mode);
  }

  @Override
  public Summarization<VertexPojo, EdgePojo, GraphHeadPojo>
  getSummarizationImpl(
    String vertexGroupingKey, boolean useVertexLabel, String edgeGroupingKey,
    boolean useEdgeLabel) {
    return new SummarizationGroupSort<>(vertexGroupingKey, edgeGroupingKey,
      useVertexLabel, useEdgeLabel);
  }
}
