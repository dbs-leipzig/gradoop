
package org.gradoop.model.impl.operators.collection.unary;

import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.util.FlinkAsciiGraphLoader;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
public class GraphCollectionCombineTest extends GraphCollectionReduceTest {
  public GraphCollectionCombineTest(TestExecutionMode mode) {
    super(mode);
  }

  @Test
  public void combineCollectionTest() throws Exception {

    FlinkAsciiGraphLoader<VertexPojo, EdgePojo, GraphHeadPojo> loader =
      getLoaderFromString("" +
        "g1[(a)-[e1]->(b)];g2[(b)-[e2]->(c)];" +
        "g3[(c)-[e3]->(d)];g4[(a)-[e4]->(b)];" +
        "exp12[(a)-[e1]->(b)-[e2]->(c)];" +
        "exp13[(a)-[e1]->(b);(c)-[e3]->(d)];" +
        "exp14[(a)-[e1]->(b)]"
      );

    checkExpectationsEqualResults(loader);

  }
}
