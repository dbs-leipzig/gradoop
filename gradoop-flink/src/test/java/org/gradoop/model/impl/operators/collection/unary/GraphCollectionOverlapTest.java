/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.gradoop.model.impl.operators.collection.unary;

import org.gradoop.model.FlinkTestBase;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.util.FlinkAsciiGraphLoader;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(Parameterized.class)
public class GraphCollectionOverlapTest extends GraphCollectionReduceTest {
  public GraphCollectionOverlapTest(TestExecutionMode mode) {
    super(mode);
  }

  @Test
  public void overlapCollectionTest() throws Exception {

    FlinkAsciiGraphLoader<VertexPojo, EdgePojo, GraphHeadPojo> loader =
      getLoaderFromString("" +
        "g1[(a)-[e1]->(b)];g2[(b)-[e2]->(c)];" +
        "g3[(c)-[e3]->(d)];g4[(a)-[e4]->(b)];" +
        "exp12[(b)];" +
        "exp13[];" +
        "exp14[(a)-[e1]->(b)]"
      );

    checkExpectationsEqualResults(loader);
  }
}
