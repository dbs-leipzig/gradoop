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

package org.gradoop.model.impl.operators.logicalgraph.unary;

import org.gradoop.model.FlinkTestBase;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.functions.UnaryFunction;
import org.gradoop.model.impl.id.GradoopIds;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(Parameterized.class)
public class LogicalGraphAggregateTest extends FlinkTestBase {

  public LogicalGraphAggregateTest(TestExecutionMode mode) {
    super(mode);
  }

  @Test
  public void aggregateEdgeCountTest() throws Exception {

    LogicalGraph<VertexPojo, EdgePojo, GraphHeadPojo>
      forumGraph = getGraphStore().getGraph(GradoopIds.fromLong(3L));
    final String aggPropertyKey = "eCount";

    UnaryFunction<LogicalGraph<VertexPojo, EdgePojo, GraphHeadPojo>, Long>
      aggregateFunc =
      new UnaryFunction<LogicalGraph<VertexPojo, EdgePojo, GraphHeadPojo>, Long>() {
        @Override
        public Long execute(
          LogicalGraph<VertexPojo, EdgePojo, GraphHeadPojo>
            entity) throws
          Exception {
          return entity.getEdges().count();
        }
      };

    LogicalGraph<VertexPojo, EdgePojo, GraphHeadPojo>
      newGraph = forumGraph.aggregate(aggPropertyKey, aggregateFunc);

    assertNotNull("graph was null", newGraph);
    assertEquals("wrong property count", 1, newGraph.getPropertyCount());
    assertEquals("wrong property value", 4L,
      newGraph.getProperty(aggPropertyKey));

    // original graph needs to be unchanged
    assertEquals("wrong property count", 0L, forumGraph.getPropertyCount());
  }
}
