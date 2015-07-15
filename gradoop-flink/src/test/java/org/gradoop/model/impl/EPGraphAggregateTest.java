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

package org.gradoop.model.impl;

import org.gradoop.model.EPFlinkTest;
import org.gradoop.model.helper.UnaryFunction;
import org.gradoop.model.store.EPGraphStore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class EPGraphAggregateTest extends EPFlinkTest {
  private EPGraphStore graphStore;

  public EPGraphAggregateTest() {
    this.graphStore = createSocialGraph();
  }

  @Test
  public void aggregateEdgeCountTest() throws Exception {

    EPGraph forumGraph = graphStore.getGraph(3L);
    final String aggPropertyKey = "eCount";

    UnaryFunction<EPGraph, Long> aggregateFunc =
      new UnaryFunction<EPGraph, Long>() {
        @Override
        public Long execute(EPGraph entity) throws Exception {
          return entity.getEdges().size();
        }
      };

    EPGraph newGraph = forumGraph.aggregate(aggPropertyKey, aggregateFunc);

    assertNotNull("graph was null", newGraph);
    assertEquals("wrong property count", 1, newGraph.getPropertyCount());
    assertEquals("wrong property value", 4L,
      newGraph.getProperty(aggPropertyKey));

    // original graph needs to be unchanged
    assertEquals("wrong property count", 0L, forumGraph.getPropertyCount());
  }
}
