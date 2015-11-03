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

package org.gradoop.model;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.test.util.MultipleProgramsTestBase;
import org.gradoop.GradoopTestBaseUtils;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.model.impl.EPGMDatabase;

/**
 * Used for tests that require a Flink cluster up and running.
 */
public class FlinkTestBase extends MultipleProgramsTestBase {

  private EPGMDatabase<VertexPojo, EdgePojo, GraphHeadPojo>
    graphStore;

  private ExecutionEnvironment env;

  public FlinkTestBase(TestExecutionMode mode) {
    super(mode);
    this.env = ExecutionEnvironment.getExecutionEnvironment();
    this.graphStore = createSocialGraph();
  }

  /**
   * Creates a social network as a basis for tests.
   * <p/>
   * An image of the network can be found in
   * gradoop/dev-support/social-network.pdf
   *
   * @return graph store containing a simple social network for tests.
   */
  protected EPGMDatabase<VertexPojo, EdgePojo, GraphHeadPojo> createSocialGraph() {
    return EPGMDatabase
      .fromCollection(GradoopTestBaseUtils.createVertexPojoCollection(),
        GradoopTestBaseUtils.createEdgePojoCollection(),
        GradoopTestBaseUtils.createGraphHeadCollection(), env);
  }

  protected EPGMDatabase<VertexPojo, EdgePojo, GraphHeadPojo> getGraphStore() {
    return graphStore;
  }

  protected ExecutionEnvironment getExecutionEnvironment() {
    return env;
  }
}
