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

import com.google.common.collect.Lists;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.GradoopTest;
import org.gradoop.model.impl.DefaultEdgeData;
import org.gradoop.model.impl.DefaultGraphData;
import org.gradoop.model.impl.DefaultVertexData;
import org.gradoop.model.impl.EPGMDatabase;

import java.util.List;

public abstract class FlinkTest extends GradoopTest {

  protected ExecutionEnvironment env =
    ExecutionEnvironment.getExecutionEnvironment();

  /**
   * Creates a social network as a basis for tests.
   * <p>
   * An image of the network can be found in
   * gradoop/dev-support/social-network.pdf
   *
   * @return graph store containing a simple social network for tests.
   */
  protected EPGMDatabase<DefaultVertexData, DefaultEdgeData,
    DefaultGraphData> createSocialGraph() {
    return EPGMDatabase
      .fromCollection(createVertexDataCollection(), createEdgeDataCollection(),
        createGraphDataCollection(), env);
  }

  /**
   * Creates a list of long ids from a given string (e.g. "0 1 2 3")
   *
   * @param graphIDString e.g. "0 1 2 3"
   * @return List with long values
   */
  protected List<Long> extractGraphIDs(String graphIDString) {
    String[] tokens = graphIDString.split(" ");
    List<Long> graphIDs = Lists.newArrayListWithCapacity(tokens.length);
    for (String token : tokens) {
      graphIDs.add(Long.parseLong(token));
    }
    return graphIDs;
  }
}
