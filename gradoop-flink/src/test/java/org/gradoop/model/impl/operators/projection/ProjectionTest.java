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

package org.gradoop.model.impl.operators.projection;

import org.gradoop.model.GradoopFlinkTestBase;
import org.gradoop.model.api.EPGMGraphElement;
import org.gradoop.model.api.functions.ProjectionFunction;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.functions.bool.Equals;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.util.FlinkAsciiGraphLoader;
import org.junit.Test;

public class ProjectionTest extends GradoopFlinkTestBase {

  @Test
  public void projectionTest() throws Exception {

    FlinkAsciiGraphLoader<GraphHeadPojo, VertexPojo, EdgePojo> loader =
      getLoaderFromString("" +
        "org:Ga{k=0}[(:Va{k=0})-[:ea{k=0}]->(:Va{k=0})];" +
        "exp:Ga{k=0}[(:Vb{k=1})-[:eb{k=1}]->(:Vb{k=1})]"
      );

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> original = loader
      .getLogicalGraphByVariable("org");

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo> expectation = loader
      .getLogicalGraphByVariable("exp");

    LogicalGraph<GraphHeadPojo, VertexPojo, EdgePojo>
      result = original.project(
      new TestProjection<VertexPojo>(),
      new TestProjection<EdgePojo>()
    );

    collectAndAssertTrue(result.equalsByElementData(expectation));
    collectAndAssertFalse(
      result.getGraphHead()
        .cross(original.getGraphHead())
        .with(new Equals<GraphHeadPojo>())
    );
  }

  public static class TestProjection<GE extends EPGMGraphElement>
    implements ProjectionFunction<GE> {

    @Override
    public GE execute(GE element) throws Exception {
      element.setLabel(element.getLabel().replace('a', 'b'));
      element.setProperty("k", 1);

      return element;
    }
  }
}
