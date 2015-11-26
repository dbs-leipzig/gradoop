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

import com.google.common.collect.Lists;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.gradoop.model.GradoopFlinkTestBase;
import org.gradoop.model.api.EPGMGraphElement;
import org.gradoop.model.impl.LogicalGraph;
import org.gradoop.model.impl.functions.UnaryFunction;
import org.gradoop.model.impl.functions.bool.Equals;
import org.gradoop.model.impl.id.GradoopIds;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.gradoop.util.FlinkAsciiGraphLoader;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static org.junit.Assert.*;

@RunWith(Parameterized.class)
public class LogicalGraphProjectionTest extends GradoopFlinkTestBase {

  public LogicalGraphProjectionTest(TestExecutionMode mode) {
    super(mode);
  }

  @Test
  public void projectionTest() throws Exception {

    FlinkAsciiGraphLoader<VertexPojo, EdgePojo, GraphHeadPojo> loader =
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

    collectAndAssertEquals(result.equalsByElementData(expectation));
    collectAndAssertNotEquals(
      result.getGraphHead()
        .cross(original.getGraphHead())
        .with(new Equals<GraphHeadPojo>())
    );


  }

  private class TestProjection<GE extends EPGMGraphElement>
    implements UnaryFunction<GE, GE> {

    @Override
    public GE execute(GE element) throws Exception {
      element.setLabel(element.getLabel().replace('a', 'b'));
      element.setProperty("k", 1);

      return element;
    }
  }
}
