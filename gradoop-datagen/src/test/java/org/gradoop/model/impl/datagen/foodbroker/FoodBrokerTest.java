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
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */
package org.gradoop.model.impl.datagen.foodbroker;

import org.apache.flink.api.java.DataSet;
import org.gradoop.model.GradoopFlinkTestBase;
import org.gradoop.model.impl.GradoopFlinkTestUtils;
import org.gradoop.model.impl.GraphCollection;
import org.gradoop.model.impl.datagen.foodbroker.config.FoodBrokerConfig;
import org.gradoop.model.impl.pojo.EdgePojo;
import org.gradoop.model.impl.pojo.GraphHeadPojo;
import org.gradoop.model.impl.pojo.VertexPojo;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;

public class FoodBrokerTest extends GradoopFlinkTestBase {
  @Test
  public void testGenerate() throws Exception {

    FoodBrokerConfig config = FoodBrokerConfig.fromFile(
      FoodBrokerTest.class.getResource("/foodbroker/config.json").getFile());

    config.setScaleFactor(0);

    FoodBroker<GraphHeadPojo, VertexPojo, EdgePojo> foodBroker =
      new FoodBroker<>(getExecutionEnvironment(), getConfig(), config);

    GraphCollection<GraphHeadPojo, VertexPojo, EdgePojo> cases =
      foodBroker.execute();

    // GradoopFlinkTestUtils.printGraphCollection(cases);
  }
}