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

package org.gradoop.flink.datagen.transactions.foodbroker;

import com.google.common.collect.Lists;
import org.codehaus.jettison.json.JSONException;
import org.gradoop.flink.datagen.transactions.foodbroker.config.FoodBrokerConfig;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.junit.Test;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class FoodBrokerConfigTest extends GradoopFlinkTestBase {


  @Test
  public void testGetValueHomogenInfluence() throws URISyntaxException, IOException, JSONException {
    String configPath = Paths.get(
      FoodBrokerTest.class.getResource("/foodbroker/config.json").toURI()).toFile().getPath();

    TestFoodBrokerConfig config = new TestFoodBrokerConfig(configPath);

    List<Float> influencingMasterData = Lists.newArrayList(
      0.33f, 0.33f, 0.33f, 0.5f, 0.5f, 0.5f, 0.67f, 0.67f, 0.67f);

    boolean higherIsBetter = true;
    Float influence = 0.1f;
    Float startValue = 3.0f;

    Float result = config.getValue(influencingMasterData, higherIsBetter, influence, startValue);
    assertEquals(3.0f, result.floatValue(), 0.0f);
  }

  @Test
  public void testGetValueHomogenInfluence2() throws URISyntaxException, IOException,
    JSONException {
    String configPath = Paths.get(
      FoodBrokerTest.class.getResource("/foodbroker/config.json").toURI()).toFile().getPath();

    TestFoodBrokerConfig config = new TestFoodBrokerConfig(configPath);

    List<Float> influencingMasterData = Lists.newArrayList(
      0.33f, 0.33f, 0.67f, 0.67f);

    boolean higherIsBetter = true;
    Float influence = 0.1f;
    Float startValue = 3.0f;

    Float result = config.getValue(influencingMasterData, higherIsBetter, influence, startValue);
    assertEquals(3.0f, result.floatValue(), 0.0f);
  }

  @Test
  public void testGetValueHomogenInfluence3() throws URISyntaxException, IOException,
    JSONException {
    String configPath = Paths.get(
      FoodBrokerTest.class.getResource("/foodbroker/config.json").toURI()).toFile().getPath();

    TestFoodBrokerConfig config = new TestFoodBrokerConfig(configPath);

    List<Float> influencingMasterData = Lists.newArrayList(
      0.33f, 0.67f);

    boolean higherIsBetter = true;
    Float influence = 0.1f;
    Float startValue = 3.0f;

    Float result = config.getValue(influencingMasterData, higherIsBetter, influence, startValue);
    assertEquals(3.0f, result.floatValue(), 0.0f);
  }


  @Test
  public void testGetValueOnePositivInfluence() throws URISyntaxException, IOException,
    JSONException {
    String configPath = Paths.get(
      FoodBrokerTest.class.getResource("/foodbroker/config.json").toURI()).toFile().getPath();

    TestFoodBrokerConfig config = new TestFoodBrokerConfig(configPath);

    List<Float> influencingMasterData = Lists.newArrayList(
      0.33f, 0.33f, 0.5f, 0.5f, 0.5f, 0.67f, 0.67f, 0.67f);

    boolean higherIsBetter = true;
    Float influence = 0.1f;
    Float startValue = 3.0f;

    Float result = config.getValue(influencingMasterData, higherIsBetter, influence, startValue);
    assertEquals(3.1f, result.floatValue(), 0.0f);
  }

  @Test
  public void testGetValueTwoPositivInfluence() throws URISyntaxException, IOException,
    JSONException {
    String configPath = Paths.get(
      FoodBrokerTest.class.getResource("/foodbroker/config.json").toURI()).toFile().getPath();

    TestFoodBrokerConfig config = new TestFoodBrokerConfig(configPath);

    List<Float> influencingMasterData = Lists.newArrayList(
      0.33f, 0.67f, 0.67f, 0.67f);
//      0.33f, 0.5f, 0.5f, 0.5f, 0.67f, 0.67f, 0.67f);

    boolean higherIsBetter = true;
    Float influence = 0.1f;
    Float startValue = 3.0f;

    Float result = config.getValue(influencingMasterData, higherIsBetter, influence, startValue);
    assertEquals(3.2f, result.floatValue(), 0.0f);
  }


  private class TestFoodBrokerConfig extends FoodBrokerConfig {

    /**
     * Valued constructor.
     *
     * @param path path to config file
     * @throws IOException
     * @throws JSONException
     */
    public TestFoodBrokerConfig(String path) throws IOException, JSONException {
      super(path);
    }

    public Float getValue(List<Float> influencingMasterDataQuality, boolean higherIsBetter,
      Float influence, Float startValue) {
      return super.getValue(influencingMasterDataQuality, higherIsBetter, influence, startValue);
    }
  }
}
