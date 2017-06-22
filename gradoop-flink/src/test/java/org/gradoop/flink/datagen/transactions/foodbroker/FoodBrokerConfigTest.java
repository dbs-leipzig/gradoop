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
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Paths;
import java.util.List;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class FoodBrokerConfigTest extends GradoopFlinkTestBase {

// typical values for normal resources.foodbroker config

  @Test
  public void testGetIntRangeConfigurationValue()
    throws URISyntaxException, IOException, JSONException {
    String configPath = getConfigPath();
    TestFoodBrokerConfig config = new TestFoodBrokerConfig(configPath);

    List<Float> influencingMasterData = Lists.newArrayList(0.33f, 0.01f);
    boolean higherIsBetter = true;
    Float influence = 0.0f;
    Float startValue = 3.0f;
    Float resultInt = config.getValue(influencingMasterData, higherIsBetter, influence, startValue);
    assertEquals(3.0f, resultInt, 0.00001f);

    influencingMasterData = Lists.newArrayList();
    higherIsBetter = true;
    influence = 5.0f;
    startValue = 55f;
    resultInt = config.getValue(influencingMasterData, higherIsBetter, influence, startValue);
    assertEquals(55.0f, resultInt, 0.00001f);

    influencingMasterData = Lists.newArrayList(0.33f, 0.33f);
    higherIsBetter = true;
    influence = 0.0f;
    startValue = 21.0f;
    resultInt = config.getValue(influencingMasterData, higherIsBetter, influence, startValue);
    assertEquals(21.0f, resultInt, 0.00001f);

    influencingMasterData = Lists.newArrayList(0.01f, 0.32f, 0.66f);
    higherIsBetter = false;
    influence = 1.0f;
    startValue = 1.0f;
    resultInt = config.getValue(influencingMasterData, higherIsBetter, influence, startValue);
    assertEquals(2.0f, resultInt, 0.00001f);

    influencingMasterData = Lists.newArrayList(0.32f, 0.66f, 0.99f);
    higherIsBetter = false;
    influence = 1.0f;
    startValue = 1.0f;
    resultInt = config.getValue(influencingMasterData, higherIsBetter, influence, startValue);
    assertEquals(0.0f, resultInt, 0.00001f);
  }

  @Test
  public void testGetDecimalVariationConfigurationValue()
    throws URISyntaxException, IOException, JSONException {
    String configPath = getConfigPath();
    TestFoodBrokerConfig config = new TestFoodBrokerConfig(configPath);

    List<Float> influencingMasterData = Lists.newArrayList(0.33f, 0.32f, 0.33f);
    boolean higherIsBetter = true;
    Float influence = 0.02f;
    Float startValue = 0.05f;
    Float result = config.getValue(influencingMasterData, higherIsBetter, influence, startValue);
    assertEquals(0.03f, result, 0.00001f);

    influencingMasterData = Lists.newArrayList(0.33f, 0.33f, 0.66f);
    higherIsBetter = true;
    influence = 0.02f;
    startValue = 0.05f;
    result = config.getValue(influencingMasterData, higherIsBetter, influence, startValue);
    assertEquals(0.05f, result, 0.00001f);

    influencingMasterData = Lists.newArrayList(0.50f, 0.82f);
    higherIsBetter = false;
    influence = 0.02f;
    startValue = 0.01f;
    result = config.getValue(influencingMasterData, higherIsBetter, influence, startValue);
    assertEquals(-0.01f, result, 0.00001f);
  }

  private String getConfigPath() throws URISyntaxException {
    return FoodBroker.class.getResource("/foodbroker/config.json").getFile();
  }

  @Test
  public void testHappensTransitionConfiguration()
    throws URISyntaxException, IOException, JSONException {
    String configPath = getConfigPath();
    TestFoodBrokerConfig config = new TestFoodBrokerConfig(configPath);

    List<Float> influencingMasterData = Lists.newArrayList(0.50f, 0.82f);
    boolean higherIsBetter = true;
    Float influence = 0.2f;
    Float startValue = 0.6f;
    Float result = config.getValue(influencingMasterData, higherIsBetter, influence, startValue);
    assertEquals(0.8f, result, 0.00001f);

    influencingMasterData = Lists.newArrayList(0.32f, 0.50f);
    higherIsBetter = true;
    influence = 0.2f;
    startValue = 0.6f;
    result = config.getValue(influencingMasterData, higherIsBetter, influence, startValue);
    assertEquals(0.4f, result, 0.00001f);

    influencingMasterData = Lists.newArrayList(0.82f, 0.82f);
    higherIsBetter = true;
    influence = 0.2f;
    startValue = 0.6f;
    result = config.getValue(influencingMasterData, higherIsBetter, influence, startValue);
    assertEquals(1.0f, result, 0.00001f);
  }


  // additional tests

  @Test
  public void testGetValueHomogenInfluence()
    throws URISyntaxException, IOException, JSONException {
    String configPath = getConfigPath();
    TestFoodBrokerConfig config = new TestFoodBrokerConfig(configPath);

    List<Float> influencingMasterData = Lists.newArrayList(
      0.17f, 0.17f, 0.17f, 0.5f, 0.5f, 0.5f, 0.83f, 0.83f, 0.83f);
    boolean higherIsBetter = true;
    Float influence = 0.1f;
    Float startValue = 3.0f;
    Float result = config.getValue(influencingMasterData, higherIsBetter, influence, startValue);
    assertEquals(3.0f, result, 0.0f);
  }

  @Test
  public void testGetValueHomogenInfluence2()
    throws URISyntaxException, IOException, JSONException {
    String configPath = getConfigPath();
    TestFoodBrokerConfig config = new TestFoodBrokerConfig(configPath);

    List<Float> influencingMasterData = Lists.newArrayList(
      0.17f, 0.17f, 0.83f, 0.83f);
    boolean higherIsBetter = true;
    Float influence = 0.1f;
    Float startValue = 3.0f;
    Float result = config.getValue(influencingMasterData, higherIsBetter, influence, startValue);
    assertEquals(3.0f, result, 0.0f);
  }

  @Test
  public void testGetValueHomogenInfluence3()
    throws URISyntaxException, IOException, JSONException {
    String configPath = getConfigPath();
    TestFoodBrokerConfig config = new TestFoodBrokerConfig(configPath);

    List<Float> influencingMasterData = Lists.newArrayList(0.17f, 0.83f);
    boolean higherIsBetter = true;
    Float influence = 0.1f;
    Float startValue = 3.0f;
    Float result = config.getValue(influencingMasterData, higherIsBetter, influence, startValue);
    assertEquals(3.0f, result, 0.0f);
  }


  @Test
  public void testGetValueOnePositivInfluence()
    throws URISyntaxException, IOException, JSONException {
    String configPath = getConfigPath();
    TestFoodBrokerConfig config = new TestFoodBrokerConfig(configPath);

    List<Float> influencingMasterData = Lists.newArrayList(
      0.16f, 0.16f, 0.5f, 0.5f, 0.5f, 0.83f, 0.83f, 0.83f);
    boolean higherIsBetter = true;
    Float influence = 0.1f;
    Float startValue = 3.0f;
    Float result = config.getValue(influencingMasterData, higherIsBetter, influence, startValue);
    // because of the normal values (0.5) the avg value inclines to 0.5 and
    // the positive influence is reduced
    assertEquals(3.0f, result, 0.0f);
  }

  @Test
  public void testGetValueTwoPositivInfluence()
    throws URISyntaxException, IOException, JSONException {
    String configPath = getConfigPath();
    TestFoodBrokerConfig config = new TestFoodBrokerConfig(configPath);

    List<Float> influencingMasterData = Lists.newArrayList(0.01f, 0.99f, 0.99f, 0.99f);
    boolean higherIsBetter = true;
    Float influence = 0.1f;
    Float startValue = 3.0f;
    Float result = config.getValue(influencingMasterData, higherIsBetter, influence, startValue);
    assertEquals(3.2f, result, 0.0f);
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

    /**
     * {@inheritDoc}
     */
    @Override
    public Float getValue(List<Float> influencingMasterDataQuality, boolean higherIsBetter,
      Float influence, Float startValue) {
      return super.getValue(influencingMasterDataQuality, higherIsBetter, influence, startValue);
    }
  }
}
