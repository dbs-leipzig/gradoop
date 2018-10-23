/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.flink.datagen.transactions.foodbroker;

import com.google.common.collect.Lists;
import org.apache.commons.io.FileUtils;
import org.codehaus.jettison.json.JSONException;
import org.gradoop.flink.datagen.transactions.foodbroker.config.FoodBrokerConfig;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.util.List;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class FoodBrokerConfigTest extends GradoopFlinkTestBase {

// typical values for normal resources.foodbroker config

  @Test
  public void testGetIntRangeConfigurationValue()
    throws URISyntaxException, IOException, JSONException {
    String configPath = getConfigPath();
    File file = FileUtils.getFile(configPath);
    TestFoodBrokerConfig config = new TestFoodBrokerConfig(FileUtils.readFileToString(file));

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
    File file = FileUtils.getFile(configPath);
    TestFoodBrokerConfig config = new TestFoodBrokerConfig(FileUtils.readFileToString(file));

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

  private String getConfigPath() throws URISyntaxException, UnsupportedEncodingException {
    return getFilePath("/foodbroker/config.json");
  }

  @Test
  public void testHappensTransitionConfiguration()
    throws URISyntaxException, IOException, JSONException {
    String configPath = getConfigPath();
    File file = FileUtils.getFile(configPath);
    TestFoodBrokerConfig config = new TestFoodBrokerConfig(FileUtils.readFileToString(file));

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
  public void testGetValueHomogeneousInfluence()
    throws URISyntaxException, IOException, JSONException {
    String configPath = getConfigPath();
    File file = FileUtils.getFile(configPath);
    TestFoodBrokerConfig config = new TestFoodBrokerConfig(FileUtils.readFileToString(file));

    List<Float> influencingMasterData = Lists.newArrayList(
      0.17f, 0.17f, 0.17f, 0.5f, 0.5f, 0.5f, 0.83f, 0.83f, 0.83f);
    boolean higherIsBetter = true;
    Float influence = 0.1f;
    Float startValue = 3.0f;
    Float result = config.getValue(influencingMasterData, higherIsBetter, influence, startValue);
    assertEquals(3.0f, result, 0.0f);
  }

  @Test
  public void testGetValueHomogeneousInfluence2()
    throws URISyntaxException, IOException, JSONException {
    String configPath = getConfigPath();
    File file = FileUtils.getFile(configPath);
    TestFoodBrokerConfig config = new TestFoodBrokerConfig(FileUtils.readFileToString(file));

    List<Float> influencingMasterData = Lists.newArrayList(
      0.17f, 0.17f, 0.83f, 0.83f);
    boolean higherIsBetter = true;
    Float influence = 0.1f;
    Float startValue = 3.0f;
    Float result = config.getValue(influencingMasterData, higherIsBetter, influence, startValue);
    assertEquals(3.0f, result, 0.0f);
  }

  @Test
  public void testGetValueHomogeneousInfluence3()
    throws URISyntaxException, IOException, JSONException {
    String configPath = getConfigPath();
    File file = FileUtils.getFile(configPath);
    TestFoodBrokerConfig config = new TestFoodBrokerConfig(FileUtils.readFileToString(file));

    List<Float> influencingMasterData = Lists.newArrayList(0.17f, 0.83f);
    boolean higherIsBetter = true;
    Float influence = 0.1f;
    Float startValue = 3.0f;
    Float result = config.getValue(influencingMasterData, higherIsBetter, influence, startValue);
    assertEquals(3.0f, result, 0.0f);
  }


  @Test
  public void testGetValueOnePositiveInfluence()
    throws URISyntaxException, IOException, JSONException {
    String configPath = getConfigPath();
    File file = FileUtils.getFile(configPath);
    TestFoodBrokerConfig config = new TestFoodBrokerConfig(FileUtils.readFileToString(file));

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
  public void testGetValueTwoPositiveInfluence()
    throws URISyntaxException, IOException, JSONException {
    String configPath = getConfigPath();
    File file = FileUtils.getFile(configPath);
    TestFoodBrokerConfig config = new TestFoodBrokerConfig(FileUtils.readFileToString(file));

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
     * @param content config content
     * @throws JSONException
     */
    public TestFoodBrokerConfig(String content) throws JSONException {
      super(content);
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
