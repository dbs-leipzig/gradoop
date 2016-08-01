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
package org.gradoop.model.impl.datagen.foodbroker.config;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.hadoop.shaded.org.apache.http.impl.cookie
  .DateParseException;
import org.apache.flink.hadoop.shaded.org.apache.http.impl.cookie.DateUtils;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.gradoop.model.impl.datagen.foodbroker.tuples.AbstractMasterDataTuple;
import org.gradoop.model.impl.datagen.foodbroker.tuples.MDTuple;
import org.gradoop.model.impl.datagen.foodbroker.tuples.MasterDataTuple;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

public class FoodBrokerConfig implements Serializable {
  private final JSONObject root;
  private Integer scaleFactor = 0;

  public FoodBrokerConfig(String path) throws IOException, JSONException {
    File file = FileUtils.getFile(path);

    root = new JSONObject(FileUtils.readFileToString(file));
  }

  public static FoodBrokerConfig fromFile(String path) throws
    IOException, JSONException {
    return new FoodBrokerConfig(path);
  }

  private JSONObject getMasterDataConfigNode(String className) throws
    JSONException {
    return root.getJSONObject("MasterData").getJSONObject(className);
  }

  public Double getMasterDataGoodRatio(String className)  {

    Double good = null;

    try {
      good = getMasterDataConfigNode(className).getDouble("good");
    } catch (JSONException e) {
      e.printStackTrace();
    }

    return good;
  }

  public Double getMasterDataBadRatio(String className)  {
    Double bad = null;

    try {
      bad = getMasterDataConfigNode(className).getDouble("bad");
    } catch (JSONException e) {
      e.printStackTrace();
    }

    return bad;
  }

  public Integer getMasterDataOffset(String className)  {
    Integer offset = null;

    try {
      offset = getMasterDataConfigNode(className).getInt("offset");
    } catch (JSONException e) {
      e.printStackTrace();
    }

    return offset;
  }


  public Integer getMasterDataGrowth(String className) {
    Integer growth = null;

    try {
      growth = getMasterDataConfigNode(className).getInt("growth");
    } catch (JSONException e) {
      e.printStackTrace();
    }

    return growth;
  }

  public Float getProductMinPrice() {
    Float minPrice = Float.MIN_VALUE;

    try {
      minPrice = (float) getMasterDataConfigNode("Product")
        .getDouble("minPrice");
    } catch (JSONException e) {
      e.printStackTrace();
    }

    return minPrice;
  }

  public Float getProductMaxPrice() {
    Float maxPrice = Float.MIN_VALUE;

    try {
      maxPrice = (float) getMasterDataConfigNode("Product")
        .getDouble("maxPrice");
    } catch (JSONException e) {
      e.printStackTrace();
    }

    return maxPrice;
  }

  public void setScaleFactor(Integer scaleFactor) {
    this.scaleFactor = scaleFactor;
  }

  public Integer getScaleFactor() {
    return scaleFactor;
  }

  public Integer getCaseCount() {

    Integer casesPerScaleFactor = 0;

    try {
      casesPerScaleFactor = root
        .getJSONObject("Process").getInt("casesPerScaleFactor");
    } catch (JSONException e) {
      e.printStackTrace();
    }

    int caseCount = scaleFactor * casesPerScaleFactor;

    if(caseCount == 0) {
      caseCount = 10;
    }

    return caseCount;
  }

  public Date getStartDate() {
    Date startDate = null;

    try {
      startDate = DateUtils.parseDate(root.getJSONObject("Process")
        .getString("casesPerScaleFactor"));
    } catch (DateParseException e) {
      e.printStackTrace();
    } catch (JSONException e) {
      e.printStackTrace();
    }

    return startDate;
  }

  public Integer getMasterDataCount(String className) {
    return getMasterDataOffset(className) +
      (getMasterDataGrowth(className) * scaleFactor);
  }


  protected JSONObject getTransactionalNodes() throws JSONException {
    return root.getJSONObject("TransactionalData");
  }

// Quality

  protected JSONObject getQualityNode() throws JSONException {
    return root.getJSONObject("Quality");
  }

  public Float getQualityGood() {
    Float quality = null;
    try {
      quality = (float)
        getQualityNode().getDouble("good");
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return quality;
  }

  public Float getQualityNormal() {
    Float quality = null;
    try {
      quality = (float)
        getQualityNode().getDouble("normal");
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return quality;
  }

  public Float getQualityBad() {
    Float quality = null;
    try {
      quality = (float)
        getQualityNode().getDouble("bad");
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return quality;
  }

  private boolean getHigherIsBetter(String node, String key,
    boolean defaultValue) {
    Boolean value = defaultValue;
    try {
      value = getTransactionalNodes().getJSONObject(node)
        .getBoolean(key + "HigherIsBetter");
    } catch (JSONException e) {
    }
    return value;
  }

  private Float getInfluence(String node, String key, Float defaultValue) {
    Float value = defaultValue;
    try {
      value = (float)getTransactionalNodes().getJSONObject(node)
        .getDouble(key + "Influence");
    } catch (JSONException e) {
    }
    return value;
  }



  private<T extends AbstractMasterDataTuple> Float getValue(List<T>
    influencedMasterDataObjects,
    boolean higherIsBetter, Float influence, Float startValue) {
    Float value = startValue;

    for (T tuple : influencedMasterDataObjects) {
      if (tuple.getQuality() == 0.66) {
        //getQualityGood()){
        if(higherIsBetter){
          value += influence;
        }
        else{
          value -= influence;
        }
      }
      else if (tuple.getQuality() == getQualityBad()){
        if(higherIsBetter){
          value -= influence;
        }
        else{
          value += influence;
        }
      }
    }
    return value;
  }

  // getIntRangeConfigurationValue(liste, "SalesQuotation",
  // "confirmationDelay")
  public Integer getIntRangeConfigurationValue(List<MasterDataTuple>
    influencedMasterDataObjects, String node, String key) {
    Integer min = 0;
    Integer max = 0;
    Boolean higherIsBetter = null;
    Float influence = null;

    try {
      min = getTransactionalNodes().getJSONObject(node).getInt(key + "Min");
      max = getTransactionalNodes().getJSONObject(node).getInt(key + "Max");
      higherIsBetter = getHigherIsBetter(node, key, true);
      influence = getInfluence(node, key, 0.0f);

    } catch (JSONException e) {
      e.printStackTrace();
    }

    Integer startValue = 1 + (int)((double)(max - min) * Math.random()) +
      min;

    Integer value = getValue(influencedMasterDataObjects, higherIsBetter,
      influence, startValue.floatValue()).intValue();

    if(value < min){
      value = min;
    }
    else if (value > max ){
      value = max;
    }

    return value;
  }

  public BigDecimal getDecimalVariationConfigurationValue(
    List<AbstractMasterDataTuple> influencedMasterDataObjects, String node,
    String key)
    {
    Float baseValue = null;
    Boolean higherIsBetter = null;
    Float influence = null;

    try {
      baseValue = (float) getTransactionalNodes().getJSONObject(node)
        .getDouble(key);
      higherIsBetter = getTransactionalNodes().getJSONObject(node)
        .getBoolean(key + "HigherIsBetter");
      influence = (float) getTransactionalNodes().getJSONObject(node)
        .getDouble(key + "Influence");
    } catch (JSONException e) {
      e.printStackTrace();
    }
     Float value = getValue(influencedMasterDataObjects, higherIsBetter,
       influence, baseValue);

    return BigDecimal.valueOf(value).setScale(2, BigDecimal.ROUND_HALF_UP);
  }

  public boolean happensTransitionConfiguration(
    List<MasterDataTuple> influencedMasterDataObjects, String node, String key)
    {
      Float baseValue = null;
      Boolean higherIsBetter = null;
      Float influence = null;

      try {
        baseValue = (float) getTransactionalNodes().getJSONObject(node)
          .getDouble(key);
        higherIsBetter = getTransactionalNodes().getJSONObject(node)
          .getBoolean(key + "HigherIsBetter");
        influence = (float) getTransactionalNodes().getJSONObject(node)
          .getDouble(key + "Influence");
      } catch (JSONException e) {
        e.printStackTrace();
      }
      Float value = getValue(influencedMasterDataObjects, higherIsBetter,
        influence, baseValue);

      return (float) Math.random() <= value;
  }


  public Date delayDelayConfiguration (Date date, List<MasterDataTuple>
    influencingMasterDataObjects, String node, String key){
    int delay = getIntRangeConfigurationValue(influencingMasterDataObjects,
      node, key);

    Calendar calendar = Calendar.getInstance();
    calendar.setTime(date);
    calendar.add(Calendar.DATE,delay);

    return calendar.getTime();
  }

  public Date delayDelayConfiguration (Date date,
    MasterDataTuple influencingMasterDataObject, String node, String key) {
    List <MasterDataTuple> influencingMasterDataObjects = new ArrayList<>();
    influencingMasterDataObjects.add(influencingMasterDataObject);
    return delayDelayConfiguration(date,influencingMasterDataObjects, node, key);
  }

}
