package org.gradoop.model.impl.datagen.foodbroker.config;

import org.apache.commons.io.FileUtils;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.io.File;
import java.io.IOException;

public class FoodBrokerConfig {
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

  public void setScaleFactor(Integer scaleFactor) {
    this.scaleFactor = scaleFactor;
  }

  public Integer getScaleFactor() {
    return scaleFactor;
  }
}
