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

  public Integer getMasterDataCount(String className) {
    return getMasterDataOffset(className) +
      (getMasterDataGrowth(className) * scaleFactor);
  }

  public Integer getMinSalesQuotationLines() {
    int minLines = 0;
    try {
      minLines = getSalesQuotationNode().getInt("minLines");
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return minLines;
  }

  protected JSONObject getSalesQuotationNode() throws JSONException {
    return getTransactionalNodes().getJSONObject("SalesQuotation");
  }

  protected JSONObject getTransactionalNodes() throws JSONException {
    return root.getJSONObject("TransactionalData");
  }

  public Integer getMaxQuotationLines() {
    int maxLines = 0;
    try {
      maxLines = getSalesQuotationNode().getInt("maxLines");
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return maxLines;
  }

  public Float getSalesQuotationConfirmationProbability() {
    Float probability = null;
    try {
      probability = (float)
        getSalesQuotationNode().getDouble("probability");
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return probability;
  }

  public Float getSalesQuotationConfirmationProbabilityInfluence() {
    Float influence = null;
    try {
      influence = (float)
        getSalesQuotationNode().getDouble("probabilityInfluence");
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return influence;
  }
}
