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


  protected JSONObject getTransactionalNodes() throws JSONException {
    return root.getJSONObject("TransactionalData");
  }

 // SalesQuotation

  protected JSONObject getSalesQuotationNode() throws JSONException {
    return getTransactionalNodes().getJSONObject("SalesQuotation");
  }

  public Integer getMinSalesQuotationLines() {
    int minLines = 0;
    try {
      minLines = getSalesQuotationNode().getInt("linesMin");
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return minLines;
  }

  public Integer getMaxSalesQuotationLines() {
    int maxLines = 0;
    try {
      maxLines = getSalesQuotationNode().getInt("linesMax");
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return maxLines;
  }

  public Integer getMinSalesQuotationLineQuantity() {
    int minQuantity = 0;
    try {
      minQuantity = getSalesQuotationNode().getInt("lineQuantityMin");
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return minQuantity;
  }

  public Integer getMaxSalesQuotationLineQuantity() {
    int maxQuantity = 0;
    try {
      maxQuantity = getSalesQuotationNode().getInt("lineQuantityMax");
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return maxQuantity;
  }

  public Float getSalesQuotationMargin() {
    Float margin = null;
    try {
      margin = (float)
        getSalesQuotationNode().getDouble("salesMargin");
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return margin;
  }

  public Float getSalesQuotationMarginInfluence() {
    Float influence = null;
    try {
      influence = (float)
        getSalesQuotationNode().getDouble("salesMarginInfluence");
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return influence;
  }

  public Float getSalesQuotationConfirmationProbability() {
    Float probability = null;
    try {
      probability = (float)
        getSalesQuotationNode().getDouble("confirmationProbability");
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return probability;
  }

  public Float getSalesQuotationConfirmationProbabilityInfluence() {
    Float influence = null;
    try {
      influence = (float)
        getSalesQuotationNode().getDouble("confirmationProbabilityInfluence");
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return influence;
  }

  public Float getSalesQuotationLineConfirmationProbability() {
    Float influence = null;
    try {
      influence = (float)
        getSalesQuotationNode().getDouble("lineConfirmationProbability");
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return influence;
  }

  public Integer getMinSalesQuotationConfirmationDelay() {
    int influence = 0;
    try {
      influence = getSalesQuotationNode().getInt("confirmationDelayMin");
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return influence;
  }

  public Integer getMaxSalesQuotationConfirmationDelay() {
    int influence = 0;
    try {
      influence = getSalesQuotationNode().getInt("confirmationDelayMax");
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return influence;
  }

  public Integer getSalesQuotationConfirmationDelayInfluence() {
    int influence = 0;
    try {
      influence = getSalesQuotationNode().getInt("confirmationDelayInfluence");
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return influence;
  }

// SalesOrder

  protected JSONObject getSalesOrderNode() throws JSONException {
    return getTransactionalNodes().getJSONObject("SalesOrder");
  }

  public Integer getMinSalesOrderDeliveryAgreementDelay() {
    int influence = 0;
    try {
      influence = getSalesOrderNode().getInt("deliveryAgreementDelayMin");
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return influence;
  }

  public Integer getMaxSalesOrderDeliveryAgreementDelay() {
    int influence = 0;
    try {
      influence = getSalesOrderNode().getInt("deliveryAgreementDelayMax");
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return influence;
  }

  public Integer getSalesOrderDeliveryAgreementDelayInfluence() {
    int influence = 0;
    try {
      influence = getSalesOrderNode().getInt("deliveryAgreementDelayInfluence");
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return influence;
  }

  public Integer getMinSalesOrderPurchaseDelay() {
    int influence = 0;
    try {
      influence = getSalesOrderNode().getInt("purchaseDelayMin");
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return influence;
  }

  public Integer getMaxSalesOrderPurchaseDelay() {
    int influence = 0;
    try {
      influence = getSalesOrderNode().getInt("purchaseDelayMax");
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return influence;
  }

  public Integer getSalesOrderPurchaseDelayInfluence() {
    int influence = 0;
    try {
      influence = getSalesOrderNode().getInt("purchaseDelayInfluence");
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return influence;
  }

  public Integer getMinSalesOrderInvoiceDelay() {
    int influence = 0;
    try {
      influence = getSalesOrderNode().getInt("invoiceDelayMin");
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return influence;
  }

  public Integer getMaxSalesOrderInvoiceDelay() {
    int influence = 0;
    try {
      influence = getSalesOrderNode().getInt("invoiceDelayMax");
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return influence;
  }

  public Integer getSalesOrderInvoiceDelayInfluence() {
    int influence = 0;
    try {
      influence = getSalesOrderNode().getInt("invoiceDelayInfluence");
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return influence;
  }

// PurchOrder

  protected JSONObject getPurchOrderNode() throws JSONException {
    return getTransactionalNodes().getJSONObject("PurchOrder");
  }

  public Float getPurchOrderPriceVariation() {
    Float influence = null;
    try {
      influence = (float)
        getSalesQuotationNode().getDouble("priceVariation");
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return influence;
  }

  public Float getPurchOrderPriceVariationInfluence() {
    Float influence = null;
    try {
      influence = (float)
        getSalesQuotationNode().getDouble("priceVariationInfluence");
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return influence;
  }

  public Integer getMinPurchOrderDeliveryDelay() {
    int influence = 0;
    try {
      influence = getPurchOrderNode().getInt("deliveryDelayMin");
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return influence;
  }

  public Integer getMaxPurchOrderDeliveryDelay() {
    int influence = 0;
    try {
      influence = getPurchOrderNode().getInt("deliveryDelayMax");
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return influence;
  }

  public Integer getSalesPurchDeliveryDelayInfluence() {
    int influence = 0;
    try {
      influence = getPurchOrderNode().getInt("deliveryDelayInfluence");
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return influence;
  }

  public Integer getMinPurchOrderInvoiceDelay() {
    int influence = 0;
    try {
      influence = getPurchOrderNode().getInt("invoiceDelayMin");
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return influence;
  }

  public Integer getMaxPurchOrderInvoiceDelay() {
    int influence = 0;
    try {
      influence = getPurchOrderNode().getInt("invoiceDelayMax");
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return influence;
  }

  public Integer getSalesPurchInvoiceDelayInfluence() {
    int influence = 0;
    try {
      influence = getPurchOrderNode().getInt("invoiceDelayInfluence");
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return influence;
  }


// Ticket

  protected JSONObject getTicketNode() throws JSONException {
    return getTransactionalNodes().getJSONObject("Ticket");
  }

  public Float getTicketBadQualityProbability() {
    Float influence = null;
    try {
      influence = (float)
        getTicketNode().getDouble("badQualityProbability");
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return influence;
  }

  public Float getTicketBadQualityProbabilityInfluence() {
    Float influence = null;
    try {
      influence = (float)
        getTicketNode().getDouble("badQualityProbabilityInfluence");
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return influence;
  }

  public Float getTicketSalesRefund() {
    Float influence = null;
    try {
      influence = (float)
        getTicketNode().getDouble("salesRefund");
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return influence;
  }

  public Float getTicketSalesRefundInfluence() {
    Float influence = null;
    try {
      influence = (float)
        getTicketNode().getDouble("salesRefundInfluence");
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return influence;
  }

  public Float getTicketPurchRefund() {
    Float influence = null;
    try {
      influence = (float)
        getTicketNode().getDouble("purchRefund");
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return influence;
  }

  public Float getTicketPurchRefundInfluence() {
    Float influence = null;
    try {
      influence = (float)
        getTicketNode().getDouble("salesRefundInfluence");
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return influence;
  }

// Quality

  protected JSONObject getQualityNode() throws JSONException {
    return root.getJSONObject("Quality");
  }

  public Float getQualityGood() {
    Float quality = null;
    try {
      quality = (float)
        getTicketNode().getDouble("good");
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return quality;
  }

  public Float getQualityNormal() {
    Float quality = null;
    try {
      quality = (float)
        getTicketNode().getDouble("normal");
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return quality;
  }

  public Float getQualityBad() {
    Float quality = null;
    try {
      quality = (float)
        getTicketNode().getDouble("bad");
    } catch (JSONException e) {
      e.printStackTrace();
    }
    return quality;
  }



}
