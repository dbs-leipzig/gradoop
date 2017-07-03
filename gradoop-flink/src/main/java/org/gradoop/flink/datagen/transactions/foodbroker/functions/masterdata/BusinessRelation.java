
package org.gradoop.flink.datagen.transactions.foodbroker.functions.masterdata;

import org.apache.flink.configuration.Configuration;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.flink.datagen.transactions.foodbroker.config.Constants;
import org.gradoop.flink.datagen.transactions.foodbroker.config.FoodBrokerConfig;
import org.gradoop.flink.datagen.transactions.foodbroker.tuples.MasterDataSeed;

import java.util.List;
import java.util.Random;

/**
 * Creates a person vertex.
 */
public abstract class BusinessRelation extends Person {

  /**
   * List of possible companies.
   */
  private List<String> companies;
  /**
   * List of possible holdings.
   */
  private List<String> holdings;

  /**
   * Valued constructor.
   *
   * @param vertexFactory    EPGM vertex factory
   * @param foodBrokerConfig FoodBroker configuration
   */
  public BusinessRelation(VertexFactory vertexFactory, FoodBrokerConfig foodBrokerConfig) {
    super(vertexFactory, foodBrokerConfig);
  }


  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    // load broadcasted lists
    companies = getRuntimeContext().getBroadcastVariable(Constants.COMPANIES_BC);
    holdings = getRuntimeContext().getBroadcastVariable(Constants.HOLDINGS_BC);
  }

  @Override
  public Vertex map(MasterDataSeed seed) throws  Exception {
    Vertex vertex = super.map(seed);

    // set rnd company
    Random rand = new Random();
    int companyNumber = rand.nextInt(getFoodBrokerConfig().getCompanyCount());
    String company = companies.get(companyNumber);
    String holding = holdings.get(companyNumber % getFoodBrokerConfig().getHoldingCount());
    int branchNumber = rand.nextInt(
      (getFoodBrokerConfig().getBranchMaxAmount() -
        getFoodBrokerConfig().getBranchMinAmount()) +
      1) + getFoodBrokerConfig().getBranchMinAmount();
    vertex.setProperty(Constants.BRANCHNUMBER_KEY, branchNumber);
    vertex.setProperty(Constants.COMPANY_KEY, company);
    vertex.setProperty(Constants.HOLDING_KEY, holding);

    return vertex;
  }
}
