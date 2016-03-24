package org.gradoop.model.impl.datagen.foodbroker.generator;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.model.api.EPGMEdge;
import org.gradoop.model.api.EPGMVertex;
import org.gradoop.model.impl.datagen.foodbroker.config.FoodBrokerConfig;

import java.util.ArrayList;
import java.util.Collection;

public class CaseGenerator<V extends EPGMVertex, E extends EPGMEdge> {

  private final FoodBrokerConfig foodBrokerConfig;
  private final ExecutionEnvironment executionEnvironment;

  public CaseGenerator(ExecutionEnvironment env,
    FoodBrokerConfig foodBrokerConfig) {
    this.executionEnvironment = env;
    this.foodBrokerConfig = foodBrokerConfig;
  }

  public DataSet<Long> generate() {

    Integer caseCount = foodBrokerConfig.getCaseCount();

    if(caseCount == 0) {
      caseCount = 10;
    }

    Collection<Long> caseSeeds = new ArrayList<>();

    for(int i = 1; i <= caseCount; i++) {
      caseSeeds.add((long) i);
    }

    return executionEnvironment
      .fromCollection(caseSeeds);
  }
}
