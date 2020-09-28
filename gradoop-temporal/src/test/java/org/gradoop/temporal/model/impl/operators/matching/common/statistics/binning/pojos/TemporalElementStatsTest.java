/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
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
package org.gradoop.temporal.model.impl.operators.matching.common.statistics.binning.pojos;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.temporal.model.impl.operators.matching.common.statistics.binning.pojo.Binning;
import org.gradoop.temporal.model.impl.operators.matching.common.statistics.binning.pojo.TemporalElementStats;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TemporalElementStatsTest {

  @Test
  public void simpleTemporalElementStatsTest() {
    ArrayList<TemporalVertex> input = getInputVertices();
    TemporalElementStats stats = new TemporalElementStats(5000);
    for (TemporalVertex vertex : input) {
      assertTrue(stats.addElement(vertex));
    }
    Binning[] timeBins = stats.getEstimatedTimeBins();
    assertEquals(timeBins.length, 4);
    Long[] startValues = new Long[] {0L, 10000L, 1000000L, 2000000L};

    for (int i = 0; i < 4; i++) {
      Long[] bins = timeBins[i].getBins();
      assertEquals((long) bins[0], Long.MIN_VALUE);
      for (int k = 1; k < 100; k++) {
        // 50 is the default bin size
        assertEquals((long) bins[k], startValues[i] + (50 * k) - 1);
      }
    }
  }

  @Test
  public void durationStatsTest() {
    ArrayList<TemporalVertex> input = getInputVertices();
    for (int i = 0; i < input.size(); i++) {
      // add some distortion
      // => mean of tx duration now 2.5, variance (0.5)²=0.25
      long newTxFrom = i % 2 == 0 ? 0L : 2L;
      long newTxTo = i % 2 == 0 ? 3L : 4L;
      // => mean of val duration now 7.5, variance (2.5²) = 6.75
      long newValFrom = i % 2 == 0 ? 0L : 5L;
      long newValTo = 10L;
      input.get(i).setTransactionTime(new Tuple2<>(newTxFrom, newTxTo));
      input.get(i).setValidTime(new Tuple2<>(newValFrom, newValTo));
    }

    TemporalElementStats stats = new TemporalElementStats();
    for (TemporalVertex vertex : input) {
      stats.addElement(vertex);
    }

    double txDurationMean = stats.getTxDurationStats()[0];
    double txDurationVariance = stats.getTxDurationStats()[1];
    assertEquals(txDurationMean, 2.5, 0.001);
    assertEquals(txDurationVariance, 0.25, 0.001);

    double valDurationMean = stats.getValDurationStats()[0];
    double valDurationVariance = stats.getValDurationStats()[1];
    assertEquals(valDurationMean, 7.5, 0.001);
    assertEquals(valDurationVariance, 6.25, 0.001);
  }

  @Test
  public void txToValToStatsTest() {
    ArrayList<TemporalVertex> input = getInputVertices();
    for (int i = 0; i < input.size(); i++) {
      input.get(i).setTxFrom(0L);
      input.get(i).setValidFrom(0L);
      /*
       * 1/10th of the vertices: both TOs unbounded
       * 1/10th of the vertices: tx to unbounded
       * 1/5th : val to unbounded
       * if val to bounded, val to = 8
       * half of the unbounded tx to = 8, other half = 16
       * => mean(tx_to - val_to)(both unbounded) = 4
       * => variance(tx_to - val_to)(both unbounded) = 16
       * => P(both unbounded) = 0.8
       */
      Long newTxTo = i % 10 == 0 ? Long.MAX_VALUE :
        i % 2 == 0 ? 8L : 16L;
      Long newValTo = i % 5 == 0 ? Long.MAX_VALUE : 8L;
      input.get(i).setTxTo(newTxTo);
      input.get(i).setValidTo(newValTo);
    }
    TemporalElementStats stats = new TemporalElementStats();
    for (TemporalVertex vertex : input) {
      stats.addElement(vertex);
    }
  }

  @Test
  public void numericalPropertyTest() {
    ArrayList<TemporalVertex> input = getInputVertices();
    //----------------------------------------
    // ----------------  DATA ----------------
    //-----------------------------------------

    //----------------------------------------------------
    // Float values
    //----------------------------------------------------

    // 1% with float property
    int numFloatDecimals = 50;
    String decimalKeyFloat = "float";
    float[] decimalValuesFloat = new float[numFloatDecimals];

    // mean = 1.3f: half of the values have value 1.2, the other half 1.4
    // variance is then (0.1)*(0.1) = 0.01
    for (int i = 0; i < numFloatDecimals; i++) {
      float value = i % 2 == 0 ? 1.2f : 1.4f;
      input.get(i).setProperty(decimalKeyFloat, value);
    }
    double meanFloat = 1.3;
    double varianceFloat = 0.01;

    //----------------------------------------------------
    // Double values
    //----------------------------------------------------

    //3 % with double property
    int numDoubleDecimals = 150;
    String decimalKeyDouble = "double";
    double[] decimalValuesDouble = new double[numDoubleDecimals];

    // mean = 1.0, variance = ((0.5)^2)*2/3 = 1/6
    for (int i = 0; i < numDoubleDecimals; i = i + 3) {
      input.get(i).setProperty(decimalKeyDouble, 1.0);
      input.get(i + 1).setProperty(decimalKeyDouble, 1.5);
      input.get(i + 2).setProperty(decimalKeyDouble, 0.5);
    }
    double meanDouble = 1.0;
    double varianceDouble = 0.166666;

    //----------------------------------------------------
    // Integer values
    //----------------------------------------------------

    //10% with integer property
    int numIntegers = 500;
    String integerKey = "int";
    int[] integerValues = new int[numIntegers];

    // mean = 3, variance = 0.4*(2^2) + 0.4*(1^2) = 2
    for (int i = 0; i < numIntegers; i = i + 5) {
      input.get(i).setProperty(integerKey, 1);
      input.get(i + 1).setProperty(integerKey, 2);
      input.get(i + 2).setProperty(integerKey, 3);
      input.get(i + 3).setProperty(integerKey, 4);
      input.get(i + 4).setProperty(integerKey, 5);
    }
    double meanInteger = 3.;
    double varianceInteger = 2.;

    //----------------------------------------------------
    // Long values
    //----------------------------------------------------
    // 20% with long property
    int numLongs = 1000;
    String longKey = "long";
    int[] longValues = new int[numLongs];

    // mean = 262.5, variance = 62*5^2 = 3906.25
    for (int i = 0; i < numLongs; i++) {
      long value = i % 2 == 0 ? 200 : 325;
      input.get(i).setProperty(longKey, value);
    }
    double meanLong = 262.5;
    double varianceLong = 3906.25;


    TemporalElementStats stats = new TemporalElementStats();
    for (TemporalVertex vertex : input) {
      stats.addElement(vertex);
    }

    //-------------------------------------------
    //--------------RESULTS--------------------
    //------------------------------------------
    Map<String, Double> occurence = stats.getNumericalOccurrenceEstimation();
    Map<String, Double[]> meanAndVariance = stats.getNumericalPropertyStatsEstimation();

    assertEquals(occurence.get(decimalKeyFloat), 0.01, 0.);
    assertEquals(meanAndVariance.get(decimalKeyFloat)[0], meanFloat, 0.);
    assertEquals(meanAndVariance.get(decimalKeyFloat)[1], varianceFloat, 0.00001);

    assertEquals(occurence.get(decimalKeyDouble), 0.03, 0.);
    assertEquals(meanAndVariance.get(decimalKeyDouble)[0], meanDouble, 0.);
    assertEquals(meanAndVariance.get(decimalKeyDouble)[1], varianceDouble, 0.001);

    assertEquals(occurence.get(integerKey), 0.1, 0.);
    assertEquals(meanAndVariance.get(integerKey)[0], meanInteger, 0.);
    assertEquals(meanAndVariance.get(integerKey)[1], varianceInteger, 0.000001);

    assertEquals(occurence.get(longKey), 0.2, 0.);
    assertEquals(meanAndVariance.get(longKey)[0], meanLong, 0.);
    assertEquals(meanAndVariance.get(longKey)[1], varianceLong, 0.);

  }

  @Test
  public void categoricalPropertiesTest() {
    ArrayList<TemporalVertex> input = getInputVertices();

    //first property occurs in 5 nodes (0.1%)
    String prop1 = "prop1";
    // one value 3 times (0.06% of all nodes), 2 further values only once (0.2% of all nodes)
    PropertyValue[] prop1Values = new PropertyValue[] {PropertyValue.create("a"),
      PropertyValue.create("b"), PropertyValue.create("c")};
    input.get(0).setProperty(prop1, prop1Values[0]);
    input.get(1).setProperty(prop1, prop1Values[0]);
    input.get(2).setProperty(prop1, prop1Values[0]);
    input.get(3).setProperty(prop1, prop1Values[1]);
    input.get(4).setProperty(prop1, prop1Values[2]);
    double[] prop1Selectivity = new double[] {0.0006, 0.0002, 0.0002};

    // second property occurs in 10 nodes (0.2%)
    String prop2 = "prop2";
    // two values, 5 times each
    PropertyValue[] prop2Values = new PropertyValue[] {PropertyValue.create("x"),
      PropertyValue.create("y")};
    for (int i = 0; i < 10; i++) {
      String value = i % 2 == 0 ? prop2Values[0].getString() : prop2Values[1].getString();
      input.get(i).setProperty(prop2, value);
    }
    double[] prop2Selectivity = new double[] {0.001, 0.001};


    TemporalElementStats stats = new TemporalElementStats();
    for (TemporalVertex vertex : input) {
      stats.addElement(vertex);
    }
    Map<String, Map<PropertyValue, Double>> categoricalEstimations =
      stats.getCategoricalSelectivityEstimation();
    assertEquals(categoricalEstimations.keySet().size(), 2);

    // first property
    Map<PropertyValue, Double> firstProperty = categoricalEstimations.get(prop1);
    assertEquals(firstProperty.keySet().size(), 3);
    for (int i = 0; i < prop1Values.length; i++) {
      double expected = prop1Selectivity[i];
      double actual = firstProperty.get(prop1Values[i]);
      assertEquals(actual, expected, 0.0001);
    }

    // second
    Map<PropertyValue, Double> secondProperty = categoricalEstimations.get(prop2);
    assertEquals(secondProperty.keySet().size(), 2);
    for (int i = 0; i < prop2Values.length; i++) {
      double expected = prop2Selectivity[i];
      double actual = secondProperty.get(prop2Values[i]);
      assertEquals(actual, expected, 0.0001);
    }
  }

  /**
   * Generates 5000 temporal vertices. All of them have tx duration 10000L and val duration
   * 1000000L
   *
   * @return list of temporal vertices
   */
  private ArrayList<TemporalVertex> getInputVertices() {
    ArrayList<TemporalVertex> input = new ArrayList<>();
    long txFrom = 0L;
    long txTo = 10000L;
    long validFrom = 1000000L;
    long validTo = 2000000L;
    for (int i = 0; i < 5000; i++) {
      TemporalVertex vertex = new TemporalVertex();
      vertex.setLabel("test");
      vertex.setTransactionTime(new Tuple2<>(txFrom, txTo));
      vertex.setValidTime(new Tuple2<>(validFrom, validTo));
      txFrom++;
      txTo++;
      validFrom++;
      validTo++;
      input.add(vertex);
    }
    return input;
  }


}
