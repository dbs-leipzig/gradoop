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
package org.gradoop.temporal.model.impl.operators.matching.common.statistics.binning.pojo;

import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.temporal.model.impl.pojo.TemporalElement;
import org.s1ck.gdl.model.comparables.time.TimeLiteral;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Basic statistical information about temporal elements with a certain label.
 * Uses reservoir sampling for bigger amounts of data,
 * so that statistics may only be estimated based on a sample(!)
 */
public class TemporalElementStats implements Serializable {

  /**
   * Default Number of bins used for temporal estimations
   */
  public static final int DEFAULT_NUM_BINS = 100;
  /**
   * Holds mean and variance of (tx_from - val_from)
   */
  private final double[] txFromValFromStats;
  /**
   * Samples vertices
   */
  private final ReservoirSampler<TemporalElement> sampler;
  /**
   * The label of the vertices
   */
  private String label;
  /**
   * Frequency distribution for element labels
   */
  private long elementCount;
  /**
   * Bins to estimate distributions of tx_from, tx_to, val_from, val_to values
   */
  private Binning[] estimatedTimeBins;
  /**
   * Holds mean and variance of tx durations
   */
  private double[] txDurationStats;
  /**
   * Holds mean and variance of val durations
   */
  private double[] valDurationStats;
  /**
   * Holds mean and variance of (tx_to - val_to). 3rd element is probability that
   * both tx and val are not as of now
   */
  private double[] txToValToStats;
  /**
   * estimation of the probability that tx.asOf(now) holds for a element
   */
  private double txAsOfNowEstimation;
  /**
   * estimation of the probability that valid.asOf(now) holds for a element
   */
  private double validAsOfNowEstimation;
  /**
   * estimations of the probability that a vertex has a certain property with a
   * certain PropertyValue.
   * This only refers to categorical, i.e. non-numeric properties.
   * The estimations are only computed on the reservoir sample!
   */
  private Map<String, Map<PropertyValue, Double>> categoricalSelectivityEstimation;
  /**
   * Holds for every numerical property an estimation of its mean and variance, based
   * on the reservoir sample
   */
  private Map<String, Double[]> numericalPropertyStatsEstimation;
  /**
   * Holds for every numerical property an estimation of its probability to occur
   * in an element
   */
  private Map<String, Double> numericalOccurrenceEstimation;
  /**
   * flag indicates that temporal statistics should be recomputed as the reservoir sample
   * changed since the last computation.
   */
  private boolean recomputeTemporalDataFlag;
  /**
   * flag indicates that property statistics should be recomputed as the reservoir sample
   * changed since the last computation.
   */
  private boolean recomputePropertyDataFlag;


  /**
   * Creates a element statistics object and set the size of the reservoir sample to compute
   * the statistics
   *
   * @param reservoirSampleSize size of reservoir sample
   */
  public TemporalElementStats(int reservoirSampleSize) {
    label = "";
    elementCount = 0;
    estimatedTimeBins = new Binning[] {};
    txAsOfNowEstimation = 0.;
    validAsOfNowEstimation = 0.;
    categoricalSelectivityEstimation = new HashMap<>();
    numericalPropertyStatsEstimation = new HashMap<>();
    numericalOccurrenceEstimation = new HashMap<>();
    recomputeTemporalDataFlag = false;
    recomputePropertyDataFlag = false;
    txDurationStats = new double[] {};
    valDurationStats = new double[] {};
    txFromValFromStats = new double[] {};
    txToValToStats = new double[] {};
    sampler = new ReservoirSampler<TemporalElement>(reservoirSampleSize);
  }

  /**
   * Creates a element statistics object that uses a reservoir sample of default size
   * {@code SimpleElementStats.DEFAULT_SAMPLE_SIZE} to compute the statistics
   */
  public TemporalElementStats() {
    this(ReservoirSampler.DEFAULT_SAMPLE_SIZE);
  }

  /**
   * Updates the statistics by adding a new element's data
   *
   * @param element the element to include in the stats
   * @return true iff the element was actually included in the reservoir sample
   */
  public boolean addElement(TemporalElement element) {
    elementCount++;

    boolean changed = sampler.updateSample(element);
    recomputeTemporalDataFlag = recomputeTemporalDataFlag || changed;
    recomputePropertyDataFlag = recomputePropertyDataFlag || changed;
    return changed;

  }


  /**
   * Returns the binning estimations for tx_from, tx_to, val_from, val_to depending on
   * the vertices that are currently in the reservoir sample
   *
   * @return binning estimations for temporal data
   */
  public Binning[] getEstimatedTimeBins() {
    if (recomputeTemporalDataFlag) {
      computeTemporalEstimations();
    }
    return estimatedTimeBins.clone();
  }

  /**
   * Returns the estimation of the probability that tx.asOf(now) holds for a element,
   * computed from the current reservoir sample
   *
   * @return estimation of the probability that tx.asOf(now) holds for a element
   */
  public double getTxAsOfNowEstimation() {
    if (recomputeTemporalDataFlag) {
      computeTemporalEstimations();
    }
    return txAsOfNowEstimation;
  }

  /**
   * Returns the estimation of the probability that valid.asOf(now) holds for a element,
   * computed from the current reservoir sample
   *
   * @return estimation of the probability that valid.asOf(now) holds for a element
   */
  public double getValidAsOfNowEstimation() {
    if (recomputeTemporalDataFlag) {
      computeTemporalEstimations();
    }
    return validAsOfNowEstimation;
  }

  /**
   * Initializes or updates all temporal estimations (dependent on the current reservoir
   * sample)
   */
  private void computeTemporalEstimations() {
    Long now = new TimeLiteral("now").getMilliseconds();

    List<TemporalElement> reservoirSample = sampler.getReservoirSample();
    int sampleSize = reservoirSample.size();
    if (sampleSize == 0) {
      return;
    }

    int numBins = Math.min(DEFAULT_NUM_BINS, sampleSize);

    // sample size must be a multiple of the number of bins
    if (sampleSize % numBins != 0) {
      reservoirSample = reservoirSample.subList(0, (
        sampleSize - sampleSize % numBins
      ));
    }

    ArrayList<Long> txFroms = new ArrayList<Long>();
    ArrayList<Long> txTos = new ArrayList<Long>();
    List<Long> txDurations = new ArrayList<Long>();
    ArrayList<Long> valFroms = new ArrayList<Long>();
    ArrayList<Long> valTos = new ArrayList<Long>();
    List<Long> valDurations = new ArrayList<Long>();

    // holds differences between tx and val to values, if they both aren't as of now
    ArrayList<Long> txToValTos = new ArrayList<>();

    int txAsOfNowCount = 0;
    int valAsOfNowCount = 0;
    int bothAsOfNowCount = 0;

    for (TemporalElement element : reservoirSample) {

      long txFrom = element.getTxFrom();
      long txTo = element.getTxTo();
      txFroms.add(txFrom);
      txTos.add(txTo);
      if (txTo == Long.MAX_VALUE) {
        txAsOfNowCount += 1;
        txDurations.add(now - txFrom);
      } else {
        txDurations.add(txTo - txFrom);
      }

      long valFrom = element.getValidFrom();
      long valTo = element.getValidTo();
      valFroms.add(valFrom);
      valTos.add(valTo);
      if (valTo == Long.MAX_VALUE) {
        valAsOfNowCount += 1;
        valDurations.add(now - valFrom);
      } else {
        valDurations.add(valTo - valFrom);
      }

      if (valTo == Long.MAX_VALUE && txTo == Long.MAX_VALUE) {
        bothAsOfNowCount += 1;
      }
      if (valTo != Long.MAX_VALUE && txTo != Long.MAX_VALUE) {
        txToValTos.add(txTo - valTo);
      }
    }


    txAsOfNowEstimation = (double) txAsOfNowCount / (double) sampleSize;
    validAsOfNowEstimation = (double) valAsOfNowCount / (double) sampleSize;

    estimatedTimeBins = new Binning[] {
      new Binning(txFroms, numBins),
      new Binning(txTos, numBins),
      new Binning(valFroms, numBins),
      new Binning(valTos, numBins)
    };


    double txDurationMean = mean(txDurations);
    double txDurationVar = variance(txDurations, txDurationMean);
    txDurationStats = new double[] {txDurationMean, txDurationVar};

    // drop some durations if necessary
        /*txDurations = txDurations.subList(0,
                txDurations.size()- txDurations.size()%10);*/

    double valDurationMean = mean(valDurations);
    double valDurationVar = variance(valDurations, valDurationMean);
    valDurationStats = new double[] {valDurationMean, valDurationVar};

        /*txDurationBins = new Binning(txDurations, txDurations.size()/10);
        valDurations = valDurations.subList(0,
                valDurations.size()- valDurations.size()%10);
        validDurationBins = new Binning(valDurations, valDurations.size()/10);*/

    double txToValToMean = mean(txToValTos);
    double txToValToVar = variance(txToValTos, txToValToMean);
    double txToValToRatio = (double) txToValTos.size() / sampleSize;
    txToValToStats = new double[] {txToValToMean, txToValToVar, txToValToRatio};

    recomputeTemporalDataFlag = false;
  }

  /**
   * Calculates the mean of a list of doubles
   *
   * @param ls list of doubles to calculate the mean of
   * @return mean
   */
  private double mean(List<Long> ls) {
    double mean = 0.;
    for (Long l : ls) {
      mean += (double) l / ls.size();
    }
    return mean;
  }

  /**
   * Calculates the variance of a list of doubles, given its mean
   *
   * @param ls   list to calculate variance of
   * @param mean mean of the list
   * @return variance
   */
  private double variance(List<Long> ls, double mean) {
    double var = 0.;
    for (Long l : ls) {
      var += (l - mean) * (l - mean) * (1. / ls.size());
    }
    return var;
  }

  /**
   * Return the statistics for relation between tx-to and val-to stamps.
   * Recomputes all temporal estimations if necessary.
   * @return statistics for valid time durations in the form {mean, variance}
   */
  public double[] getTxToValToStats() {
    if (recomputeTemporalDataFlag) {
      computeTemporalEstimations();
    }
    return txToValToStats.clone();
  }

  /**
   * Initializes or updates all property related estimations (dependent on the current reservoir
   * sample)
   */
  private void computePropertyEstimations() {
    List<TemporalElement> reservoirSample = sampler.getReservoirSample();
    int sampleSize = reservoirSample.size();
    if (sampleSize == 0) {
      return;
    }

    // classify and collect all property values
    HashMap<String, List<PropertyValue>> categoricalData = new HashMap<>();
    HashMap<String, List<PropertyValue>> numericalData = new HashMap<>();

    for (TemporalElement element : reservoirSample) {
      if (element.getPropertyKeys() == null) {
        continue;
      }
      for (String key : element.getPropertyKeys()) {
        PropertyValue value = element.getPropertyValue(key);
        HashMap<String, List<PropertyValue>> data =
          categoricalData.containsKey(key) ? categoricalData :
            numericalData.containsKey(key) ? numericalData :
              isNumerical(value) ? numericalData : categoricalData;
        if (!data.containsKey(key)) {
          data.put(key, new ArrayList<>());
        }
        data.get(key).add(value);
      }
    }

    computeCategoricalEstimations(categoricalData, sampleSize);
    computeNumericalEstimations(numericalData, sampleSize);
    recomputePropertyDataFlag = false;
  }

  /**
   * Returns the label of the elements
   *
   * @return label of the elements
   */
  public String getLabel() {
    return label;
  }

  /**
   * sets the label
   *
   * @param label label
   */
  public void setLabel(String label) {
    this.label = label;
  }

  /**
   * Checks if a PropertyValue is of numerical type
   *
   * @param value the PropertyValue
   * @return true iff value is of numerical type
   */
  private boolean isNumerical(PropertyValue value) {
    Class cls = value.getType();
    return cls.equals(Integer.class) || cls.equals(Long.class) || cls.equals(Double.class) ||
      cls.equals(Float.class);
  }

  /**
   * Initializes or updates the selectivity estimation for categorical properties
   *
   * @param data       map from categorical property names to a list of their respective
   *                   values in the sample
   * @param sampleSize size of the sample
   */
  private void computeCategoricalEstimations(Map<String, List<PropertyValue>> data,
                                             int sampleSize) {
    categoricalSelectivityEstimation = new HashMap<>();

    for (Map.Entry<String, List<PropertyValue>> entry : data.entrySet()) {
      Map<PropertyValue, Double> selectivityMap = new HashMap<>();

      for (PropertyValue value : entry.getValue()) {
        selectivityMap.put(value,
          selectivityMap.getOrDefault(value, 0.) + 1. / sampleSize);
      }

      categoricalSelectivityEstimation.put(entry.getKey(), selectivityMap);
    }

  }

  /**
   * Initializes or updates the estimations for numerical properties
   *
   * @param data       map from numerical property names to a list of their respective
   *                   values in the sample
   * @param sampleSize size of the sample
   */
  private void computeNumericalEstimations(Map<String, List<PropertyValue>> data,
                                           int sampleSize) {
    numericalOccurrenceEstimation = new HashMap<>();
    numericalPropertyStatsEstimation = new HashMap<>();
    for (Map.Entry<String, List<PropertyValue>> entry : data.entrySet()) {
      List<PropertyValue> values = data.get(entry.getKey());

      numericalOccurrenceEstimation.put(entry.getKey(),
        (double) values.size() / sampleSize);

      Class cls = values.get(0).getType();
      List<Double> doubleValues = values.stream()
        .map(val -> propertyValueToDouble(val, cls))
        .collect(Collectors.toList());

      Double sum = doubleValues.stream().reduce(0., Double::sum);
      Double mean = sum / values.size();
      Double variance = doubleValues.stream().reduce(0.,
        (i, j) -> i + ((j - mean) * (j - mean) * (1. / values.size())));

      numericalPropertyStatsEstimation.put(entry.getKey(), new Double[] {mean, variance});

    }
  }

  /**
   * Converts a numerical PropertyValue to its value as a double
   *
   * @param value PropertyValue to convert
   * @param cls   type of the PropertyValue
   * @return double representation of value
   */
  private double propertyValueToDouble(PropertyValue value, Class cls) {
    if (cls.equals(Integer.class)) {
      return value.getInt();
    }
    if (cls.equals(Double.class)) {
      return value.getDouble();
    }
    if (cls.equals(Long.class)) {
      return (double) value.getLong();
    }
    if (cls.equals(Float.class)) {
      return Double.parseDouble(value.toString());
    }
    return 0.;
  }

  /**
   * Returns a map that maps every numerical property to an estimation of its mean and variance
   * based on the current reservoir sample
   *
   * @return map of property names to double arrays. Every array is of the form {mean, variance}.
   */
  public Map<String, Double[]> getNumericalPropertyStatsEstimation() {
    if (recomputePropertyDataFlag) {
      computePropertyEstimations();
    }
    return numericalPropertyStatsEstimation;
  }

  /**
   * Returns a map that maps every numerical property to an estimation of the probability of
   * its occurrence (based on the current reservoir sample)
   *
   * @return map of property name to probability of its occurence
   */
  public Map<String, Double> getNumericalOccurrenceEstimation() {
    if (recomputePropertyDataFlag) {
      computePropertyEstimations();
    }
    return numericalOccurrenceEstimation;
  }

  /**
   * Returns a map that maps categorical property names to
   * estimations of the probability that an element has this property with a
   * certain PropertyValue.
   * The map is only computed on the sample, so that not every possible property value is
   * included!
   *
   * @return map of property name to a map from property values to their estimated selectivity
   */
  public Map<String, Map<PropertyValue, Double>> getCategoricalSelectivityEstimation() {
    if (recomputePropertyDataFlag) {
      computePropertyEstimations();
    }
    return categoricalSelectivityEstimation;
  }

  public Long getElementCount() {
    return elementCount;
  }

  /**
   * Return the statistics for tx time durations. Recomputes all temporal estimations if
   * necessary.
   * @return statistics for tx time durations in the form {mean, variance}
   */
  public double[] getTxDurationStats() {
    if (recomputeTemporalDataFlag) {
      computeTemporalEstimations();
    }
    return txDurationStats.clone();
  }

  /**
   * Return the statistics for valid time durations. Recomputes all temporal estimations if
   * necessary.
   * @return statistics for valid time durations in the form {mean, variance}
   */
  public double[] getValDurationStats() {
    if (recomputeTemporalDataFlag) {
      computeTemporalEstimations();
    }
    return valDurationStats.clone();
  }

  public List<TemporalElement> getSample() {
    return sampler.getReservoirSample();
  }

  @Override
  public int hashCode() {
    return sampler.hashCode();
  }
}
