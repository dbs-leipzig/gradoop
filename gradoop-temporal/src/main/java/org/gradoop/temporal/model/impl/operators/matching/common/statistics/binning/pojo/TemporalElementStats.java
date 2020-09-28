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
import org.s1ck.gdl.model.comparables.time.TimeSelector;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
   * Samples vertices
   */
  private final ReservoirSampler<TemporalElement> sampler;
  /**
   * The label of the vertices/edges
   */
  private String label;
  /**
   * number of elements in the sample
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
   * Holds mean, variance and prob. that val_from=Long.MIN_VALUE
   */
  private double[] valFromStats;
  /**
   * Holds mean, variance and prob. that tx_from=Long.MIN_VALUE
   */
  private double[] txFromStats;
  /**
   * Holds mean, variance and prob. that val_to=Long.MAX_VALUE
   */
  private double[] valToStats;
  /**
   * Holds mean, variance and prob. that tx_to=Long.MAX_VALUE
   */
  private double[] txToStats;
  /**
   * list of numerical properties
   */
  private Set<String> numericalProperties;
  /**
   * list of categorical properties
   */
  private Set<String> categoricalProperties;
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
   * the statistics. The numerical and categorical properties are set by the user.
   * Omitted properties are ignored, always assigned the probability 0.5
   * @param reservoirSampleSize size of reservoir sample
   * @param numericalProperties list of numerical properties to consider
   * @param categoricalProperties list of categorical properties to consider
   */
  public TemporalElementStats(int reservoirSampleSize, Set<String> numericalProperties,
                              Set<String> categoricalProperties) {
    this(reservoirSampleSize);
    this.numericalProperties = numericalProperties;
    this.categoricalProperties = categoricalProperties;
  }

  /**
   * Creates a element statistics object and set the size of the reservoir sample to compute
   * the statistics. The numerical and categorical properties are determined
   * automatically later (possibly not working very good), all properties are
   * considered.
   *
   * @param reservoirSampleSize size of reservoir sample
   */
  public TemporalElementStats(int reservoirSampleSize) {
    label = "";
    elementCount = 0;
    estimatedTimeBins = new Binning[] {};
    categoricalSelectivityEstimation = new HashMap<>();
    numericalPropertyStatsEstimation = new HashMap<>();
    numericalOccurrenceEstimation = new HashMap<>();
    recomputeTemporalDataFlag = false;
    recomputePropertyDataFlag = false;
    valFromStats = new double[] {};
    txFromStats = new double[] {};
    valToStats = new double[] {};
    txToStats = new double[] {};
    txDurationStats = new double[] {};
    valDurationStats = new double[] {};
    sampler = new ReservoirSampler<TemporalElement>(reservoirSampleSize);
  }

  /**
   * Creates a element statistics object that uses a reservoir sample of default size
   * {@code SimpleElementStats.DEFAULT_SAMPLE_SIZE} to compute the statistics
   * All properties will be considered, their type (numerical/categorical) detected
   * automatically (may not work very good).
   */
  public TemporalElementStats() {
    this(ReservoirSampler.DEFAULT_SAMPLE_SIZE);
  }

  /**
   * Creates an element statistics object that uses a reservoir sample of default size.
   * Numerical and categorical properties to consider are given explicitly
   * @param numericalProperties numerical properties to consider
   * @param categoricalProperties categorical properties to consider
   */
  public TemporalElementStats(Set<String> numericalProperties, Set<String> categoricalProperties) {
    this(ReservoirSampler.DEFAULT_SAMPLE_SIZE, numericalProperties, categoricalProperties);
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
    // set recompute flags, if the element was actually added
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
      reservoirSample = reservoirSample.subList(0,
        sampleSize - sampleSize % numBins);
    }

    // lists collecting all values for each time property, i.e. Long.MIN_VALUE/MAX_VALUE, too
    ArrayList<Long> allTxFroms = new ArrayList<Long>();
    ArrayList<Long> allTxTos = new ArrayList<Long>();
    ArrayList<Long> allValFroms = new ArrayList<Long>();
    ArrayList<Long> allValTos = new ArrayList<Long>();

    // lists collecting only those values for each time property unequal to Long.MIN_VALUE/MAX_VALUE, too
    ArrayList<Long> txFroms = new ArrayList<Long>();
    ArrayList<Long> txTos = new ArrayList<Long>();
    ArrayList<Long> valFroms = new ArrayList<Long>();
    ArrayList<Long> valTos = new ArrayList<Long>();

    List<Long> valDurations = new ArrayList<Long>();
    List<Long> txDurations = new ArrayList<Long>();
/*    // holds differences between tx and val to values, if they both aren't as of now
    ArrayList<Long> txToValTos = new ArrayList<>();*/

    // count, how often to values are Long.MAX_VALUE
    int txToMaxCount = 0;
    int valToMaxCount = 0;
    // count, how often from values are Long.MIN_VALUE
    int txFromMinCount = 0;
    int valFromMinCount = 0;

    for (TemporalElement element : reservoirSample) {
      // collect all relevant temporal properties from the reservoir
      long txFrom = element.getTxFrom();
      long txTo = element.getTxTo();
      allTxFroms.add(txFrom);
      allTxTos.add(txTo);
      if (txFrom > Long.MIN_VALUE) {
        txFroms.add(txFrom);
      } else {
        txFromMinCount++;
      }
      if (txTo == Long.MAX_VALUE) {
        txToMaxCount  += 1;
        txDurations.add(now - txFrom);
      } else {
        txTos.add(txTo);
        txDurations.add(txTo - txFrom);
      }

      long valFrom = element.getValidFrom();
      long valTo = element.getValidTo();
      allValFroms.add(valFrom);
      allValTos.add(valTo);
      if (valFrom > Long.MIN_VALUE) {
        valFroms.add(valFrom);
      } else {
        valFromMinCount++;
      }
      if (valTo == Long.MAX_VALUE) {
        valToMaxCount  += 1;
        valDurations.add(now - valFrom);
      } else {
        valTos.add(valTo);
        valDurations.add(valTo - valFrom);
      }

    }

    // statistics for temporal properties (without binning)
    double txFromMean = mean(txFroms);
    double txFromVariance = variance(txFroms, txFromMean);
    double txFromMinEstimation = (double) txFromMinCount / (double) sampleSize;
    txFromStats = new double[] {txFromMean, txFromVariance, txFromMinEstimation};

    double valFromMean = mean(valFroms);
    double valFromVariance = variance(valFroms, valFromMean);
    double valFromMinEstimation = (double) valFromMinCount / (double) sampleSize;
    valFromStats = new double[] {valFromMean, valFromVariance, valFromMinEstimation};

    double txToMean = mean(txTos);
    double txToVariance = variance(txTos, txToMean);
    double txToMaxEstimation = (double) txToMaxCount / (double) sampleSize;
    txToStats = new double[] {txToMean, txToVariance, txToMaxEstimation};

    double valToMean = mean(valTos);
    double valToVariance = variance(valTos, valToMean);
    double valToMaxEstimation = (double) valToMaxCount / (double) sampleSize;
    valToStats = new double[] {valToMean, valToVariance, valToMaxEstimation};

    // create binnings for the temporal properties
    estimatedTimeBins = new Binning[] {
      new Binning(allTxFroms, numBins),
      new Binning(allTxTos, numBins),
      new Binning(allValFroms, numBins),
      new Binning(allValTos, numBins)
    };


    // determine means and variances of the durations
    double txDurationMean = mean(txDurations);
    double txDurationVar = variance(txDurations, txDurationMean);
    txDurationStats = new double[] {txDurationMean, txDurationVar};

    double valDurationMean = mean(valDurations);
    double valDurationVar = variance(valDurations, valDurationMean);
    valDurationStats = new double[] {valDurationMean, valDurationVar};


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
      mean  += (double) l / ls.size();
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
      var  += (l - mean) * (l - mean) * (1. / ls.size());
    }
    return var;
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

    // classify all property values, if not already done
    if (categoricalProperties == null || numericalProperties == null) {
      detectPropertyTypes(reservoirSample);
    }
    // these maps collect all property values in the sample
    HashMap<String, List<PropertyValue>> categoricalData = new HashMap<>();
    HashMap<String, List<PropertyValue>> numericalData = new HashMap<>();

    // add element data to the corresponding map
    for (TemporalElement element : reservoirSample) {
      if (element.getPropertyKeys() == null) {
        continue;
      }
      for (String key : element.getPropertyKeys()) {
        PropertyValue value = element.getPropertyValue(key);
        HashMap<String, List<PropertyValue>> data;
        // no outlier, i.e. NULL, NaN etc.?
        if (numericalProperties.contains(key) && isNumerical(value)) {
          data = numericalData;
        } else if (categoricalProperties.contains(key)) {
          data = categoricalData;
        } else {
          continue;
        }
/*        // original
        HashMap<String, List<PropertyValue>> data =
            numericalData.containsKey(key) ? numericalData :
              isNumerical(value) ? numericalData : categoricalData;*/
        data.putIfAbsent(key, new ArrayList<>());
        data.get(key).add(value);
      }
    }
    computeCategoricalEstimations(categoricalData, sampleSize);
    computeNumericalEstimations(numericalData, sampleSize);
    recomputePropertyDataFlag = false;
  }

  /**
   * Tries to automatically detect which properties are numerical and categorical.
   * Sets {@code numericalProperties} and {@code categoricalProperties} accordingly.
   * @param reservoirSample the list of elements to use for the detection
   */
  private void detectPropertyTypes(List<TemporalElement> reservoirSample) {
    numericalProperties = new HashSet<>();
    categoricalProperties = new HashSet<>();
    // maps count which property is considered numerical/categorical how often
    HashMap<String, Integer> numerical = new HashMap<>();
    HashMap<String, Integer> categorical = new HashMap<>();
    for (TemporalElement element : reservoirSample) {
      if (element.getPropertyKeys() == null) {
        continue;
      }
      for (String key : element.getPropertyKeys()) {
        PropertyValue value = element.getPropertyValue(key);
        if (isNumerical(value)) {
          numerical.put(key, numerical.getOrDefault(key, 0) + 1);
        } else {
          categorical.put(key, categorical.getOrDefault(key, 0) + 1);
        }
      }
    }
    // check which property is of what type using a threshold
    double thresh = 0.9;
    HashSet<String> allProperties = new HashSet<>();
    allProperties.addAll(numerical.keySet());
    allProperties.addAll(categorical.keySet());
    for (String property : allProperties) {
      int countNumerical = numerical.getOrDefault(property, 0);
      int countCategorical = categorical.getOrDefault(property, 0);
      int countOverall = countCategorical  +  countNumerical;
      // which type?
      if ((double) countNumerical / (double) countOverall >= thresh) {
        numericalProperties.add(property);
      } else if ((double) countCategorical / (double) countOverall >= thresh) {
        categoricalProperties.add(property);
      }
      // else, the property is too ambiguous and simply ignored
    }
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
          selectivityMap.getOrDefault(value, 0.)  +  1. / sampleSize);
      }

      categoricalSelectivityEstimation.put(entry.getKey(), selectivityMap);
    }

  }

  /**
   * Initializes or updates the estimations for numerical properties
   * All of them are considered to be normally distributed
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

      // estimate how often the property occurs
      numericalOccurrenceEstimation.put(entry.getKey(),
        (double) values.size() / sampleSize);

      // cast all values to doubles
      Class cls = values.get(0).getType();
      List<Double> doubleValues = values.stream()
        .map(val -> propertyValueToDouble(val, cls))
        .collect(Collectors.toList());

      // compute mean and variance for the property
      Double sum = doubleValues.stream().reduce(0., Double::sum);
      Double mean = sum / values.size();
      Double variance = doubleValues.stream().reduce(0.,
        (i, j) -> i  +  ((j - mean) * (j - mean) * (1. / values.size())));

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


  /**
   * Returns the (not binning-based) statistics for temporal properties.
   * They are arrays of the form {mean, variance, prob. that from=Long.MIN_VALUE / to=Long.MAX_VALUE}
   * @param property temporal property
   * @return statistics array for the property
   */
  public double[] getTemporalPropertyStats(TimeSelector.TimeField property) {
    if (recomputeTemporalDataFlag) {
      computeTemporalEstimations();
    }
    if (property == TimeSelector.TimeField.TX_FROM) {
      return txFromStats.clone();
    } else if (property == TimeSelector.TimeField.TX_TO) {
      return txToStats.clone();
    } else if (property == TimeSelector.TimeField.VAL_FROM) {
      return valFromStats.clone();
    } else {
      return valToStats.clone();
    }
  }
}
