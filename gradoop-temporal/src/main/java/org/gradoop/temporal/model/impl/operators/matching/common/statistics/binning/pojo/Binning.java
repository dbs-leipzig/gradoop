package org.gradoop.temporal.model.impl.operators.matching.common.statistics.binning.pojo;

import java.io.Serializable;
import java.util.*;

/**
 * Implements equal frequency binning of long values.
 */
public class Binning implements Serializable {

    /**
     * represents the bins. Every bin is defined by its lowest value
     */
    private Long[] bins = new Long[]{};

    /**
     * Default number of bins
     */
    public static final int DEFAULT_NUM_OF_BINS = 100;

    /**
     * Creates new bins from values. Furthermore, the number of bins to create is given.
     * @param values values to create bins from
     * @param numberOfBins number of bins to create
     */
    public Binning(List<Long> values, int numberOfBins){
        createBins(values, numberOfBins);
    }

    /**
     * Creates new bins from values. Number of bins is default.
     * @param values values to create bins from
     */
    public Binning(List<Long> values){
        this(values, DEFAULT_NUM_OF_BINS);
    }

    /**
     * Creates the bins from values. The value list is sorted and divided into
     * equal sized bins. The first bin gets lowest value {@code Long.MIN_VALUE},
     * every other bin's lowest value is computed as
     *  {@code max(previous bin) + (min(current bin) - max(previous bin))/2}
     *
     * @param values values to create bins from. Its size must be a multiple of the specified
     *               number of bins!
     * @param numberOfBins number of bins to create
     */
    private void createBins(List<Long> values, int numberOfBins) throws IllegalArgumentException{
        if(values.size() % numberOfBins != 0){
            throw new IllegalArgumentException("Number of values must be a multiple " +
                    "of the number of bins!");
        }

        values.sort((aLong, t1) -> Long.compare(aLong, t1));

        if(values.size() < numberOfBins){
            numberOfBins = values.size();
        }

        int binSize = (int) Math.ceil((double)values.size()/(double) numberOfBins);


        bins = new Long[numberOfBins];
        bins[0] = Long.MIN_VALUE;

        for(int i=1; i<numberOfBins; i++){
            long maxPreviousBin = values.get((i*binSize)-1);
            long minCurrentBin = values.get(i*binSize);
            bins[i] = maxPreviousBin + (minCurrentBin - maxPreviousBin)/2;
        }


    }

    /**
     * Returns the bins
     * @return bins
     */
    public Long[] getBins(){
        return bins;
    }


    public void setBins(Long[] bins) {
        this.bins = bins;
    }

    @Override
    public int hashCode(){
        return Arrays.hashCode(bins);
    }
}
