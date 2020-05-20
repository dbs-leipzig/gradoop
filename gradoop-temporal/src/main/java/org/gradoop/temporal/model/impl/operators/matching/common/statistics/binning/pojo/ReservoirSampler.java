package org.gradoop.temporal.model.impl.operators.matching.common.statistics.binning.pojo;

import org.gradoop.temporal.model.impl.pojo.TemporalElement;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Implements Reservoir Sampling. Fulfills Flink POJO conditions
 */
public class ReservoirSampler<T> implements Serializable {

    /**
     * The number of vertices to include in the reservoir sample
     */
    private int reservoirSampleSize;

    /**
     * Default value for reservoir sample size
     */
    public static final int DEFAULT_SAMPLE_SIZE = 5000;

    /**
     * Holds the reservoir sampled vertices
     */
    private ArrayList<T> reservoirSample;

    /**
     * Random for use in reservoir sampling
     */
    private Random random;

    /**
     * Number of all elements already seen
     */
    private long count;

    /**
     * Creates a new reservoir sampler
     * @param reservoirSampleSize maximum size of the sample
     */
    public ReservoirSampler(int reservoirSampleSize){
        this.reservoirSampleSize = reservoirSampleSize;
        reservoirSample = new ArrayList<>();
        random = new Random();
        count = 0;
    }

    /**
     * Creates a new reservoir sample with default sample size
     */
    public ReservoirSampler(){
        this(DEFAULT_SAMPLE_SIZE);
    }

    /**
     * Updates the sample with a given element, i.e. might add this element
     * @param element element that might be included in the sample
     * @return true iff element was actually included in the sample
     */
    public boolean updateSample(T element){
        count++;
        if(count <= reservoirSampleSize){
            reservoirSample.add(element);
            return true;
        }
        else{
            double randomDouble = random.nextDouble();
            double ratio = (double) reservoirSampleSize / (double) count;
            // include vertex?
            if(randomDouble <= ratio){
                int randomIndex = random.nextInt(reservoirSampleSize);
                reservoirSample.set(randomIndex, element);
                return true;
            }
        }
        return false;
    }

    /**
     * Updates the sample with a given element list, i.e. might add elements from this list
     * @param elements elements that might be included in the sample
     * @return true iff at least one element was actually included in the sample
     */
    public boolean updateSample(List<T> elements){
        boolean updated = false;
        for(T element : elements){
            updated = updateSample(element) || updated;
        }
        return updated;
    }

    /**
     * Returns the reservoir sample
     * @return reservoir sample
     */
    public List<T> getReservoirSample(){
        return reservoirSample;
    }

    /**
     * Returns the size of the sample
     * @return size of the sample
     */
    public int getSampleSize(){
        return reservoirSample.size();
    }


    @Override
    public int hashCode(){
        if(reservoirSample == null){
            return 0;
        }
        return reservoirSample.hashCode();
    }
}
