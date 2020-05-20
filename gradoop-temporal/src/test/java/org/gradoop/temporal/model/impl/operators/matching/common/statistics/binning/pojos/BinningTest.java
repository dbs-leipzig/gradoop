package org.gradoop.temporal.model.impl.operators.matching.common.statistics.binning.pojos;

import org.gradoop.temporal.model.impl.operators.matching.common.statistics.binning.pojo.Binning;
import org.junit.Test;

import java.util.ArrayList;

import static org.junit.Assert.assertEquals;

public class BinningTest {

    @Test
    public void simpleBinsTest(){
        ArrayList<Long> input = new ArrayList<>();
        for(int i=0; i<300; i++){
            input.add((long) i);
        }

        Binning binning = new Binning(input, 100);
        Long[] bins = binning.getBins();
        assertEquals(bins.length, 100);
        assertEquals((long)bins[0], Long.MIN_VALUE);
        for(int i=1; i<100; i++){
            assertEquals((long)bins[i], 3*i-1);
        }

    }

    @Test(expected = IllegalArgumentException.class)
    public void illegalArgumentTest(){
        ArrayList<Long> input = new ArrayList<>();
        for(int i=0; i<142; i++){
            input.add((long) i);
        }
        Binning binning = new Binning(input, 100);
    }
}
