package org.gradoop.flink.model.impl.operators.layouting;

public class RandomLayouterTest extends LayoutingAlgorithmTest {

    @Override
    public LayoutingAlgorithm getLayouter(int w, int h) {
        return new RandomLayouter(0,w,0,h);
    }

}
