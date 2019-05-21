package org.gradoop.flink.model.impl.operators.layouting;

public class FRLayouterNaiveTest extends LayoutingAlgorithmTest {

    @Override
    public LayoutingAlgorithm getLayouter(int w, int h) {
        return new FRLayouterNaive(FRLayouter.calculateK(w, h, 10), 5, w, h);
    }

}
