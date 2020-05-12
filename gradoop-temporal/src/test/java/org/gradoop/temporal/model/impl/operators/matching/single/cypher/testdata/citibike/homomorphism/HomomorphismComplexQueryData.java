package org.gradoop.temporal.model.impl.operators.matching.single.cypher.testdata.citibike.homomorphism;

import org.gradoop.temporal.model.impl.operators.matching.TemporalTestData;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.CBCypherTemporalPatternMatchingTest;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Scanner;


import static org.gradoop.temporal.model.impl.operators.matching.single.cypher.testdata.citibike.util.Util.getDataFromFile;

public class HomomorphismComplexQueryData implements TemporalTestData {
    @Override
    public Collection<String[]> getData() {
        return getDataFromFile(
                "src/test/resources/data/patternmatchingtest/complex/complex_queries_homomorphism");
    }


}
