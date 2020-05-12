package org.gradoop.temporal.model.impl.operators.matching.single.cypher.testdata.citibike.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Generates random complex test cases from simple ones.
 */
public class RandomTestGenerator {

    /**
     * Contains manually checked simple test cases to build complex ones from
     */
    ArrayList<String[]> basicTestCases;

    /**
     * Contains all frequent graph query patterns in {@code basicTestCases}.
     * Every pattern is paired with an integer representing a cumulative sum.
     * E.g. ((a), 3), ((a)-[e]->(b), 7) would mean that (a) occurs 3 times and
     * ((a)-[e]->(b)) 4 times. This is used to sample a random pattern according to
     * the pattern distribution.
     */
    HashMap<String, Integer> patternDistribution;

    /**
     * Number of queries in {@code basicTestCases} that are considered for building more
     * complex queries.
     */
    int numberOfRelevantQueries;

    /**
     * Random queries are built as binary trees. This indicates the minimum depth of such a tree
     */
    static final int MIN_DEPTH = 1;
    /**
     * Random queries are built as binary trees. This indicates the maximum depth of such a tree
     */
    static final int MAX_DEPTH = 2;

    /**
     * String representations of all operators used to build complex queries
     */
    static final String[] OPERATORS = new String[]{"AND", "OR", "XOR"};

    /**
     * Path of the log file
     */
    private String logFilePath;

    /**
     * Path to default log file
     */
    static final String defaultLogFilePath =
            "src/test/resources/data/patternmatchingtest/complex/complexTest.txt";


    /**
     * Builds a new test generator
     * @param basicTestCases manually checked simple test cases to build more complex ones from
     * @param logFilePath path to the log file to use
     */
    public RandomTestGenerator(ArrayList<String[]> basicTestCases, String logFilePath){
        this.basicTestCases = basicTestCases;
        this.logFilePath = logFilePath;
        System.out.println(basicTestCases.size());
        numberOfRelevantQueries = initPatternDistribution(3);
        System.out.println(numberOfRelevantQueries);
    }

    /**
     * Builds a new test generator using the default log file
     * @param basicTestCases manually checked simple test cases to build more complex ones from
     */
    public RandomTestGenerator(ArrayList<String[]> basicTestCases){
        this(basicTestCases, defaultLogFilePath);
    }

    /**
     * Initializes {@code patternDistribution}. Only considers patterns that occur frequently
     * @param threshold frequency threshold. Pattern is only considered, if it occurs at least that
     *                  often in {@code basicTestCases}
     * @return patternDistribution
     */
    private int initPatternDistribution(int threshold){
        ArrayList<String> patterns = new ArrayList<>();
        patternDistribution = new HashMap<>();
        for(String[] testCase: basicTestCases){
            patterns.add(extractPatternFromQuery(testCase[2]));
        }
        int counter = 0;
        for(String pattern: new HashSet<>(patterns)){
            int frequency = Collections.frequency(patterns, pattern);
            if(frequency < threshold){
                continue;
            }
            counter+= frequency;
            patternDistribution.put(pattern, counter);
        }
        System.out.println(patternDistribution);
        return counter+1;
    }

    /**
     * Extracts the graph pattern from a given query
     * @param query the query to extract the graph pattern from
     * @return graph pattern of the query
     */
    private String extractPatternFromQuery(String query){
        return query.substring(query.indexOf("MATCH")+5, query.indexOf("WHERE")).trim();
    }

    /**
     * Samples a pattern from {@code patternDistribution}
     * @return random pattern
     */
    private String randomPattern(){
        int random = ThreadLocalRandom.current().nextInt(0, numberOfRelevantQueries);

        int minCumulativeSum = Integer.MAX_VALUE;
        String pattern = "";
        // find first pattern with cumulative sum < random
        for(String p: patternDistribution.keySet()){
            if(patternDistribution.get(p) >= random && patternDistribution.get(p)< minCumulativeSum){
                minCumulativeSum = patternDistribution.get(p);
                pattern = p;
                return p;
            }
        }
        return pattern;
    }

    /**
     * Samples a random depth {@code d} with {@code MIN_DEPTH <= d <= MAX_DEPTH}
     * @return random depth
     */
    private int randomDepth(){
        return ThreadLocalRandom.current().nextInt(MIN_DEPTH, MAX_DEPTH+1);
    }

    /**
     * Creates random test cases
     * @param n number of test cases to create
     * @return {@code n} random test cases
     * @throws IOException if logging goes wrong
     */
    public ArrayList<String[]> createRandomTestCases(int n) throws IOException {
        if(!new File(logFilePath).exists()){
            new File(logFilePath).createNewFile();
        }
        BufferedWriter writer = new BufferedWriter(new FileWriter(logFilePath));
        ArrayList<String[]> testCases = new ArrayList<>();
        for(int i= 0; i<n; i++){
            String[] testCase = randomQuery();
            log(testCase, writer);
            testCases.add(testCase);
        }
        writer.close();
        return testCases;
    }

    /**
     * Writes a test case to the log file
     * @param testCase the test case to log
     * @param writer the writer to use
     * @throws IOException if logging goes wrong
     */
    private void log(String[] testCase, BufferedWriter writer) throws IOException{
        for(String s: testCase){
            writer.write(s+"\n");
        }
        writer.write("\n");
    }

    /**
     * Create a random query (random pattern, random depth)
     * @return random query
     */
    private String[] randomQuery(){
        String pattern = randomPattern();
        int depth = randomDepth();
        System.out.println("Depth: "+depth);
        return buildRandomQuery(pattern, depth);
    }

    /**
     * Create a random query for a given pattern and depth
     * @param pattern graph pattern of the random query
     * @param depth depth of the random query
     * @return random query with given pattern and depth
     */
    private String[] buildRandomQuery(String pattern, int depth){
        if(depth<1){
            throw new IllegalArgumentException("depth must be positive");
        }
        String[] t1 = null;
        String[] t2 = null;
        if(depth==1){
            t1 = getRandomTestWithPattern(pattern);
            t2 = getRandomTestWithPattern(pattern);

        }
        else{
            t1 = buildRandomQuery(pattern, depth-1);
            t2 = buildRandomQuery(pattern, depth-1);
        }

        String operator = getRandomOperator();
        return buildQuery(t1, t2, operator);
    }

    /**
     * Sample a random operator from {@code OPERATORS}
     * @return random operator string representation
     */
    private String getRandomOperator() {
        int random = ThreadLocalRandom.current().nextInt(0, OPERATORS.length);
        return OPERATORS[random];
    }

    /**
     * Sample a basic test case that has a given graph pattern
     * @param pattern graph pattern
     * @return basic test case with given graph pattern
     */
    private String[] getRandomTestWithPattern(String pattern) {
        ArrayList<String[]> candidates = new ArrayList<>();
        for(String[] testCase: basicTestCases){
            String candidatePattern= extractPatternFromQuery(testCase[2]);
            if(matchesPattern(candidatePattern, pattern)){
                candidates.add(testCase);
            }
        }
        System.out.println("Candidates for "+pattern+": "+candidates);
        int random = ThreadLocalRandom.current().nextInt(0, candidates.size());
        return candidates.get(random);
    }

    /**
     * Given two basic test cases and an operator, build a new test case from them
     * @param test1 first basic test case
     * @param test2 second basic test case
     * @param operator operator used to build a new test case from the given two
     * @return complex test case
     */
    private String[] buildQuery(String[] test1, String[] test2, String operator){
        if(!(operator.equals("XOR") || operator.equals("AND") || operator.equals("OR"))){
            throw new IllegalArgumentException("not a valid operator");
        }

        String[] newTest = new String[5];
        newTest[0] = newId(test1[0], test2[0], operator);
        newTest[1] = test1[1];

        String pattern = extractPatternFromQuery(test1[2]);
        String where = newWhere(extractWhereFromQuery(test1[2]), extractWhereFromQuery(test2[2]),
                operator);
        newTest[2] = buildMatch(pattern, where);

        String[] results = null;
        if(operator.equals("XOR")){
            results = xorResults(test1[4], test2[4]);
        }
        else if(operator.equals("AND")){
            results = mergeResults(test1[4], test2[4]);
        }
        else{
            results = joinResults(test1[4], test2[4]);
        }
        newTest[3] = buildExpectedList(results.length);
        String resultGraphs = buildResultGraphs(results);
        newTest[4] = resultGraphs;

        return newTest;
    }

    private String buildResultGraphs(String[] results) {
        for(int i=0; i<results.length; i++){
            results[i]= "expected"+(String.valueOf(i+1))+"["+results[i]+"]";
        }
        return String.join(", ", results);
    }

    /**
     * Create a query expression given a pattern and a where clause
     * @param pattern graph pattern of the query
     * @param where where clause of the query
     * @return query
     */
    private String buildMatch(String pattern, String where) {
        return "MATCH "+pattern+" WHERE "+where;
    }

    /**
     * Extracts where clause from a query expression
     * @param queryExpression the expression to extract the where clause from
     * @return where clause
     */
    private String extractWhereFromQuery(String queryExpression) {
        return queryExpression.substring(queryExpression.indexOf("WHERE")+"WHERE".length()).trim();
    }

    /**
     * builds a new test case id given ids of two basic test cases and an operator
     * @param id1 id of first basic test case
     * @param id2 id of second basic test case
     * @param operator operator
     * @return new test case id
     */
    private String newId(String id1, String id2, String operator){
        return operator+"("+id1+", "+id2+")";
    }

    /**
     * builds a new where clause from two where clauses and an operator
     * @param oldWhere1 first where clause
     * @param oldWhere2 second where clause
     * @param operator operator
     * @return new where clause
     */
    private String newWhere(String oldWhere1, String oldWhere2, String operator){
        return "(("+oldWhere1+") "+ operator+" ("+oldWhere2+"))";
    }

    /**
     * checks, whether two patterns match
     * @param pattern1 first pattern
     * @param pattern2 second pattern
     * @return true iff patterns match
     */
    private boolean matchesPattern(String pattern1, String pattern2){
        return pattern1.trim().equals(pattern2.trim());
    }

    /**
     * Builds the expression that enumerates the names of all expected results (3rd argument
     * in a test case), e.g.
     * {@code expected1,expected2,expected3}
     * @param n number of expected graph results
     * @return expression that enumerates the names of all expected results
     */
    private String buildExpectedList(int n){
        String[] exp = new String[n];
        for(int i= 0; i<n; i++){
            exp[i]= "expected"+String.valueOf(i+1);
        }
        return String.join(",", exp);
    }

    /**
     * Extracts result patterns from a list of results (4th argument in a test case)
     * @param testResult 4th argument of a test case to extract patterns from
     * @return extracted result patterns
     */
    private String[] extractResultPatterns(String testResult){
        System.out.println("Extracting result patterns from "+testResult);
        if(testResult.isEmpty()){
            return new String[]{};
        }
        String[] results = testResult.split(",");
        for(int i= 0; i<results.length; i++){
            results[i]= results[i].substring(
                    results[i].indexOf("[")+1,
                    results[i].lastIndexOf("]"))
            .trim();
        }
        return results;
    }

    /**
     * Joins two result sets
     * @param testResults1 first result set (4th argument of a test case)
     * @param testResults2 second result set (4th argument of a test case)
     * @return joined result set
     */
    private String[] joinResults(String testResults1, String testResults2){
        String[] exp1 = extractResultPatterns(testResults1);
        String[] exp2  = extractResultPatterns(testResults2);
        if(testResults1.isEmpty()){
            return exp2;
        }
        if(testResults2.isEmpty()){
            return exp1;
        }


        ArrayList<String> expected = new ArrayList<>(Arrays.asList(exp1));
        for(String r : exp2){
            if(expected.contains(r.trim())){
                continue;
            }
            else{
                expected.add(r.trim());
            }
        }

        String[] ret = new String[expected.size()];
        for(int i=0; i<ret.length; i++){
            ret[i] = expected.get(i);
        }
        return ret;
    }

    /**
     * Merges two result sets
     * @param testResults1 first result set (4th argument of a test case)
     * @param testResults2 second result set (4th argument of a test case)
     * @return merged result set
     */
    private String[] mergeResults(String testResults1, String testResults2){
        if(testResults1.isEmpty() || testResults2.isEmpty()){
            return new String[]{};
        }

        String[] exp1 = extractResultPatterns(testResults1);
        String[] exp2  = extractResultPatterns(testResults2);

        ArrayList<String> exp1List = new ArrayList<>(Arrays.asList(exp1));

        ArrayList<String> expected = new ArrayList<>();
        for(String r : exp2){
            if(exp1List.contains(r.trim())){
                expected.add(r);
            }
        }

        String[] ret = new String[expected.size()];
        for(int i=0; i<ret.length; i++){
            ret[i] = expected.get(i);
        }
        return ret;
    }

    /**
     * Xor of two result sets
     * @param testResults1 first result set (4th argument of a test case)
     * @param testResults2 second result set (4th argument of a test case)
     * @return xor result set
     */
    private String[] xorResults(String testResults1, String testResults2){
        String[] exp1 = extractResultPatterns(testResults1);
        String[] exp2  = extractResultPatterns(testResults2);
        if(testResults1.isEmpty()){
            return exp2;
        }
        if(testResults2.isEmpty()){
            return exp1;
        }


        ArrayList<String> intersection = new ArrayList<>(Arrays.asList(mergeResults(testResults1, testResults2)));

        ArrayList<String> expected = new ArrayList<>();
        for(String r : exp1){
            if(!intersection.contains(r.trim())){
                expected.add(r);
            }
        }
        for(String r: exp2){
            if(!intersection.contains(r.trim())){
                expected.add(r);
            }
        }

        String[] ret = new String[expected.size()];
        for(int i=0; i<ret.length; i++){
            ret[i] = expected.get(i);
        }
        return ret;
    }
}
