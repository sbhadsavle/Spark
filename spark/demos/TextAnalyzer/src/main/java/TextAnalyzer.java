/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import scala.Tuple2;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.regex.Pattern;

public final class TextAnalyzer {
  public static void main(String[] args) throws Exception {
    if (args.length < 1) {
      System.err.println("Usage: TextAnalyzer <file>");
      System.exit(1);
    }

    SparkConf sparkConf = new SparkConf().setAppName("TextAnalyzer").setJars(new String[]{"/root/TextAnalyzer.jar"});
    sparkConf.setMaster("spark://ec2-54-86-243-145.compute-1.amazonaws.com:7077");
    JavaSparkContext ctx = new JavaSparkContext(sparkConf);
    JavaRDD<String> lines = ctx.textFile(args[0], 1);

    JavaPairRDD<String, Tuple2<String, Integer>> aggregated = lines.mapToPair(new PairFunction<String, String, Tuple2<String, Integer>>() {
      @Override
      public Tuple2<String, Tuple2<String, Integer>> call(String s) {
        String line = s.toLowerCase().replaceAll("\\W", " ").replaceAll("_", " ");
        String[] tokens = line.split("\\s+");
        Set<String> seen = new HashSet<String>();

        for (int i = 0; i < tokens.length; i += 1) {
          String contextWord = tokens[i];
          if (contextWord.length() == 0) continue;
          if (seen.contains(contextWord)) continue;
          seen.add(contextWord);

          Map<String, Integer> m = new HashMap<String, Integer>();
          for (int j = 0; j < tokens.length; j += 1) {
            String otherWord = tokens[j];
            if (i == j) continue;
            if (otherWord.length() == 0) continue;
            if (!m.containsKey(otherWord)) {
              m.put(otherWord, 1);
              continue;
            }
            m.put(otherWord, m.get(otherWord)+1);
          }

          for (Map.Entry<String, Integer> entry : m.entrySet()) {
            String k = entry.getKey();
            Integer v = entry.getValue();
            return new Tuple2<String, Tuple2<String, Integer>>(contextWord, new Tuple2<String, Integer>(k, v));
          }
        }
      }
    });

    // public <C> JavaPairRDD<K,C> combineByKey(Function<V,C> createCombiner,
    //                                 Function2<C,V,C> mergeValue,
    //                                 Function2<C,C,C> mergeCombiners,
    //                                 Partitioner partitioner)
    // Generic function to combine the elements for each key using a custom set of aggregation functions. Turns a JavaPairRDD[(K, V)] into a result of type JavaPairRDD[(K, C)], for a "combined type" C * Note that V and C can be different -- for example, one might group an RDD of type (Int, Int) into an RDD of type (Int, List[Int]). Users provide three functions:
    // - createCombiner, which turns a V into a C (e.g., creates a one-element list) - mergeValue, to merge a V into a C (e.g., adds it to the end of a list) - mergeCombiners, to combine two C's into a single one.

    // In addition, users can control the partitioning of the output RDD, and whether to perform map-side aggregation (if a mapper can produce multiple items with the same key).

    JavaPairRDD<String, List<Tuple2<String, Integer>>> listed = aggregated.combineByKey(new Function<Tuple2<String, Integer>, List<Tuple2<String, Integer>>>() {
      @Override
      public List<Tuple2<String, Integer>> call(Tuple2<String, Integer> tup) {
        return Arrays.asList(tup);
      }
    }, new Function2<List<Tuple2<String, Integer>>, Tuple2<String, Integer>, List<Tuple2<String, Integer>>>() {
      @Override
      public List<Tuple2<String, Integer>> call(List<Tuple2<String, Integer>> c, Tuple2<String, Integer> v) {
        c.add(v);
        return c;
      }
    }, new Function2<List<Tuple2<String, Integer>>, List<Tuple2<String, Integer>>, List<Tuple2<String, Integer>>>() {
      @Override
      public List<Tuple2<String, Integer>> call(List<Tuple2<String, Integer>> c1, List<Tuple2<String, Integer>> c2) {
        c1.addAll(c2);
        return c1;
      }
    });

    JavaPairRDD<String, List<Tuple2<String, Integer>>> counts = listed.reduceByKey(new Function2<List<Tuple2<String, Integer>>, List<Tuple2<String, Integer>>, List<Tuple2<String, Integer>>>() {
      @Override
      public List<Tuple2<String, Integer>> call(List<Tuple2<String, Integer>> list1, List<Tuple2<String, Integer>> list2) {
        Map<String, Integer> m = new HashMap<String, Integer>();
        for (Tuple2<String, Integer> tup : list1) m.put(tup._1(), 0);
        for (Tuple2<String, Integer> tup : list2) m.put(tup._1(), 0);
        for (Tuple2<String, Integer> tup : list1) m.put(tup._1(), m.get(tup._1()) + tup._2());
        for (Tuple2<String, Integer> tup : list2) m.put(tup._1(), m.get(tup._1()) + tup._2());
        List<Tuple2<String, Integer>> ret = new ArrayList<Tuple2<String, Integer>>();
        for (Map.Entry<String, Integer> entry : m.entrySet()) {
          ret.add(new Tuple2<String, Integer>(entry.getKey(), entry.getValue()));
        }
        return ret;
      }
    });

    List<Tuple2<String, List<Tuple2<String, Integer>>>> output = counts.collect();
    for (Tuple2<String, List<Tuple2<String, Integer>>> outerTuple : output) {
      String contextWord = outerTuple._1();
      List<Tuple2<String, Integer>> pairs = outerTuple._2();
      System.out.println(contextWord);

      for (Tuple2<String, Integer> tup : pairs) {
        String otherWord = tup._1();
        Integer num = tup._2();
        System.out.println("<" + otherWord + ", " + Integer.toString(num) + ">");
      }
    }

    ctx.stop();
  }
}
