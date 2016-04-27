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

        SparkConf sparkConf = new SparkConf().setAppName("TextAnalyzer").setJars(new String[] {"/root/TextAnalyzer.jar"});
        sparkConf.setMaster("spark://ec2-54-86-243-145.compute-1.amazonaws.com:7077");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        JavaRDD<String> lines = ctx.textFile(args[0], 1);

        JavaRDD<Tuple2<String, String>> wordPairs = lines.flatMap(new FlatMapFunction<String, Tuple2<String, String>>() {
            @Override
            public Iterable<Tuple2<String, String>> call(String line) {
                line = line.toLowerCase().replaceAll("\\W", " ").replaceAll("_", " ");
                String[] tokens = line.split("\\s+");

                Set<String> seen = new HashSet<String>();
                ArrayList<Tuple2<String, String>> ret = new ArrayList<Tuple2<String, String>>();

                for (int i = 0; i < tokens.length; i += 1) {
                    String contextWord = tokens[i];
                    if (contextWord.length() == 0) continue;
                    if (seen.contains(contextWord)) continue;
                    seen.add(contextWord);

                    for (int j = 0; j < tokens.length; j += 1) {
                        String otherWord = tokens[j];
                        if (i == j) continue;
                        if (otherWord.length() == 0) continue;
                        ret.add(new Tuple2<String, String>(contextWord, otherWord));
                    }
                }

                return ret;
            }
        });

        JavaPairRDD<String, Tuple2<String, Integer>> ones = wordPairs.mapToPair(new PairFunction<Tuple2<String, String>, String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Tuple2<String, Integer>> call(Tuple2<String, String> tup) {
                return new Tuple2<String, Tuple2<String, Integer>>(tup._1(), new Tuple2<String, Integer>(tup._2(), 1));
            }
        });

        JavaPairRDD<String, ArrayList<Tuple2<String, Integer>>> list = ones.combineByKey(new Function<Tuple2<String, Integer>, ArrayList<Tuple2<String, Integer>>>() {
            @Override
            public ArrayList<Tuple2<String, Integer>> call(Tuple2<String, Integer> v) {
                ArrayList<Tuple2<String, Integer>> l = new ArrayList<Tuple2<String, Integer>>();
                l.add(v);
                return l;
            }
        }, new Function2<ArrayList<Tuple2<String, Integer>>, Tuple2<String, Integer>, ArrayList<Tuple2<String, Integer>>>() {
            @Override
            public ArrayList<Tuple2<String, Integer>> call(ArrayList<Tuple2<String, Integer>> c, Tuple2<String, Integer> v) {
                for (int i = 0; i < c.size(); i += 1) {
                    Tuple2<String, Integer> tup = c.get(i);
                    if (tup._1().equals(v._1())) {
                        c.set(i, new Tuple2<String, Integer>(tup._1(), tup._2()+v._2()));
                        return c;
                    }
                }

                c.add(v);
                return c;
            }
        }, new Function2<ArrayList<Tuple2<String, Integer>>, ArrayList<Tuple2<String, Integer>>, ArrayList<Tuple2<String, Integer>>>() {
            @Override
            public ArrayList<Tuple2<String, Integer>> call(ArrayList<Tuple2<String, Integer>> c1, ArrayList<Tuple2<String, Integer>> c2) {
                c1.addAll(c2);
                return c1;
            }
        });

        JavaPairRDD<String, ArrayList<Tuple2<String, Integer>>> counts = list.reduceByKey(new Function2<ArrayList<Tuple2<String, Integer>>, ArrayList<Tuple2<String, Integer>>, ArrayList<Tuple2<String, Integer>>>() {
            @Override
            public ArrayList<Tuple2<String, Integer>> call(ArrayList<Tuple2<String, Integer>> list1, ArrayList<Tuple2<String, Integer>> list2) {
                System.out.println("list1, list2");
                System.out.println(list1.toString() + " / " + list2.toString());

                Map<String, Integer> m = new TreeMap<String, Integer>();
                for (Tuple2<String, Integer> tup : list1) m.put(tup._1(), 0);
                for (Tuple2<String, Integer> tup : list2) m.put(tup._1(), 0);
                for (Tuple2<String, Integer> tup : list1) m.put(tup._1(), m.get(tup._1()) + tup._2());
                for (Tuple2<String, Integer> tup : list2) m.put(tup._1(), m.get(tup._1()) + tup._2());
                ArrayList<Tuple2<String, Integer>> ret = new ArrayList<Tuple2<String, Integer>>();
                for (Map.Entry<String, Integer> entry : m.entrySet()) {
                    ret.add(new Tuple2<String, Integer>(entry.getKey(), entry.getValue()));
                }
                return ret;
            }
        });

        List<Tuple2<String, ArrayList<Tuple2<String, Integer>>>> output = counts.sortByKey().collect();
        for (Tuple2<String, ArrayList<Tuple2<String, Integer>>> outerTuple : output) {
            String contextWord = outerTuple._1();
            ArrayList<Tuple2<String, Integer>> pairs = outerTuple._2();
            System.out.println(contextWord);

            for (Tuple2<String, Integer> tup : pairs) {
                String otherWord = tup._1();
                Integer num = tup._2();
                System.out.println("<" + otherWord + ", " + num.toString() + ">");
            }
        }

        ctx.stop();
    }
}
