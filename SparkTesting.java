package com.app.spark;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;

class GetPow implements Function<Integer, Integer> {
	  public Integer call(Integer s) {
		  Integer res;
		  res = (int) Math.pow(s, 2);
		  return res; 
	  }
}

class GetFilter implements Function<Integer, Boolean> {
	  public Boolean call(Integer s) {
		  Boolean res = null;
		  if(s > 2) {
			  res = true;
		  } else {
			  res = false;
		  }
		  return res; 
	  }
}

class GetEachNumberPow implements FlatMapFunction<Integer, Integer> {
	  public Iterator<Integer> call(Integer s) {
		  return Arrays.asList(s*3, s*2, s*4).iterator();
	  }
}

class GetEachPair implements PairFunction<Integer, String, Integer> {
	  public Tuple2<String, Integer> call(Integer s) {
		  if(s < 3) {
			  return new Tuple2<String, Integer>("kurang", s);
		  }else {
			  return new Tuple2<String, Integer>("lebih", s);
		  }
	  }
}

class RedPow implements Function2<Integer, Integer, Integer>{
	public Integer call(Integer s1, Integer s2) {
		return s1+s2;
	}
}

public class SparkTesting {
	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("SparkAsik").setMaster("local");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
		List<String> stringData = Arrays.asList("Hello", "World", "how", "are", "you");
		JavaRDD<Integer> distData = sc.parallelize(data);
		JavaRDD<String> distString = sc.parallelize(stringData);
		JavaPairRDD<String,Integer> petsData = JavaPairRDD.fromJavaRDD(sc.parallelize(
				Arrays.asList(
					new Tuple2<String,Integer>("cat", 1),
					new Tuple2<String,Integer>("dog", 5),
					new Tuple2<String,Integer>("cat", 3)
				)
			)
		);
		
		JavaRDD<String> distText = sc.textFile("G:\\Bahan ngajar\\Read.txt");
		
		JavaRDD<Integer> dataPow = distData.map(new GetPow());
		JavaRDD<Integer> dataFilter = dataPow.filter(new GetFilter());
		JavaPairRDD<String, Integer> pairs = distData.mapToPair(new GetEachPair());
		JavaRDD<Integer> dataFlat = distData.flatMap(new GetEachNumberPow());
		JavaRDD<Integer> dist = distData.distinct(5);
		JavaPairRDD<String, Integer> petsAsKey = petsData.reduceByKey(new RedPow());
		JavaPairRDD<String, Iterable<Integer>> petsGroup = petsData.groupByKey();
		
		long res1 = dataPow.reduce(new RedPow());
		long res0 = dataPow.count();
		List<Integer> result = dataPow.collect();
		List<Tuple2<String, Integer>> res6 = pairs.collect();
		List<Tuple2<String, Integer>> res8 = petsAsKey.collect();
		List<Tuple2<String, Iterable<Integer>>> res9 = petsGroup.collect();
		List<Tuple2<String, Integer>> res10 = petsData.collect();
		List<Integer> res2 = dataFilter.collect();
		List<Integer> res3 = dataFlat.collect();
		List<Integer> res4 = dist.collect();
		List<String> res5 = distText.collect();
		String res7 = pairs.collect().toString();
		
		System.out.println(res0);
		System.out.println(res1);
		System.out.println(result);
		System.out.println(res2);
		System.out.println(res3);
		System.out.println(res4);
		System.out.println(res5);
		System.out.println(res6);
		System.out.println(res7);
		System.out.println(res6.getClass().getName());
		System.out.println(res7.getClass().getName());
		System.out.println(res8);
		System.out.println(res9);
		System.out.println(res10);
		
		sc.close();
	}
}
