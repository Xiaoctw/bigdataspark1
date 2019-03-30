package com.company;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.io.Serializable;
import java.util.*;

public class Main {
    private static double val1;
    private static double val2;
    private static double maxIncome,minIncome,maxLatitude,minLatitude,maxLongitude,minLongitude;
    static transient SparkConf conf=new SparkConf().setMaster("local").setAppName("数据筛选");
    static transient JavaSparkContext context=new JavaSparkContext(conf);
    private static double mean_value=2860;

    public static void main(String[] args) throws FileNotFoundException {
	// write your code here
        JavaRDD<String> lines=context.textFile("/home/xiao/文档/大数据分析/数据/large_data.txt");
      //  long count1=lines.count();
        //首先获得所有样本,抽取到的
        JavaPairRDD<Character,String> stringJavaPairRDD=lines.mapToPair((PairFunction<String, Character, String>) s -> {
            String[] strings=s.split("\\|");
            char user_career=strings[10].charAt(0);
            return new Tuple2<>(user_career,s);
        });
       // long count2=stringJavaPairRDD.count();
        JavaPairRDD<Character,Iterable<String>> stringIter = stringJavaPairRDD.groupByKey();
        JavaRDD<Iterable<String>> values=stringIter.values();
      //  long count3=values.count();
        JavaRDD<Iterable<String>> samples1=values.map((Function<Iterable<String>, Iterable<String>>) strings -> {//获得所有的样本数据
            List<String> list=new ArrayList<>();
            for (String string : strings) {
                if(Math.random()*10<0.1){
                    list.add(string);
                }
            }
            return list;
        });
        JavaRDD<String> samples=samples1.flatMap((FlatMapFunction<Iterable<String>, String>) Iterable::iterator);
        //接下来进行数据筛选
        JavaRDD<String> hasMissingValues=samples.filter((Function<String, Boolean>) s -> {
            String rating=s.split("\\|")[6];
            return rating.equals("?");
        });
       // JavaRDD<String> noMissingValues=samples.subtract(hasMissingValues);
        JavaRDD<String> noMissingValues=samples.filter((Function<String, Boolean>) s -> {
            String rating=s.split("\\|")[6];
            return !rating.equals("?");
        });
        long num=noMissingValues.count();
        Comparator<String> comparator= (Comparator<String> & Serializable)(o1, o2) -> {//比较函数,将其进行序列化
            return comDouble(o1,o2,true);
        };
        noMissingValues.takeOrdered(10,comparator);
        List<String> a = noMissingValues.takeOrdered((int) (num - num / 100), comparator);
        JavaRDD<String> javaRDD=context.parallelize(a);
        List<String> b = noMissingValues.takeOrdered((int)  num / 100, comparator);
        JavaRDD<String> javaRDD1=javaRDD.subtract(context.parallelize(b));
        val1= Double.parseDouble(javaRDD1.first().split("\\|")[6]);
        List<String> strings=javaRDD1.takeOrdered(1, (Comparator<String> & Serializable)(o1, o2) -> comDouble(o1,o2,false));
        val2=Double.parseDouble(strings.get(0).split("\\|")[6]);
        //获得filter数据
        JavaRDD<String> filteredRdd=javaRDD1.filter((Function<String, Boolean>) Main::filter);
        JavaRDD<String> javaRDD2=hasMissingValues.filter((Function<String, Boolean>) Main::filter);
        JavaRDD<String> javaRDD3=filteredRdd.map((Function<String, String>) s -> {//进行标准化操作
            String[] strings1=s.split("\\|");
            return strings1[0]+"|"+strings1[1]+"|"+strings1[2]+"|"+strings1[3]+"|"+strings1[4]
                    +"|"+strings1[5]+"|"+Normalizing(Double.valueOf(strings1[6]))+"|"+strings1[7]+"|"+strings1[8]+"|"+strings1[9]+"|"+strings1[10]+"|"+strings1[11];
        });
        JavaRDD<String> javaRDD4=javaRDD3.union(javaRDD2);
        //接下来进行数据的标准化
        JavaRDD<String> javaRDD5=javaRDD4.map((Function<String, String>) s -> {
            String[] strings1=s.split("\\|");
            String temperature=changeTemperature(strings1[5]);
            String birthday=StandardData(strings1[8]);
            String review=StandardData(strings1[4]);
            return strings1[0]+"|"+strings1[1]+"|"+strings1[2]+"|"+strings1[3]+"|"+review +"|"+temperature+"|"+strings1[6]+"|"+strings1[7]+"|"+birthday+"|"+strings1[9]+"|"+strings1[10]+"|"+strings1[11];
        });
       // System.out.println(javaRDD5.count());
        //开始进行最后的步骤,填充缺失值
        JavaPairRDD<Integer,String> javaPairRDD=javaRDD5.mapToPair((PairFunction<String, Integer, String>) s -> {
            String[] strings1=s.split("\\|");
            int val=strings1[10].hashCode()+strings1[9].hashCode();
            return new Tuple2<>(val,s);
        });
        JavaPairRDD<Integer, Iterable<String>> javaPairRDD1 = javaPairRDD.groupByKey();
//        JavaPairRDD<Integer,Iterable<String>> javaPairRDD2=javaPairRDD1.map(new Function<Tuple2<Integer, Iterable<String>>, Object>() {
//            @Override
//            public Object call(Tuple2<Integer, Iterable<String>> integerIterableTuple2) throws Exception {
//
//            }
//        })
        JavaRDD<Iterable<String>> javaRDD6 = javaPairRDD1.values();
        javaRDD6=javaRDD6.map((Function<Iterable<String>, Iterable<String>>) strings12 -> {
            List<String> list1=new ArrayList<>();
            List<String> list2=new ArrayList<>();
            List<String> list3=new ArrayList<>();
            for (String string : strings12) {
                String[] strings1=string.split("\\|");
                if(strings1[11].equals("?")){
                    list1.add(string);
                }else {
                    list2.add(string);
                }
            }
            if(list2.size()==0){
                for (String s : list1) {
                    StringBuilder sb=new StringBuilder();
                    String[] strings2=s.split("\\|");
                    for (int i = 0; i <=10; i++) {
                        sb.append(strings2[i]).append("|");
                    }
                    sb.append(mean_value);
                }
            }else {
                int sum=0;
                int count=0;
                for (String s : list2) {
                    String[] strings2=s.split("\\|");
                    sum+=Integer.parseInt(strings2[11]);
                    count++;
                   // context.write(key,new Text(s));
                }
                int mean=sum/count;
                for (String s : list1) {
                    StringBuilder sb=new StringBuilder();
                    String[] strings2=s.split("\\|");
                    for (int i = 0; i <=10; i++) {
                        sb.append(strings2[i]).append("|");
                    }
                    sb.append(mean);
                    list3.add(sb.toString());
                }
            }
         //   list1.addAll(list2);
            list3.addAll(list2);
            return list3;
        });
        JavaRDD<String> javaRDD7=javaRDD6.flatMap((FlatMapFunction<Iterable<String>, String>) Iterable::iterator);//进行填充收入
        JavaRDD<String> javaRDD8=javaRDD7.filter((Function<String, Boolean>) s -> s.split("\\|")[6].equals("?"));
        JavaRDD<String> javaRDD9=javaRDD7.filter((Function<String, Boolean>) s -> !s.split("\\|")[6].equals("?"));
        List<Item> list1=new ArrayList<>();
        javaRDD9.foreach((VoidFunction<String>) s -> {
            //String[] s1=s.split("\t");
            String[] strings3=s.split("\\|");
            double income= Double.parseDouble(strings3[11]);
            double longitude= Double.parseDouble(strings3[1]);
            double latitude= Double.parseDouble(strings3[2]);
            if (income>maxIncome){
                maxIncome=income;
            }
            if(income<minIncome){
                minIncome=income;
            }
            if(longitude>maxLongitude){
                maxLongitude=longitude;
            }
            if(longitude<minLongitude){
                minLongitude=longitude;
            }
            if(latitude>maxLatitude){
                maxLongitude=latitude;
            }
            if(latitude<minLatitude){
                minLatitude=latitude;
            }
        });
        File file=new File("/home/xiao/文档/大数据分析/数据/sparkRes");
        PrintStream printStream=new PrintStream(file);
        System.setOut(printStream);
        List<String> list=javaRDD9.collect();
        for (String s : list) {
            String[] strings13 = s.split("\\|");
            double income= Double.parseDouble(strings13[11]);
            double longitude= Double.parseDouble(strings13[1]);
            double latitude= Double.parseDouble(strings13[2]);
            double rating= Double.parseDouble(strings13[6]);
            list1.add(new Item(standard(0,income),standard(1,latitude),standard(2,longitude),rating));
            System.out.println(s);
        }
        List<String> list2=javaRDD8.collect();
        for (String s : list2) {
            String[] strings1=s.split("\\|");
            double income= Double.parseDouble(strings1[11]);
            double longitude= Double.parseDouble(strings1[1]);
            double latitude= Double.parseDouble(strings1[2]);
            Item item=new Item(standard(0,income),standard(1,latitude),standard(2,longitude));
            double rat=findRat(item,list1);
            String res=strings1[0]+"|"+strings1[1]+"|"+strings1[2]+"|"+strings1[3]+"|"+strings1[4]
                    +"|"+strings1[5]+"|"+String.format("%.2f",rat)+"|"+strings1[7]+"|"+strings1[8]+"|"+strings1[9]+"|"+strings1[10]+"|"+strings1[11];
            System.out.println(res);
        }
    }
    private static double findRat(Item item,List<Item> list){
        list.sort((o1, o2) -> {
            double dis1=Math.pow((o1.income-item.income),2)+Math.pow((o1.latitude-item.latitude),2)+Math.pow(o1.longitude-item.longitude,2);
            double dis2=Math.pow((o2.income-item.income),2)+Math.pow((o2.latitude-item.latitude),2)+Math.pow(o2.longitude-item.longitude,2);
            return Double.compare(dis1, dis2);
        });
        if (list.size()==0){
            return mean_value;
        }
        return list.get(0).rating;
    }
    private static int comDouble(String o1, String o2, boolean reversed){
        double p1 = Double.parseDouble(o1.split("\\|")[6]);
        double p2 = Double.parseDouble(o2.split("\\|")[6]);
        if(reversed){
            return Double.compare(p2,p1);
//            return (int) ((p2 - p1) * 1000);
        }else{
            return Double.compare(p1,p2);
        }
    }
    private static boolean filter(String s){
        String[] strings1=s.split("\\|");
        double longtitude=Double.parseDouble(strings1[1]);
        double latitude=Double.parseDouble(strings1[2]);
        //context.write(new Text(rating1),new Text(strings[1]));
        return longtitude >= 8.1461259 && longtitude <= 11.1993265 && latitude >= 56.5824856 && latitude <= 57.750511;
    }
    private static String StandardData(String data) {
        if(data.charAt(0)>='0'&&data.charAt(0)<='9'){
            if(data.charAt(4)=='-'){
                return data;
            }else {
                String[] strings=data.split("/");
                return strings[0] + "-" + strings[1] + "-" + strings[2];
            }
        }
        String[] strings=data.split(" ");
        String[] strings1=strings[1].split(",");
        String s=strings1[0]+"-"+strings1[1];
        String month=strings[0];
        switch (month) {
            case "January":
                return "1-" + s;
            case "February":
                return "2-" + s;
            case "March":
                return "3-" + s;
            case "April":
                return "4-" + s;
            case "May":
                return "5-" + s;
            case "June":
                return "6-" + s;
            case "July":
                return "7-" + s;
            case "August":
                return "8-" + s;
            case "September":
                return "9-" + s;
            case "October":
                return "10-" + s;
            case "November":
                return "11-" + s;
            default:
                return "12" + s;
        }
    }
    private static String Normalizing(Double rate){
        return String.format("%.2f",(rate-Math.min(val1,val2))/(Math.abs(val1-val2)));
    }
    private static String changeTemperature(String s){
        int len=s.length();
        double num= Double.parseDouble(s.substring(0,len-2));
        if(s.charAt(len-1)=='℃'){
            return String.format("%.1f", num)+"℃";
        }else {
            double a=(num-32)/1.8;
            return String.format("%.1f",a)+"℃";
        }
    }
    private static double standard(int key,double val){
        if(key==0){
            return val-minIncome/(maxIncome-minIncome);
        }else if(key==1){
            return val-minLatitude/(maxLatitude-minLatitude);
        }
        return val-minLongitude/(maxLongitude-minLongitude);
    }
}
