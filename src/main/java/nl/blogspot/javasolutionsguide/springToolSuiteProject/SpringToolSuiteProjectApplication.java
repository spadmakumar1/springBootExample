package nl.blogspot.javasolutionsguide.springToolSuiteProject;

import java.util.Arrays;
import java.util.Map;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class SpringToolSuiteProjectApplication {

	public static void main(String[] args) {
		SpringApplication.run(SpringToolSuiteProjectApplication.class, args);
		System.out.println("padma line 12");
		/*SparkSession sparkSession = SparkSession
				.builder()
				.appName("SparkWithSpring")
				.master("local")
				.getOrCreate();
		System.out.println("Spark Version Padma : " + sparkSession.version());*/
		


        Logger.getLogger("org").setLevel(Level.ERROR);
        SparkConf conf = new SparkConf().setAppName("wordCounts").setMaster("local[3]");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("in/word_count.text");
        JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

        Map<String, Long> wordCounts = words.countByValue();

        for (Map.Entry<String, Long> entry : wordCounts.entrySet()) {
            System.out.println(entry.getKey() + " : " + entry.getValue());
        }
    
	}
}
