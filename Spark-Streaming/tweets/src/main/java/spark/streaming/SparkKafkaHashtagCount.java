package spark.streaming;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.desc;
import static org.apache.spark.sql.functions.from_json;
import static org.apache.spark.sql.functions.sum;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class SparkKafkaHashtagCount {

    public static void main(String[] args) throws Exception {

        if (args.length < 3) {
            System.err.println("Usage: SparkKafkaHashtagCount <bootstrap-servers> <subscribe-topics> <group-id>");
            System.exit(1);
        }

        String bootstrapServers = args[0];
        String topics = args[1];
        String groupId = args[2];

        SparkSession spark = SparkSession
                .builder()
                .appName("SparkKafkaHashtagCount")
                .getOrCreate();

        Dataset<Row> df = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", bootstrapServers)
                .option("subscribe", topics)
                .option("kafka.group.id", groupId)
                .load();
        df.selectExpr("CAST(value AS STRING)").writeStream().format("console").start();
                // Define the schema for the JSON data
                StructType schema = new StructType()
                .add("content", DataTypes.StringType)
                .add("hashtags", DataTypes.StringType)
                .add("likeCount", DataTypes.IntegerType)
                .add("quoteCount", DataTypes.IntegerType)
                .add("replyCount", DataTypes.IntegerType)
                .add("retweetCount", DataTypes.IntegerType)
                .add("sourceLabel", DataTypes.StringType)
                .add("username", DataTypes.StringType);
    
            // Parse the JSON data
            Dataset<Row> jsonData = df.selectExpr("CAST(value AS STRING) as json")
                .select(from_json(col("json"), schema).as("data"))
                .select("data.*");
    
        // Calculate the count of each hashtag
        Dataset<Row> hashtagCounts = jsonData
            .groupBy("hashtags")
            .count()
            .sort(desc("count"));

        // Calculate the count of tweets for each user
        Dataset<Row> userTweets = jsonData
            .groupBy("username")
            .count()
            .sort(desc("count"));

        // Calculate the sum of likes for each content
        Dataset<Row> contentLikes = jsonData
            .groupBy("content")  // change this line
            .agg(sum("likeCount").as("totalLikes"))
            .sort(desc("totalLikes"));

        hashtagCounts.writeStream().outputMode("complete").format("console").start();
        userTweets.writeStream().outputMode("complete").format("console").start();
        contentLikes.writeStream().outputMode("complete").format("console").start();  // change this line

        spark.streams().awaitAnyTermination();
    }
    
}
//spark-submit --class spark.streaming.SparkKafkaHashtagCount --master local  tweets-2-jar-with-dependencies.jar localhost:9092 tweets mySparkConsumerGroup >> tweetsCountv3
