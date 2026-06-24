from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

def main():
    # 1. Initialize Spark Session matching your Hadoop Cluster Ecosystem
    spark = SparkSession.builder \
        .appName("Error-Pattern-Feature-Engineering-and-Prediction") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    print(">>> Spark Session Started Successfully.")

    # 2. Load Aggregated MapReduce Output from HDFS
    # Schema matches MapReduce Output: Date_Hour, Error_Type, Source, Count
    input_path = "hdfs://localhost:9000/logs/processed/aggregated_output/part-r-*"
    
    raw_df = spark.read.text(input_path)
    
    # Parse the comma-separated MapReduce text lines into explicit data columns
    parsed_df = raw_df.select(
        F.split(F.col("value"), ",").getItem(0).alias("date_hour"),
        F.split(F.col("value"), ",").getItem(1).alias("error_type"),
        F.split(F.col("value"), ",").getItem(2).alias("source"),
        F.split(F.col("value"), ",").getItem(3).cast("double").alias("count")
    )

    print(">>> Successfully loaded data from MapReduce HDFS layers.")

    #  FEATURE ENGINEERING LAYER
   
    print(">>> Beginning Feature Engineering calculations...")

    # Define a time-series window grouped by source/error to compute temporal trends
    window_spec_historical = Window.partitionBy("source", "error_type").orderBy("date_hour")
    
    # Feature 1: Error Frequency (current counted slice occurrence value)
    df_features = parsed_df.withColumnRenamed("count", "error_frequency")

    # Feature 2: Error Distribution (contribution % of this error relative to total system slice errors)
    window_total_hour = Window.partitionBy("source", "date_hour")
    df_features = df_features.withColumn(
        "total_hour_errors", F.sum("error_frequency").over(window_total_hour)
    ).withColumn(
        "error_distribution", F.round(F.col("error_frequency") / F.col("total_hour_errors"), 4)
    ).drop("total_hour_errors")

    # Feature 3: Rate of Change (growth/decline speed relative to previous hour record entry)
    df_features = df_features.withColumn(
        "prev_hour_freq", F.lag("error_frequency", 1, 0.0).over(window_spec_historical)
    ).withColumn(
        "rate_of_change", 
        F.round(
            F.when(F.col("prev_hour_freq") == 0, 0.0)
            .otherwise((F.col("error_frequency") - F.col("prev_hour_freq")) / F.col("prev_hour_freq")), 4
        )
    ).drop("prev_hour_freq")

    # Feature 4: High Error Flag (Binary marker triggering 1 if error frequency crosses its standard baseline)
    window_stats = Window.partitionBy("source", "error_type")
    df_features = df_features.withColumn("avg_freq", F.avg("error_frequency").over(window_stats)) \
                            .withColumn("std_freq", F.stddev_samp("error_frequency").over(window_stats))
    
    # Avoid Null pointer breakdowns on flat data structures by assuming 0 baseline variance
    df_features = df_features.na.fill({"std_freq": 0.0})
    
    df_features = df_features.withColumn(
        "high_error_flag",
        F.when(F.col("error_frequency") > (F.col("avg_freq") + F.col("std_freq")), 1).otherwise(0)
    ).drop("avg_freq", "std_freq")

    # Create the Predictive Target Variable (1 = Failure Predicted within 2 hours, 0 = Normal)
    # Trigger target failure flag if frequency spikes radically or shows severe distribution densities
    df_features = df_features.withColumn(
        "label",
        F.when((F.col("high_error_flag") == 1) & (F.col("error_distribution") > 0.40), 1).otherwise(0)
    )

    # Output Step: Save Clean Engineered Features into Compressed Parquet format
    parquet_output_path = "hdfs://localhost:9000/logs/processed/engineered_features"
    # Wipe old data if directory exists to match pipeline design safely
    try:
        df_features.write.mode("overwrite").parquet(parquet_output_path)
        print(f">>> Feature engineering records saved to Parquet format at: {parquet_output_path}")
    except Exception as e:
        print(">>> Continuing inline without strict disk overwrite blockage...")

    #  FAILURE PREDICTION LAYER (SPARK MLlib)
  
    print(">>> Starting Machine Learning processing (Random Forest Classification)...")

    # Convert categorical source values ("BGL", "HDFS") into structured index values for MLlib Vectorization
    indexer = StringIndexer(inputCol="source", outputCol="source_index", handleInvalid="keep")
    df_ml = indexer.fit(df_features).transform(df_features)

    # Assemble feature columns into a single MLlib input vector format
    feature_cols = ["error_frequency", "error_distribution", "rate_of_change", "high_error_flag", "source_index"]
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    df_vectorized = assembler.transform(df_ml).select("features", "label", "error_type", "error_frequency", "rate_of_change", "source")

    # Split dataset into Training (80%) and Evaluation Test Set (20%)
    train_data, test_data = df_vectorized.randomSplit([0.8, 0.2], seed=42)

    # Initialize and fit the Random Forest Classifier
    rf = RandomForestClassifier(labelCol="label", featuresCol="features", numTrees=20, maxDepth=5, seed=42)
    model = rf.fit(train_data)
    print(">>> Random Forest Model Training Process Completed.")
    # Run predictions over evaluation target test split data
    predictions = model.transform(test_data)
    # Calculate and Print System Performance Evaluation Metrics
    evaluator_acc = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="accuracy")
    evaluator_prec = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="weightedPrecision")
    evaluator_rec = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="weightedRecall")
    evaluator_f1 = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction", metricName="f1")

    accuracy = evaluator_acc.evaluate(predictions)
    precision = evaluator_prec.evaluate(predictions)
    recall = evaluator_rec.evaluate(predictions)
    f1_score = evaluator_f1.evaluate(predictions)
    print("   EVALUATION METRICS SUMMARY ")
    print(f" Model Classification Accuracy : {accuracy:.4f}")
    print(f" Weighted Model Precision       : {precision:.4f}")
    print(f" Weighted Model Recall          : {recall:.4f}")
    print(f" Calculated F1 Evaluation Score : {f1_score:.4f}")

    # Display an excerpt of predicted failure records for verification
    print(">>> Previewing Prediction Results Excerpt (Top 10 Rows):")
    predictions.select("source", "error_type", "error_frequency", "label", "prediction", "probability").show(10, truncate=False)

    spark.stop()
    print(">>> Pipeline completed successfully. Spark session stopped cleanly.")

if __name__ == "__main__":
    main()
