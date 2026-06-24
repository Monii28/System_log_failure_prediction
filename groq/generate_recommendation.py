import os
from groq import Groq
from pyspark.sql import SparkSession

def main():
    # Make sure to set your Groq API Key!
    api_key = os.environ.get("GROQ_API_KEY", "YOUR_ACTUAL_GROQ_API_KEY_HERE")
    if api_key == "YOUR_ACTUAL_GROQ_API_KEY_HERE" or not api_key:
        print(">>> WARNING: Please replace 'YOUR_ACTUAL_GROQ_API_KEY_HERE' with your valid Groq API key.")
        return

    client = Groq(api_key=api_key)

    spark = SparkSession.builder.appName("Groq-Root-Cause-Analysis").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    parquet_path = "hdfs://localhost:9000/logs/processed/engineered_features"
    df_features = spark.read.parquet(parquet_path)

    # Grab some high error records to analyze
    failed_patterns = df_features.filter(df_features.high_error_flag == 1).limit(3).collect()

    print(f"\n>>> Found {len(failed_patterns)} error patterns requiring Root Cause Analysis.\n")

    for index, row in enumerate(failed_patterns):
        print(f"[ANALYSIS PATTERN {index + 1}] ")
        print(f"Log Source    : {row['source']}")
        print(f"Error Pattern : {row['error_type']}")
        print(f"Frequency     : {row['error_frequency']} hits/hr")
        print(f"Distribution  : {row['error_distribution'] * 100:.2f}%\n")

        prompt = f"""
        You are an expert Systems Architect and Site Reliability Engineer (SRE). 
        Analyze this error anomaly from our distributed cluster logs:
        - Infrastructure Dataset Source: {row['source']}
        - System Error Signature: "{row['error_type']}"
        - Current Window Frequency: {row['error_frequency']} occurrences
        
        Please provide:
        1. ROOT CAUSE REASON: Why this error happens in {row['source']}.
        2. EXECUTABLE RECOMMENDATION: 2 clear action steps for the SysAdmin to fix it.
        """

        try:
            chat_completion = client.chat.completions.create(
                messages=[{"role": "user", "content": prompt}],
                model="llama3-8b-8192",
                temperature=0.2,
                max_tokens=400
            )
            print(">>> Groq Analysis:")
            print(chat_completion.choices[0].message.content)
            print("-" * 65 + "\n")
        except Exception as e:
            print(f">>> Groq API Error: {e}")

    spark.stop()

if __name__ == "__main__":
    main()
