
## 🚀 Pipeline Phases & Component Summaries

### 1. Data Cleaning & Preprocessing (Apache Pig on MapReduce)
* **Objective:** Clean massive volumes of unstructured raw logs and drop background noise.
* **The Bottleneck & Solution:** Processing massive raw log datasets using rigid regular expressions over entire unparsed volumes initially caused pattern mismatches and formatting errors due to string variations. To overcome this big data challenge, high-precision engineering was applied using Apache Pig to handle malformed lines, extract critical metadata tokens, drop corrupted fields, and confidently structure a high-fidelity targeted subset of **12,800 verified records** to securely validate the end-to-end framework.

### 2. Time-Window Aggregation (Hadoop MapReduce)
* **Objective:** Group sporadic historical occurrences into structured time-series frequencies.
* **HDFS Logs Mapping:** Standardizes unparsed lines into a **Composite Key**: `(Date_Hour, Error_Type, Source)` [e.g., `("2015-11-09-09", "WARN", "HDFS")`] emitted with an integer counter **Value**: `1`.
* **BGL Logs Mapping:** Normalizes hardware/supercomputer log variants to output the exact same **Composite Key** layout: `(Date_Hour, Error_Type, Source)` [e.g., `("2005-06-04-11", "FATAL", "BGL")`] with a **Value**: `1`.
* **Reducer Logic:** Shuffles and sorts identical keys, summing up the counter arrays to generate an hourly frequency matrix (`Date_Hour, Error_Type, Source, Total_Count`).

### 3. Distributed Feature Engineering (PySpark DataFrames)
* **Objective:** Build multi-dimensional feature vectors out of basic count matrices.
* **Engineered Metrics:** Computes windowed variables directly across distributed Spark DataFrames:
  * **Error Frequency:** Hit density per hour.
  * **Error Distribution:** The percentage footprint of a specific error relative to the total cluster error load (`error_distribution > 0.40`).
  * **Rate of Change (Velocity):** Mathematical volume acceleration metrics over chronological windows.
* **Vectorization:** Implements Spark's `VectorAssembler` to pack calculated indicators into high-dimensional feature vectors.

### 4. Predictive Modeling & Verification (Spark MLlib & PySpark CLI)
* **Model:** A distributed **Random Forest Classifier** trained via Spark MLlib.
* **Prediction Logic:** Evaluates feature vectors to calculate probabilistic risk vectors and outputs binary prediction labels (`label = 1` indicates critical failure risk). It isolates random background noise from cascading failure patterns.
* **Pipeline Validation:** Launching an interactive `PySpark Shell` and running `printSchema()` confirms the successful persistence of outputs into optimized, columnar, **Snappy-compressed Parquet files** inside HDFS. Targeted `select().show(20)` instructions allow visual cross-verification of computed datasets before serving.

### 5. Intelligent Root-Cause Recommendation (Groq Engine & Prompt Builder)
* **Objective:** Automate human-readable diagnostics directly from machine-learning alerts.
* **Prompt Engineering:** Captures real-time predictive flags and dynamically binds raw variables (log source, error signature, window frequency, velocity acceleration) into a structural context template.
* **Inference Pipeline:** Submits the context payload to the ultra-fast hardware-accelerated **Groq Inference Engine** using the `llama-3.1-8b-instant` model. The AI bypasses simple threshold warnings to output explicit **Root Cause Analyses** and executable troubleshooting tasks for system administrators.

---

## 📁 Repository Structure

```text
├── preprocessing/
│   └── clean_logs.pig              # Apache Pig log parsing and filtering script
├── mapreduce/
│   ├── LogMapper.java              # Mapper producing Composite Keys for HDFS/BGL
│   └── LogReducer.java             # Reducer summing up error tallies
├── modeling/
│   └── feature_engineering_mllib.py # Spark MLlib feature generation & Random Forest model
├── recommendation/
│   └── generate_recommendation.py  # Groq API Prompt Builder & LLM pipeline
└── README.md                       # Documentation
