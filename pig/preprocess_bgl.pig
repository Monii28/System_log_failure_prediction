-- 1. Load the raw BGL text log file
raw_logs = LOAD '/logs/raw/bgl_logs/BGL_2k.log' USING TextLoader() AS (line:chararray);

-- 2. Filter out completely blank lines
clean_lines = FILTER raw_logs BY (line IS NOT NULL) AND (TRIM(line) != '');

-- 3. Resilient BGL parsing logic
-- Structure: AlertFlag Timestamp Date NodeComponent EventType Message
parsed_logs = FOREACH clean_lines GENERATE 
    FLATTEN(
        REGEX_EXTRACT_ALL(
            line, 
            '^(\\S+)\\s+(\\S+)\\s+(\\S+\\s+\\S+\\s+\\S+\\s+\\S+\\s+\\S+)\\s+(\\S+)\\s+(\\S+)\\s*(.*)$'
        )
    ) AS (
        alert_flag:chararray,
        timestamp_raw:chararray,
        log_date:chararray,
        node:chararray,
        component:chararray,
        message:chararray
    );

-- 4. Strip out lines that completely failed to parse to guarantee data quality
filtered_logs = FILTER parsed_logs BY alert_flag IS NOT NULL;

-- 5. Automatically wipe out old BGL output directory to avoid YARN collisions
sh /home/hduser/vit/hadoop-3.5.0/bin/hdfs dfs -rm -r -f /logs/processed/bgl_output

-- 6. Save the structured dataset
STORE filtered_logs INTO '/logs/processed/bgl_output' USING PigStorage(',');










