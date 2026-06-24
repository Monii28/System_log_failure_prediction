-- 1. Load the raw text log file
raw_logs = LOAD '/logs/raw/hdfs_logs/HDFS_2k.log' USING TextLoader() AS (line:chararray);

-- 2. Filter out completely empty or null lines only (No restrictive date regex)
clean_lines = FILTER raw_logs BY (line IS NOT NULL) AND (TRIM(line) != '');

-- 3. Parse fields using an adaptable, space-insensitive regex pattern
parsed_logs = FOREACH clean_lines GENERATE 
    FLATTEN(
        REGEX_EXTRACT_ALL(
            line, 
            '^(\\S+)\\s+(\\S+)\\s+(\\S+)\\s+(\\S+)\\s+([^:]+):\\s*(.*)$'
        )
    ) AS (
        log_date:chararray, 
        log_time:chararray, 
        pid:chararray, 
        log_level:chararray, 
        component:chararray, 
        message:chararray
    );

-- 4. Strip out any rows that totally failed to map to prevent null rows
filtered_logs = FILTER parsed_logs BY log_date IS NOT NULL;

-- 5. Wipe out old data to clear the runway
sh /home/hduser/vit/hadoop-3.5.0/bin/hdfs dfs -rm -r -f /logs/processed/hdfs_output

-- 6. Save the validated structured output records
STORE filtered_logs INTO '/logs/processed/hdfs_output' USING PigStorage(',');
















