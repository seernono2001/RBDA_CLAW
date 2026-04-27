#!/bin/bash
# =============================================================================
# Spotify Tracks Dataset — Full Pipeline
# NYU CSCI-GA.2436 | Anusha C (ac12273) | Group 30
#
# USAGE: Run each section in order on your Hadoop cluster.
# Set HADOOP_STREAMING_JAR to your actual jar path before running.
# =============================================================================

# --- CONFIGURE THESE ---
HADOOP_STREAMING_JAR="/usr/lib/hadoop-mapreduce/hadoop-streaming.jar"
# If unsure, find it with: find / -name "hadoop-streaming*.jar" 2>/dev/null

HDFS_BASE="/user/ac12273/spotify_project"
LOCAL_DATA="./dataset/spotify_tracks.csv"    # path to your downloaded CSV


# =============================================================================
# STEP 1 — Upload dataset to HDFS
# =============================================================================
echo ">>> Creating HDFS directories..."
hdfs dfs -mkdir -p ${HDFS_BASE}/raw
hdfs dfs -mkdir -p ${HDFS_BASE}/profiling/missing_output
hdfs dfs -mkdir -p ${HDFS_BASE}/profiling/numeric_output
hdfs dfs -mkdir -p ${HDFS_BASE}/profiling/categorical_output
hdfs dfs -mkdir -p ${HDFS_BASE}/cleaning/output

echo ">>> Uploading dataset to HDFS..."
hdfs dfs -put ${LOCAL_DATA} ${HDFS_BASE}/raw/spotify_tracks.csv

echo ">>> Verifying upload..."
hdfs dfs -ls ${HDFS_BASE}/raw/
hdfs dfs -du -h ${HDFS_BASE}/raw/spotify_tracks.csv


# =============================================================================
# STEP 2 — Profiling Job 1: Missing Values
# =============================================================================
echo ""
echo ">>> Running Profiling Job 1: Missing Values..."

# Remove output dir if it already exists
hdfs dfs -rm -r -f ${HDFS_BASE}/profiling/missing_output

hadoop jar ${HADOOP_STREAMING_JAR} \
  -input  ${HDFS_BASE}/raw/spotify_tracks.csv \
  -output ${HDFS_BASE}/profiling/missing_output \
  -mapper  "python3 profile_missing_mapper.py" \
  -reducer "python3 profile_missing_reducer.py" \
  -file    profiling/profile_missing_mapper.py \
  -file    profiling/profile_missing_reducer.py

echo ">>> Missing values result:"
hdfs dfs -cat ${HDFS_BASE}/profiling/missing_output/part-00000


# =============================================================================
# STEP 3 — Profiling Job 2: Numeric Statistics
# =============================================================================
echo ""
echo ">>> Running Profiling Job 2: Numeric Stats..."

hdfs dfs -rm -r -f ${HDFS_BASE}/profiling/numeric_output

hadoop jar ${HADOOP_STREAMING_JAR} \
  -input  ${HDFS_BASE}/raw/spotify_tracks.csv \
  -output ${HDFS_BASE}/profiling/numeric_output \
  -mapper  "python3 profile_numeric_mapper.py" \
  -reducer "python3 profile_numeric_reducer.py" \
  -file    profiling/profile_numeric_mapper.py \
  -file    profiling/profile_numeric_reducer.py

echo ">>> Numeric stats result:"
hdfs dfs -cat ${HDFS_BASE}/profiling/numeric_output/part-00000


# =============================================================================
# STEP 4 — Profiling Job 3: Categorical Column Distribution
# =============================================================================
echo ""
echo ">>> Running Profiling Job 3: Categorical Distribution..."

hdfs dfs -rm -r -f ${HDFS_BASE}/profiling/categorical_output

hadoop jar ${HADOOP_STREAMING_JAR} \
  -input  ${HDFS_BASE}/raw/spotify_tracks.csv \
  -output ${HDFS_BASE}/profiling/categorical_output \
  -mapper  "python3 profile_categorical_mapper.py" \
  -reducer "python3 profile_categorical_reducer.py" \
  -file    profiling/profile_categorical_mapper.py \
  -file    profiling/profile_categorical_reducer.py

echo ">>> Categorical distribution result:"
hdfs dfs -cat ${HDFS_BASE}/profiling/categorical_output/part-00000


# =============================================================================
# STEP 5 — Cleaning Job: Normalize + Deduplicate
# =============================================================================
echo ""
echo ">>> Running Cleaning Job: ETL + Deduplication..."

hdfs dfs -rm -r -f ${HDFS_BASE}/cleaning/output

hadoop jar ${HADOOP_STREAMING_JAR} \
  -input  ${HDFS_BASE}/raw/spotify_tracks.csv \
  -output ${HDFS_BASE}/cleaning/output \
  -mapper  "python3 clean_mapper.py" \
  -reducer "python3 clean_reducer.py" \
  -file    cleaning/clean_mapper.py \
  -file    cleaning/clean_reducer.py

echo ">>> Cleaned dataset row count:"
hdfs dfs -cat ${HDFS_BASE}/cleaning/output/part-00000 | wc -l

echo ">>> First 5 rows of cleaned data:"
hdfs dfs -cat ${HDFS_BASE}/cleaning/output/part-00000 | head -6


# =============================================================================
# STEP 6 — Save profiling results locally (for your PDF report)
# =============================================================================
echo ""
echo ">>> Saving profiling results locally..."
mkdir -p ./profiling_results

hdfs dfs -getmerge ${HDFS_BASE}/profiling/missing_output     ./profiling_results/missing_values.txt
hdfs dfs -getmerge ${HDFS_BASE}/profiling/numeric_output     ./profiling_results/numeric_stats.txt
hdfs dfs -getmerge ${HDFS_BASE}/profiling/categorical_output ./profiling_results/categorical_dist.txt

echo ""
echo ">>> All jobs complete. Results saved to ./profiling_results/"
echo ">>> Cleaned data is at: ${HDFS_BASE}/cleaning/output/"
