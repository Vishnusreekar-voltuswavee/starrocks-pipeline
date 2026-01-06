#!/bin/bash

# Script to download all necessary JAR files for Spark-Iceberg-MySQL pipeline
# Creates a 'jars' directory and downloads required dependencies

set -e

echo "=========================================="
echo "Downloading Required JAR Files"
echo "=========================================="

# Create jars directory
JARS_DIR="./jars"
mkdir -p "$JARS_DIR"

echo "JAR files will be saved to: $JARS_DIR"
echo ""

# Maven Central base URL
MAVEN_CENTRAL="https://repo1.maven.org/maven2"

# Define JAR files to download
declare -A JARS=(
    # Iceberg Spark Runtime (compatible with Spark 3.5)
    ["iceberg-spark-runtime"]="$MAVEN_CENTRAL/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.4.3/iceberg-spark-runtime-3.5_2.12-1.4.3.jar"

    # AWS SDK Bundle for S3 access
    ["aws-java-sdk-bundle"]="$MAVEN_CENTRAL/com/amazonaws/aws-java-sdk-bundle/1.12.648/aws-java-sdk-bundle-1.12.648.jar"

    # Hadoop AWS for S3A filesystem
    ["hadoop-aws"]="$MAVEN_CENTRAL/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar"

    # MySQL JDBC Driver
    ["mysql-connector-j"]="$MAVEN_CENTRAL/com/mysql/mysql-connector-j/8.2.0/mysql-connector-j-8.2.0.jar"

    # Hadoop Common (dependency for Hadoop AWS)
    ["hadoop-common"]="$MAVEN_CENTRAL/org/apache/hadoop/hadoop-common/3.3.4/hadoop-common-3.3.4.jar"
)

# Download each JAR file
for jar_name in "${!JARS[@]}"; do
    jar_url="${JARS[$jar_name]}"
    jar_filename=$(basename "$jar_url")

    echo "Downloading: $jar_name"
    echo "  URL: $jar_url"
    echo "  File: $jar_filename"

    if [ -f "$JARS_DIR/$jar_filename" ]; then
        echo "  ✓ Already exists, skipping"
    else
        wget -q --show-progress -O "$JARS_DIR/$jar_filename" "$jar_url"

        if [ $? -eq 0 ]; then
            echo "  ✓ Downloaded successfully"
        else
            echo "  ✗ Failed to download"
            exit 1
        fi
    fi
    echo ""
done

echo "=========================================="
echo "Download Summary"
echo "=========================================="
echo "Total JAR files: ${#JARS[@]}"
echo "Location: $JARS_DIR"
echo ""
ls -lh "$JARS_DIR"
echo ""
echo "✓ All JAR files downloaded successfully!"
echo ""
echo "Next Steps:"
echo "1. Set SPARK_HOME environment variable if not already set"
echo "2. Add jars to Spark classpath by copying to \$SPARK_HOME/jars:"
echo "   cp $JARS_DIR/*.jar \$SPARK_HOME/jars/"
echo "3. Or set in your application using --jars option"
echo "=========================================="
