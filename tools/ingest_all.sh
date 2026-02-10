#!/bin/bash
set -e

# 0. Ensure environment is ready
echo "🔨 Building project to ensure latest code..."
npm run build
echo "✅ Build valid."

echo "🚀 Starting Bulk Ingestion (Safe Mode - No Deletions)..."

# 1. Function to safely ingest
ingest_file() {
    FILE=$1
    if [ -f "$FILE" ]; then
        echo "---------------------------------------------------"
        echo "💾 Ingesting $FILE..."
        npm run ingest -- --file "$FILE"
    else
        echo "⚠️  Skipping $FILE (Not found)"
    fi
}

echo "📂 Looking for exports in data/exports/..."

# 3. Ingest all JSONs in exports
count=0
for f in data/exports/*.json; do
    ingest_file "$f"
    count=$((count + 1))
done

echo "---------------------------------------------------"
echo "🎉 Processed $count files."
echo "✅ ALL FILES INGESTED."
