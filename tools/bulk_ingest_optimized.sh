#!/bin/bash
set -e

echo "🚀 Optimized Bulk Ingestion Workflow"
echo "===================================="
echo ""

# Step 0: Build
echo "🔨 Step 0: Building TypeScript..."
npm run build
echo "✅ Build complete."
echo ""

# Step 1: Disable trigger
echo "⚡ Step 1: Disabling trigger for performance..."
npm run run-sql "ALTER TABLE messages DISABLE TRIGGER flag_user_dirty"
echo "✅ Trigger disabled."
echo ""

# Step 2: Bulk ingest
echo "📥 Step 2: Starting bulk ingestion..."
echo "   (This should run at ~300-400 messages/sec consistently)"
echo ""

count=0
for f in data/exports/*.json; do
    if [ -f "$f" ]; then
        echo "---------------------------------------------------"
        echo "💾 Ingesting $f..."
        npm run bulk-ingest -- --file "$f"
        count=$((count + 1))
    fi
done

echo ""
echo "✅ Ingested $count files."
echo ""

# Step 3: Re-enable trigger
echo "🔄 Step 3: Re-enabling trigger..."
npm run run-sql "ALTER TABLE messages ENABLE TRIGGER flag_user_dirty"
echo "✅ Trigger re-enabled."
echo ""

# Step 4: Flag all users for enrichment
echo "🏷️  Step 4: Flagging all users for enrichment..."
npm run run-sql "UPDATE users SET needs_enrichment = true, last_msg_at = (SELECT MAX(sent_at) FROM messages WHERE messages.user_id = users.id)"
echo "✅ All users flagged."
echo ""

echo "===================================="
echo "🎉 BULK INGESTION COMPLETE!"
echo ""
echo "Next steps:"
echo "  - Run: npm run compute-features"
echo "  - Run: npm run infer-claims"
echo "  - Run: npm run enrich-psycho"
echo ""
