#!/bin/bash
set -e

echo "🚀 Fresh Database + Optimized Bulk Ingestion"
echo "============================================"
echo ""

# Step 0: Reset database
echo "🗑️  Step 0: Resetting database (dropping all data)..."
npm run db:reset
echo "✅ Database reset complete."
echo ""

# Step 1: Build
echo "🔨 Step 1: Building TypeScript..."
npm run build
echo "✅ Build complete."
echo ""

# Step 2: Disable constraints for performance
echo "⚡ Step 2: Disabling constraints for maximum performance..."
npm run run-sql "ALTER TABLE messages DISABLE TRIGGER flag_user_dirty"
npm run run-sql "ALTER TABLE messages DISABLE TRIGGER ALL"
npm run run-sql "ALTER TABLE memberships DISABLE TRIGGER ALL"
npm run run-sql "ALTER TABLE message_mentions DISABLE TRIGGER ALL"
npm run run-sql "DROP INDEX IF EXISTS idx_messages_dedup"
npm run run-sql "DROP INDEX IF EXISTS idx_messages_user"
npm run run-sql "DROP INDEX IF EXISTS idx_messages_group"
npm run run-sql "DROP INDEX IF EXISTS idx_messages_sent_at"
echo "✅ Constraints disabled, indexes dropped."
echo ""

# Step 3: Bulk ingest
echo "📥 Step 3: Starting bulk ingestion..."
echo "   (Should run at ~300-400 messages/sec consistently)"
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

# Step 4: Re-enable constraints and rebuild indexes
echo "🔄 Step 4: Rebuilding indexes and re-enabling constraints..."
npm run run-sql "CREATE UNIQUE INDEX CONCURRENTLY idx_messages_dedup ON messages(group_id, external_message_id)"
npm run run-sql "CREATE INDEX CONCURRENTLY idx_messages_user ON messages(user_id)"
npm run run-sql "CREATE INDEX CONCURRENTLY idx_messages_group ON messages(group_id)"
npm run run-sql "CREATE INDEX CONCURRENTLY idx_messages_sent_at ON messages(sent_at)"
npm run run-sql "ALTER TABLE messages ENABLE TRIGGER ALL"
npm run run-sql "ALTER TABLE memberships ENABLE TRIGGER ALL"
npm run run-sql "ALTER TABLE message_mentions ENABLE TRIGGER ALL"
echo "✅ Indexes rebuilt, constraints re-enabled."
echo ""

# Step 5: Flag all users for enrichment
echo "🏷️  Step 5: Flagging all users for enrichment..."
npm run run-sql "UPDATE users SET needs_enrichment = true, last_msg_at = (SELECT MAX(sent_at) FROM messages WHERE messages.user_id = users.id)"
echo "✅ All users flagged."
echo ""

echo "============================================"
echo "🎉 FRESH INGESTION COMPLETE!"
echo ""
echo "Database Stats:"
npm run run-sql "SELECT COUNT(*) as total_messages FROM messages"
npm run run-sql "SELECT COUNT(*) as total_users FROM users"
npm run run-sql "SELECT COUNT(*) as total_groups FROM groups"
echo ""
echo "Next steps:"
echo "  - npm run compute-features"
echo "  - npm run infer-claims"
echo "  - npm run enrich-psycho"
echo ""
