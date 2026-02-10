#!/bin/bash
set -e

# 1. Ingest the already-exported "BD in Web3" (The big one)
echo "🚀 Processing BD in Web3 (Ingest Only)..."
npm run ingest -- --file data/exports/telethon_bd_web3.json

# 2. Function to Nuke, Export, and Ingest
sync_group() {
    NAME=$1
    ID=$2
    FILE="data/exports/$NAME.json"

    echo "---------------------------------------------------"
    echo "🔄 Syncing $NAME ($ID)..."
    
    # Remove old file to force full re-sync
    if [ -f "$FILE" ]; then
        echo "   🗑️  Removing old export $FILE..."
        rm "$FILE"
    fi

    # Export
    echo "   📥 Exporting..."
    python3 tools/telethon_collector/collect_group_export.py --group "$ID" --out "$FILE"

    # Ingest
    echo "   💾 Ingesting..."
    npm run ingest -- --file "$FILE"
    echo "   ✅ Done: $NAME"
}

# 3. Process the rest sequentially
sync_group "eth_magicians"      1420010957
sync_group "avalanche_builders" 1365896273
sync_group "btc_connect"        2122365595
sync_group "lobsterdao"         1242127973
sync_group "savvy_conferences"  2109157701
sync_group "the_best_event"     2037162803
sync_group "the_trenches"       2176273042

echo "---------------------------------------------------"
echo "🎉 ALL GROUPS SYNCED SUCCESSFULLY."
