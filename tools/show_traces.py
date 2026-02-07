#!/usr/bin/env python3
"""Display inference traces for specific users from taxonomy_review_trace.json."""
import json, sys

path = sys.argv[1] if len(sys.argv) > 1 else "data/output/taxonomy_review_trace.json"
targets = sys.argv[2:] if len(sys.argv) > 2 else [
    "Kate | Cryptorsy",
    "UC",
    "Pauline Shangett",
    "Jay Wong | Web3 Connector Connecting People, Projects and VC in Web3",
]

with open(path) as f:
    data = json.load(f)

for u in data["users"]:
    if u["display_name"] not in targets:
        continue

    print("=" * 80)
    print(f"USER: {u['display_name']}  (id={u['user_id']})")
    print("=" * 80)

    # Role claim trace
    rc = u.get("role_claim_trace")
    if rc:
        print(f"\n  ROLE CLAIM: {rc['label']}  (score={rc['raw_score']}, p={rc['probability']})")
        print("  Evidence:")
        for e in rc["evidence"]:
            print(f"    [{e['source_type']}] {e['pattern_id']}  (w={e['weight']})")
            print(f"      ref: {e['evidence_ref']}")
    else:
        print("\n  ROLE CLAIM: (none emitted)")

    # Intent claim trace
    ic = u.get("intent_claim_trace")
    if ic:
        print(f"\n  INTENT CLAIM: {ic['label']}  (score={ic['raw_score']}, p={ic['probability']})")
        print("  Evidence:")
        for e in ic["evidence"]:
            print(f"    [{e['source_type']}] {e['pattern_id']}  (w={e['weight']})")
            print(f"      ref: {e['evidence_ref']}")
    else:
        print("\n  INTENT CLAIM: (none emitted)")

    # Raw scores
    print("\n  RAW ROLE SCORES (pre-softmax):")
    for s in u.get("raw_role_scores", []):
        marker = " ◄" if s["raw_score"] > 0 else ""
        print(f"    {s['label']:20s}  score={s['raw_score']:8.4f}  p={s['probability']:.6f}{marker}")

    print("\n  RAW INTENT SCORES (pre-softmax):")
    for s in u.get("raw_intent_scores", []):
        marker = " ◄" if s["raw_score"] > 0 else ""
        print(f"    {s['label']:20s}  score={s['raw_score']:8.4f}  p={s['probability']:.6f}{marker}")

    # Top messages
    msgs = u.get("top_evidence_messages", [])
    if msgs:
        print(f"\n  TOP {len(msgs)} EVIDENCE MESSAGES:")
        for m in msgs:
            print(f"    msg_id={m['message_id']}  sent={m['sent_at'][:19]}  weight={m['total_evidence_weight']}")
            snippet = m["text_snippet"][:100]
            print(f"      text: {snippet}")
            for h in m["hits"]:
                print(f"        ↳ [{h['label_type']}:{h['label']}] pattern={h['pattern_id']}  span=\"{h['matched_span']}\"  w={h['weight']}")
    else:
        print("\n  TOP EVIDENCE MESSAGES: (none)")

    # Gating notes
    gn = u.get("gating_notes", [])
    if gn:
        print("\n  GATING NOTES:")
        for g in gn:
            print(f"    🚫 {g}")

    print()
