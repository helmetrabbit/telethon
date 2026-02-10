# Telethon Inference Engine v0.6.0 - Final Report

## Overview
This document summarizes the changes delivered in the "Advanced Inference" project phases. The system now supports temporal decay (weighting recent messages higher) and multi-label role assignment (detecting users with hybrid roles like "Founder" and "Builder").

## Architecture Upgrades

### 1. Temporal Weighting (Phase 4)
- **Goal**: Ensure user profiles reflect *current* activity, not ancient history.
- **Mechanism**: Exponential decay based on `sent_at` timestamp.
- **Parameters**: 
  - `HALF_LIFE_DAYS`: 180 days.
  - `REFERENCE_DATE`: 2026-02-07.
- **Impact**: A message from ~2 years ago has <10% the weight of a message from today.
- **Verification**: `src/cli/verify-decay.ts` validates the math.

### 2. Multi-label Roles (Phase 4)
- **Goal**: Capture the complexity of web3 users who often wear multiple hats.
- **Mechanism**: The inference engine now iterates through all candidate roles instead of picking just the top one.
- **Rules**:
  - Validates *each* candidate against `minClaimConfidence` (0.3 - 0.55).
  - Validates *each* candidate against evidence gating (must have non-membership evidence).
  - Emits all qualifying roles.

### 3. Observability Tools (Phase 5)
- **Goal**: Understand why users are gated and identify "near misses".
- **Tool**: `src/cli/analyze-results.ts`
- **Capabilities**:
  - Summarizes emit counts by predicate/status.
  - Groups abstentions by reason (Low Confidence vs Insufficient Evidence).
  - Lists top "Near Miss" users (Confidence 0.20 - 0.29) to guide future keyword tuning.

## Usage Guide

### Running Inference
```bash
INFERENCE_CONFIG=config/inference.v0.6.0.json npx tsx src/cli/infer-claims.ts
```

### verifying Temporal Math
```bash
npx tsx src/cli/verify-decay.ts
```

### Analyzing Results
```bash
npx tsx src/cli/analyze-results.ts
```

### Visual Dashboard (New!)
We have added a lightweight HTML dashboard to browse the results interactively.

1. **Export the data**:
   ```bash
   npx tsx src/cli/export-viewer.ts
   ```
2. **Open the Viewer**:
   Open `viewer/index.html` in your web browser. 
   
   *(Since the data is embedded in a JS file, you can double-click the HTML file directly on your machine without running a server).*

## Next Steps (Future)
- **Tuning**: Use the `analyze-results` output to inspect the 1,662 "Near Miss" users. If many are valid, consider lowering thresholds or adding specific keywords for them.
- **Live Deployment**: Integrate the `inference.v0.6.0.json` config into the production pipeline.
