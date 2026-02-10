
import { calculateTimeDecay } from '../inference/engine.js';

console.log("=== Temporal Decay Verification ===");
console.log("Base Date: 2026-02-07");
console.log("Half Life: 180 days");

const dates = [
  "2026-02-07T00:00:00Z", // Base date (should be ~1.0)
  "2025-08-11T00:00:00Z", // -180 days (should be ~0.5)
  "2025-02-12T00:00:00Z", // -360 days (should be ~0.25)
  "2024-02-12T00:00:00Z", // ~2 years ago (should be very small)
  "2020-01-01T00:00:00Z"  // Ancient (should be ~0)
];

dates.forEach(date => {
  const weight = calculateTimeDecay(date);
  console.log(`Date: ${date} -> Weight: ${weight.toFixed(4)}`);
});

// Verify impact on score
// Score = log2(1 + sum(weights))
console.log("\n=== Score Impact Simulation ===");

const recentMessagesCount = 10;
const oldMessagesCount = 10;

const recentWeight = calculateTimeDecay("2026-01-01T00:00:00Z"); // Near present
const oldWeight = calculateTimeDecay("2024-01-01T00:00:00Z");   // Old

const recentScore = Math.log2(1 + (recentMessagesCount * recentWeight));
const oldScore = Math.log2(1 + (oldMessagesCount * oldWeight));

console.log(`10 Recent Messages (Weight ${recentWeight.toFixed(2)}) Score: ${recentScore.toFixed(2)}`);
console.log(`10 Old Messages    (Weight ${oldWeight.toFixed(2)}) Score: ${oldScore.toFixed(2)}`);

if (oldScore < recentScore * 0.5) {
    console.log("\n✅ SUCCESS: Older messages are significantly discounted.");
} else {
    console.log("\n❌ FAILURE: Decay not aggressive enough.");
}
