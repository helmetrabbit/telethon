/**
 * Build a structured briefing for the LLM from existing DB data.
 * No stopword stripping — the LLM needs full context.
 * Instead, we select the most *informative* messages.
 */

import { ROLES, INTENTS, ORG_TYPES } from '../config/taxonomies.js';

export interface UserBriefing {
  userId: number;
  handle: string | null;
  displayName: string | null;
  bio: string | null;
  groups: { title: string; kind: string; msgCount: number; description?: string }[];
  totalMessages: number;
  sampleMessages: { sent_at: string; text: string; groupTitle?: string }[];
  existingClaims: { predicate: string; value: string; confidence: number; status: string }[];
  
  // Omni-Capture Metrics
  viralStats?: {
    totalViews: number;
    avgViews: number;
    totalReactions: number;
    totalReplies: number;
    repliesReceived: number;
  };
}

/**
 * Maximum chars of message content to include in the prompt.
 * Models like lfm-2.5-1.2b have ~8k context. We budget ~4k for messages.
 */
const MAX_MESSAGE_CHARS = 4000;

export function buildPrompt(briefing: UserBriefing): string {
  const groupList = briefing.groups
    .map((g) => `  - ${g.title} (${g.kind}, ${g.msgCount} msgs)`)
    .join('\n');

  // Select message sample: prioritize longer messages (more signal),
  // keep chronological order within that.
  let msgBlock = '';
  let charBudget = MAX_MESSAGE_CHARS;
  for (const m of briefing.sampleMessages) {
    const line = `[${m.sent_at}] ${m.text}\n`;
    if (charBudget - line.length < 0) break;
    msgBlock += line;
    charBudget -= line.length;
  }

  const claimList = briefing.existingClaims.length > 0
    ? briefing.existingClaims
        .map((c) => `  - ${c.predicate}: ${c.value} (confidence=${c.confidence}, ${c.status})`)
        .join('\n')
    : '  (none)';

  return `You are classifying a Telegram user based on their profile and chat behavior.

## User Profile
- Handle: ${briefing.handle || '(none)'}
- Display Name: ${briefing.displayName || '(none)'}
- Bio: ${briefing.bio || '(none)'}
- Total Messages: ${briefing.totalMessages}
- Groups:
${groupList || '  (none)'}

## Deterministic Engine Assessment
${claimList}

## Sample Messages (most informative, chronological)
${msgBlock || '(no messages)'}

## Task
Based on ALL the above, produce a JSON object with exactly these fields:

{
  "synthetic_bio": "A 1-2 sentence professional summary of who this person is and what they do. Be specific — mention companies, roles, and domains.",
  "roles": ["role1"],
  "role_reasoning": "Brief explanation of why you chose these roles.",
  "intents": ["intent1"],
  "intent_reasoning": "Brief explanation of why you chose these intents.",
  "org_affiliation": "Company or project name if identifiable, or null",
  "org_type": "Type of org if identifiable, or null",
  "confidence": 0.0 to 1.0
}

CONSTRAINTS:
- roles MUST be from: ${ROLES.filter((r) => r !== 'unknown').join(', ')}
- intents MUST be from: ${INTENTS.filter((i) => i !== 'unknown').join(', ')}
- org_type MUST be from: ${ORG_TYPES.filter((o) => o !== 'unknown').join(', ')} or null
- Return ONLY the JSON object, no markdown fences, no explanation outside the JSON.`;
}
