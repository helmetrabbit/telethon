/**
 * OpenRouter LLM client with retry logic, rate limiting, and structured output.
 */

import { createHash } from 'node:crypto';

export interface LLMConfig {
  apiKeys: string[];
  model: string;
  maxRetries: number;
  retryDelayMs: number;
  requestDelayMs: number; // Delay between requests (rate limiting)
  maxTokens?: number;
  temperature?: number;
  title?: string;
}

export interface LLMResponse {
  content: string;
  latencyMs: number;
  promptTokens?: number;
  completionTokens?: number;
  totalTokens?: number;
  model?: string;
  requestId?: string;
}

const DEFAULT_CONFIG: Partial<LLMConfig> = {
  maxRetries: 3,
  retryDelayMs: 2000,
  requestDelayMs: 500,
};

export function createLLMClient(config: LLMConfig) {
  const cfg = { ...DEFAULT_CONFIG, ...config };
  let lastRequestAt = 0;
  
  // Track rate limit reset times for each key: key -> timestamp (epoch ms)
  const keyRateLimits = new Map<string, number>();
  let currentKeyIndex = 0;

  function getAvailableKey(): string | null {
    const now = Date.now();
    for (let i = 0; i < cfg.apiKeys!.length; i++) {
        // Check keys starting from current index to round-robin
        const idx = (currentKeyIndex + i) % cfg.apiKeys!.length;
        const key = cfg.apiKeys![idx];
        const resetTime = keyRateLimits.get(key) || 0;
        
        if (now > resetTime) {
            currentKeyIndex = idx;
            return key;
        }
    }
    return null;
  }


  async function complete(prompt: string): Promise<LLMResponse> {
    for (let attempt = 1; attempt <= cfg.maxRetries!; attempt++) {
      // 1. Get an available key
      let activeKey = getAvailableKey();
      
      // If no keys are available, find the earliest reset time and wait
      if (!activeKey) {
          const earliestReset = Math.min(...Array.from(keyRateLimits.values()));
          const now = Date.now();
          if (earliestReset > now) {
              let waitMs = earliestReset - now + 1000;
              if (waitMs > 60000) waitMs = 60000; // Cap sleep at 60s
              console.warn(`    ⏳ All keys rate limited. Sleeping ${Math.ceil(waitMs/1000)}s...`);
              await new Promise(r => setTimeout(r, waitMs));
              // Continue loop, which will call getAvailableKey() again
              attempt--; 
              continue;
          } else {
             // Should verify why getAvailableKey failed if earliestReset <= now
             // Just force pick one to retry? No, just continue loop.
             attempt--;
             continue;
          }
      }

      // 2. Per-Request Throttling (Jitter)
      const jitter = Math.random() * 500; 
      const elapsed = Date.now() - lastRequestAt;
      if (elapsed < cfg.requestDelayMs!) {
        await new Promise((r) => setTimeout(r, (cfg.requestDelayMs! - elapsed) + jitter));
      }

      const startMs = Date.now();
      try {
        lastRequestAt = startMs;
        const response = await fetch('https://openrouter.ai/api/v1/chat/completions', {
          method: 'POST',
          headers: {
            Authorization: `Bearer ${activeKey}`,
            'Content-Type': 'application/json',
            'HTTP-Referer': 'https://github.com/helmetrabbit/telethon',
            'X-Title': cfg.title || `Telethon Inference Engine (${cfg.model})`,
          },
          body: JSON.stringify({
            model: cfg.model,
            messages: [{ role: 'user', content: prompt }],
            temperature: cfg.temperature ?? 0.2,
            ...(cfg.maxTokens ? { max_tokens: cfg.maxTokens } : {}),
          }),
        });

        if (!response.ok) {
          const bodyText = await response.text();
          
          // Handle 429 specifically
          if (response.status === 429) {
            let resetTime = 0;
            const resetHeader = response.headers.get('x-ratelimit-reset');
            if (resetHeader) {
               const val = parseFloat(resetHeader);
               if (val > 2000000000000) resetTime = val; 
               else resetTime = val * 1000; 
            }

            if (!resetTime) {
                try {
                    const json = JSON.parse(bodyText);
                    const metaReset = json?.error?.metadata?.headers?.['X-RateLimit-Reset'];
                    if (metaReset) {
                         const rawVal = parseFloat(metaReset);
                         if (rawVal > 2000000000000) resetTime = rawVal; 
                         else if (rawVal > 100000000000) resetTime = rawVal; 
                         else resetTime = rawVal * 1000;
                    }
                } catch (e) { }
            }

            if (!resetTime) resetTime = Date.now() + 60000;

            // Mark THIS key as limited
            console.warn(`    🛑 Rate limit hit for key ...${activeKey.slice(-4)}. Pausing it until ${new Date(resetTime).toLocaleTimeString()}`);
            keyRateLimits.set(activeKey, resetTime);
            
            throw new Error(`Rate limit exceeded (429)`);
          }

          throw new Error(`API ${response.status}: ${bodyText}`);
        }

        const requestId = response.headers.get('x-request-id') || response.headers.get('x-openrouter-request-id') || undefined;
        const data = await response.json();
        const content = data.choices?.[0]?.message?.content;
        if (!content) throw new Error('Empty response from LLM');

        const usage = data.usage || {};
        const promptTokens = Number.isFinite(Number(usage.prompt_tokens)) ? Number(usage.prompt_tokens) : undefined;
        const completionTokens = Number.isFinite(Number(usage.completion_tokens)) ? Number(usage.completion_tokens) : undefined;
        const totalTokens = Number.isFinite(Number(usage.total_tokens)) ? Number(usage.total_tokens) : undefined;

        return {
          content,
          latencyMs: Date.now() - startMs,
          promptTokens,
          completionTokens,
          totalTokens,
          model: typeof data.model === 'string' ? data.model : cfg.model,
          requestId,
        };

      } catch (err) {
        const msg = (err as Error).message;
        const isRateLimit = msg.includes('429') || msg.includes('Rate limit');

        if (isRateLimit) {
             attempt--; // Retry infinitely
             // Immediate continue will cycle to getAvailableKey(), which will pick a NEW key
             continue;
        }

        if (attempt === cfg.maxRetries!) {
            console.error(`    ❌ User failed after ${attempt} attempts.`);
            throw err;
        }
        

        // If it was a 500 or timeout, we do a local exponential backoff.
        const backoff = (cfg.retryDelayMs! * Math.pow(2, attempt - 1));
        console.warn(`    ⚠ Attempt ${attempt} failed (${msg}). Retrying in ${backoff}ms...`);
        if ((err as Error).message.includes('429')) {
             // Do not backoff locally for rate limits, global lock handles it
        } else {
             await new Promise((r) => setTimeout(r, backoff));
        }
      }
    }
    throw new Error('Unreachable');
  }

  return { complete };
}

export function promptHash(prompt: string): string {
  return createHash('sha256').update(prompt).digest('hex');
}
