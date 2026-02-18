// NOTE: Avoid overly generic keywords like "list" to prevent false positives
// from product/UX messages ("a list of commands") being misread as a style directive.
const CONTACT_STYLE_KEYWORD_RE = /(?:concise|short|brief|detailed|long|deep|bullet|direct|formal|professional|conversational|casual|chatty|back-and-forth)/iu;
const CONTACT_STYLE_DIRECTIVE_RE =
  /(?:talk|speak|communicate|respond|reply)\s+(?:to\s+me\s+)?(?:in|with|using)?\s*([^.!?\n]{3,100})|(?:keep|make)\s+(?:your\s+)?(?:responses|replies|messages)\s+([^.!?\n]{3,100})/giu;

function sanitizeEntity(value: string | null | undefined): string {
  return (value || '')
    .replace(/[\t\n\r]+/g, ' ')
    .replace(/\s{2,}/g, ' ')
    .replace(/["'`]+/g, '')
    .trim();
}

export function normalizeContactStyle(raw: string): string {
  const clean = sanitizeEntity(raw);
  if (!clean) return '';
  const lower = clean.toLowerCase();

  if (lower.includes('bullet')) return 'concise bullets';
  if (lower.includes('email')) return 'email';
  if (lower.includes('telegram') || lower.includes('dm')) return 'telegram dm';
  if (lower.includes('text') || lower.includes('sms')) return 'text';
  if (lower.includes('call') || lower.includes('phone')) return 'call';
  if (lower.includes('voice')) return 'voice notes';
  if (lower.includes('direct')) return 'direct';
  if (lower.includes('formal') || lower.includes('professional')) return 'formal and professional';
  if (
    lower.includes('conversational')
    || lower.includes('casual')
    || lower.includes('chatty')
    || lower.includes('back-and-forth')
  ) {
    return 'quick back-and-forth';
  }
  if (lower.includes('short') || lower.includes('concise') || lower.includes('brief')) return 'short messages';
  if (lower.includes('detailed') || lower.includes('long') || lower.includes('deep')) return 'detailed messages';
  return clean.toLowerCase();
}

export function extractContactStyleDirectives(source: string): string[] {
  const text = sanitizeEntity(source);
  if (!text) return [];
  const out = new Set<string>();

  for (const match of text.matchAll(CONTACT_STYLE_DIRECTIVE_RE)) {
    const candidate = sanitizeEntity((match[1] || match[2] || '').trim());
    if (!candidate) continue;
    if (!CONTACT_STYLE_KEYWORD_RE.test(candidate)) continue;
    const style = normalizeContactStyle(candidate);
    if (style) out.add(style);
  }

  // Handle short standalone directives: "bullets please", "be brief", "professional tone".
  // Guard: long UX/product feedback messages can contain keywords ("bullets", "brief") as examples,
  // which should NOT flip a user's stored contact style.
  const wordCount = text.split(/\s+/u).filter(Boolean).length;
  const isShort = text.length <= 80 && wordCount <= 8;
  const looksLikeStandaloneDirective = /^(?:bullets?|bullet points?|be\s+(?:brief|concise|short)|go\s+deep|detailed|professional(?:\s+tone)?|formal(?:\s+tone)?|direct|chatty|casual|conversational)\b/iu.test(text);
  if (out.size === 0 && CONTACT_STYLE_KEYWORD_RE.test(text) && (isShort || looksLikeStandaloneDirective)) {
    const style = normalizeContactStyle(text);
    if (style) out.add(style);
  }

  return [...out];
}
