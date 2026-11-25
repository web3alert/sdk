const inboxPrefixPattern = /^_INBOX(?:_[A-Za-z0-9_-]+)?$/;

export function isInboxPrefix(prefix: string): boolean {
  return typeof prefix === 'string' && inboxPrefixPattern.test(prefix) && !/\s/.test(prefix);
}

export function isInboxSubject(subject: string): boolean {
  const [prefix, ...tokens] = subject.split('.');
  return isInboxPrefix(prefix) && tokens.length > 0
    && tokens.every(token => token.length > 0 && !/[\s\x00-\x1f\x7f*>]/.test(token));
}
