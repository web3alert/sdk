const inboxPrefixPattern = /^_INBOX(?:_[A-Za-z0-9_-]+)?$/;
// NATS account service replies use two six-character tokens. Tracked replies
// append the server hash and the tracking marker.
const mappedReplyPattern = /^_R_\.[A-Za-z0-9]{6}\.[A-Za-z0-9]{6}(?:\.[A-Za-z0-9]{8}\.T)?$/;

export function isInboxPrefix(prefix: string): boolean {
  return typeof prefix === 'string' && inboxPrefixPattern.test(prefix) && !/\s/.test(prefix);
}

export function isReplySubject(subject: string): boolean {
  if (mappedReplyPattern.test(subject)) return true;
  const [prefix, ...tokens] = subject.split('.');
  return isInboxPrefix(prefix) && tokens.length > 0
    && tokens.every(token => token.length > 0 && !/[\s\x00-\x1f\x7f*>]/.test(token));
}
