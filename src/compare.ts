export type Diff = {
  same: string[];
  added: string[];
  removed: string[];
};

export function compare(prev: string[], next: string[]): Diff {
  const prevIndex = new Set(prev);
  const nextIndex = new Set(next);
  const same: string[] = [];
  
  for (const item of prev) {
    if (nextIndex.has(item)) {
      same.push(item);
      
      prevIndex.delete(item);
      nextIndex.delete(item);
    }
  }
  
  return {
    same,
    added: Array.from(nextIndex),
    removed: Array.from(prevIndex),
  };
}
