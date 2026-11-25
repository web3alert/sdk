import { isIP } from 'node:net';

type Address = { family: 4 | 6; value: bigint };
type Cidr = Address & { prefix: number; shift: bigint };

// These ranges can be opened explicitly for local and private test networks.
const privateNetworks = [
  '10.0.0.0/8', '100.64.0.0/10', '127.0.0.0/8', '169.254.0.0/16',
  '172.16.0.0/12', '192.168.0.0/16',
  '::1/128', 'fc00::/7', 'fe80::/10',
].map(parseCidr);

const specialNetworks = [
  '0.0.0.0/8',          // This network, including the unspecified address
  '168.63.129.16/32',   // Azure platform virtual IP
  '192.0.0.0/24',       // IETF protocol assignments
  '192.0.2.0/24',       // TEST-NET-1
  '192.31.196.0/24',    // AS112
  '192.52.193.0/24',    // AMT
  '192.88.99.0/24',     // Deprecated 6to4 relay
  '192.175.48.0/24',    // AS112
  '198.18.0.0/15',      // Benchmarking
  '198.51.100.0/24',    // TEST-NET-2
  '203.0.113.0/24',     // TEST-NET-3
  '224.0.0.0/4',        // Multicast
  '240.0.0.0/4',        // Reserved, including limited broadcast
  '2001::/23',          // IETF assignments, including Teredo, benchmarking and ORCHID
  '2001:db8::/32',      // Documentation
  '2002::/16',          // 6to4
  '2620:4f:8000::/48',  // AS112
  '3fff::/20',          // Documentation
].map(parseCidr);

// Excludes unspecified, IPv4-compatible, discard, site-local, multicast and NAT64
// addresses (including 64:ff9b::/96 and 64:ff9b:1::/48). Mapped IPv4 is checked below.
const globalUnicastV6 = parseCidr('2000::/3');

function parseAddress(input: string): Address | undefined {
  const address = input.startsWith('[') && input.endsWith(']') ? input.slice(1, -1) : input;
  const family = isIP(address);
  if (family === 4 && address === input) {
    const value = address.split('.').reduce((result, octet) => result * 256n + BigInt(octet), 0n);
    return { family, value };
  }
  if (family !== 6 || address.includes('%')) return undefined;

  // WHATWG serialization converts embedded dotted IPv4 to hexadecimal groups.
  const groups = new URL(`http://[${address}]/`).hostname.slice(1, -1);
  const [start, end] = groups.split('::');
  const leading = start ? start.split(':') : [];
  const trailing = end ? end.split(':') : [];
  const expanded = end === undefined
    ? leading
    : [...leading, ...Array<string>(8 - leading.length - trailing.length).fill('0'), ...trailing];
  const value = expanded.reduce((result, group) => (result << 16n) + BigInt(`0x${group}`), 0n);
  return { family, value };
}

function parseCidr(input: string): Cidr {
  const [literal, length, extra] = input.split('/');
  const address = parseAddress(literal);
  const bits = address?.family === 4 ? 32 : 128;
  const prefix = Number(length);
  if (!address || extra !== undefined || !/^(0|[1-9]\d*)$/.test(length) || prefix > bits) {
    throw new TypeError('Invalid outbound CIDR');
  }
  const shift = BigInt(bits - prefix);
  if ((address.value >> shift) << shift !== address.value) {
    throw new TypeError('Invalid outbound CIDR');
  }
  return { ...address, prefix, shift };
}

function contains(network: Cidr, address: Address): boolean {
  return network.family === address.family && network.value >> network.shift === address.value >> network.shift;
}

export function createAddressCheck(allowedPrivateCidrs: readonly string[] = []): (input: string) => boolean {
  const exceptions = allowedPrivateCidrs.map(input => {
    const network = parseCidr(input);
    if (!privateNetworks.some(base => network.prefix >= base.prefix && contains(base, network))) {
      throw new TypeError('Outbound CIDR exceptions must be within a private network');
    }
    return network;
  });

  return input => {
    let address = parseAddress(input);
    if (!address) return false;
    if (address.family === 6 && address.value >> 32n === 0xffffn) {
      address = { family: 4, value: address.value & 0xffffffffn };
    }
    if (exceptions.some(network => contains(network, address))) return true;

    return (address.family === 4 || contains(globalUnicastV6, address))
      && !privateNetworks.some(network => contains(network, address))
      && !specialNetworks.some(network => contains(network, address));
  };
}

const checkPublicAddress = createAddressCheck();

/** Check an IP literal (standard IPv4 or IPv6, with optional IPv6 brackets). */
export function isOutboundAddressAllowed(address: string, allowedPrivateCidrs?: readonly string[]): boolean {
  const check = allowedPrivateCidrs?.length ? createAddressCheck(allowedPrivateCidrs) : checkPublicAddress;
  return check(address);
}
