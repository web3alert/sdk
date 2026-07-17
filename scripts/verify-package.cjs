const assert = require('node:assert/strict');
const fs = require('node:fs');
const Module = require('node:module');
const path = require('node:path');

const root = path.resolve(__dirname, '..');
const packagePath = path.join(root, 'package.json');
const lockPath = path.join(root, 'package-lock.json');
const packageJson = JSON.parse(fs.readFileSync(packagePath, 'utf8'));
const lockJson = JSON.parse(fs.readFileSync(lockPath, 'utf8'));
const forbiddenPackages = ['amqp-connection-manager', 'amqplib', '@types/amqplib'];

for (const section of ['dependencies', 'devDependencies', 'optionalDependencies', 'peerDependencies']) {
  const dependencies = packageJson[section] ?? {};
  for (const dependency of forbiddenPackages) {
    assert.equal(
      Object.hasOwn(dependencies, dependency),
      false,
      `${dependency} remains in package.json ${section}`,
    );
  }
}

for (const packageName of Object.keys(lockJson.packages ?? {})) {
  for (const dependency of forbiddenPackages) {
    assert.notEqual(
      packageName,
      `node_modules/${dependency}`,
      `${dependency} remains in package-lock.json`,
    );
  }
}

for (const directory of ['src/legacy', 'dist/legacy']) {
  assert.equal(fs.existsSync(path.join(root, directory)), false, `${directory} still exists`);
}

function sourceFiles(directory) {
  return fs.readdirSync(directory, { withFileTypes: true }).flatMap(entry => {
    const entryPath = path.join(directory, entry.name);
    return entry.isDirectory() ? sourceFiles(entryPath) : [entryPath];
  });
}

for (const directory of ['src', 'dist']) {
  for (const file of sourceFiles(path.join(root, directory))) {
    if (!/\.(?:ts|js|map)$/.test(file)) {
      continue;
    }
    const contents = fs.readFileSync(file, 'utf8');
    assert.equal(
      /amqp-connection-manager|amqplib|rabbitmq|bus-client/i.test(contents),
      false,
      `legacy transport reference remains in ${path.relative(root, file)}`,
    );
  }
}

const loadedRequests = [];
const originalLoad = Module._load;
Module._load = function(request, parent, isMain) {
  loadedRequests.push(request);
  return originalLoad.call(this, request, parent, isMain);
};

let sdk;
try {
  sdk = require(path.join(root, 'dist/index.js'));
} finally {
  Module._load = originalLoad;
}

assert.equal(
  loadedRequests.some(request => /amqp|rabbit/i.test(request)),
  false,
  `entrypoint loaded a legacy transport: ${loadedRequests.join(', ')}`,
);
assert.equal(typeof sdk.HttpAgent, 'function', 'HttpAgent is missing from the public entrypoint');

for (const exportName of ['BusClient', 'SourceBusClient', 'System', 'PollRunner', 'LegacySource']) {
  assert.equal(Object.hasOwn(sdk, exportName), false, `${exportName} remains publicly exported`);
}

console.log('SDK package contains only the NATS transport and supported public APIs.');
