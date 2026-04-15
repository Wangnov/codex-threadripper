#!/usr/bin/env node

import { cpSync, existsSync, mkdirSync, mkdtempSync, readFileSync, readdirSync, rmSync, writeFileSync, chmodSync } from "node:fs";
import { join, dirname } from "node:path";
import { tmpdir } from "node:os";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";

const scriptDir = dirname(fileURLToPath(import.meta.url));
const repoRoot = dirname(scriptDir);
const config = JSON.parse(readFileSync(join(scriptDir, "npm-matrix-config.json"), "utf8"));

const args = new Map();
for (let index = 2; index < process.argv.length; index += 2) {
  const key = process.argv[index];
  const value = process.argv[index + 1];
  if (!key?.startsWith("--") || value === undefined) {
    console.error(`Unexpected argument sequence near ${key ?? "<end>"}`);
    process.exit(1);
  }
  args.set(key.slice(2), value);
}

const version = args.get("version");
const releaseTag = args.get("release-tag");
const artifactsDir = args.get("artifacts-dir");
const outputDir = args.get("output-dir");

if (!version || !releaseTag || !artifactsDir || !outputDir) {
  console.error("Usage: node scripts/build-npm-matrix.mjs --version <version> --release-tag <tag> --artifacts-dir <dir> --output-dir <dir>");
  process.exit(1);
}

const manifest = {
  version,
  releaseTag,
  rootPackage: null,
  platformPackages: [],
};

const repository = {
  type: "git",
  url: `git+${config.repositoryUrl}.git`,
};

const rootReadme = `# ${config.rootPackageName}

This package is the npm entrypoint for \`${config.binaryName}\`.

It installs the matching prebuilt platform package for your machine and runs the native binary from there.

Published from release \`${releaseTag}\`.
`;

const rootBin = `#!/usr/bin/env node

const { spawnSync } = require("node:child_process");
const { dirname, join } = require("node:path");

const platformPackages = {
${config.platformPackages
  .map((pkg) => `  "${pkg.os[0]}:${pkg.cpu[0]}": { packageName: "${pkg.packageName}", binaryName: "${pkg.binaryName}" }`)
  .join(",\n")}
};

const supported = Object.keys(platformPackages);
const key = \`\${process.platform}:\${process.arch}\`;
const selected = platformPackages[key];

if (!selected) {
  console.error(\`codex-threadripper is available for: \${supported.join(", ")}\`);
  process.exit(1);
}

let packageJsonPath;
try {
  packageJsonPath = require.resolve(\`\${selected.packageName}/package.json\`);
} catch (error) {
  console.error(\`Missing optional dependency \${selected.packageName}. Reinstall without skipping optional dependencies.\`);
  process.exit(1);
}

const binaryPath = join(dirname(packageJsonPath), "bin", selected.binaryName);
const result = spawnSync(binaryPath, process.argv.slice(2), { stdio: "inherit" });

if (result.error) {
  console.error(result.error.message);
  process.exit(1);
}

process.exit(result.status ?? 1);
`;

const packageJsonCommon = {
  author: config.author,
  license: config.license,
  repository,
  homepage: config.homepage,
};

rmSync(outputDir, { recursive: true, force: true });
mkdirSync(outputDir, { recursive: true });

for (const platformPackage of config.platformPackages) {
  const packageDir = join(outputDir, platformPackage.dirName);
  mkdirSync(join(packageDir, "bin"), { recursive: true });

  const artifactPath = join(artifactsDir, platformPackage.artifactName);
  if (!existsSync(artifactPath)) {
    console.error(`Missing artifact: ${artifactPath}`);
    process.exit(1);
  }

  const binaryPath = extractBinary(artifactPath, platformPackage.binaryName);
  const destinationBinaryPath = join(packageDir, "bin", platformPackage.binaryName);
  cpSync(binaryPath, destinationBinaryPath);
  if (!platformPackage.binaryName.endsWith(".exe")) {
    chmodSync(destinationBinaryPath, 0o755);
  }

  writeFileSync(
    join(packageDir, "package.json"),
    `${JSON.stringify(
      {
        name: platformPackage.packageName,
        version,
        description: platformPackage.description,
        ...packageJsonCommon,
        os: platformPackage.os,
        cpu: platformPackage.cpu,
        files: ["bin", "README.md"],
      },
      null,
      2,
    )}\n`,
  );

  writeFileSync(
    join(packageDir, "README.md"),
    `# ${platformPackage.packageName}

${platformPackage.description}

Published from release \`${releaseTag}\`.
`,
  );

  manifest.platformPackages.push({
    dir: packageDir,
    packageName: platformPackage.packageName,
  });
}

const rootPackageDir = join(outputDir, "root");
mkdirSync(join(rootPackageDir, "bin"), { recursive: true });
writeFileSync(
  join(rootPackageDir, "package.json"),
  `${JSON.stringify(
    {
      name: config.rootPackageName,
      version,
      description: config.rootPackageDescription,
      ...packageJsonCommon,
      keywords: config.keywords,
      files: ["bin", "README.md"],
      bin: {
        [config.binaryName]: "bin/codex-threadripper.js",
      },
      optionalDependencies: Object.fromEntries(
        config.platformPackages.map((pkg) => [pkg.packageName, version]),
      ),
    },
    null,
    2,
  )}\n`,
);
writeFileSync(join(rootPackageDir, "README.md"), rootReadme);
writeFileSync(join(rootPackageDir, "bin", "codex-threadripper.js"), rootBin);
chmodSync(join(rootPackageDir, "bin", "codex-threadripper.js"), 0o755);

manifest.rootPackage = {
  dir: rootPackageDir,
  packageName: config.rootPackageName,
};

writeFileSync(join(outputDir, "manifest.json"), `${JSON.stringify(manifest, null, 2)}\n`);

function extractBinary(artifactPath, binaryName) {
  const tempDir = mkdtempSync(join(tmpdir(), "codex-threadripper-npm-"));
  const extractDir = join(tempDir, "extract");
  mkdirSync(extractDir, { recursive: true });

  const extractCommand =
    artifactPath.endsWith(".zip")
      ? { command: "unzip", args: ["-q", artifactPath, "-d", extractDir] }
      : { command: "tar", args: ["xf", artifactPath, "-C", extractDir] };

  const result = spawnSync(extractCommand.command, extractCommand.args, { stdio: "pipe" });
  if (result.status !== 0) {
    console.error(result.stderr.toString() || `Failed to extract ${artifactPath}`);
    process.exit(1);
  }

  const discoveredBinary = findFile(extractDir, binaryName);
  if (!discoveredBinary) {
    console.error(`Could not find ${binaryName} inside ${artifactPath}`);
    process.exit(1);
  }

  return discoveredBinary;
}

function findFile(directory, fileName) {
  for (const entry of readdirSync(directory, { withFileTypes: true })) {
    const entryPath = join(directory, entry.name);
    if (entry.isFile() && entry.name === fileName) {
      return entryPath;
    }
    if (entry.isDirectory()) {
      const nestedPath = findFile(entryPath, fileName);
      if (nestedPath) {
        return nestedPath;
      }
    }
  }
  return null;
}
