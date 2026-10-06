#!/usr/bin/env node
/**
 * vulntrace — cross-reference audit advisories against code that is actually reachable.
 *
 * `npm/yarn audit` reports every vulnerable package in the installed tree, including
 * ones that only exist to support the build, and ones whose installed version has
 * already been patched by a `resolutions` entry. This walks the real module graph with
 * @vercel/nft from the project's entry points, then reports each advisory with:
 *
 *   - whether the vulnerable version is actually installed
 *   - whether the vulnerable package is reachable from a runtime entry point, and which
 *   - a risk grade that folds severity together with how exposed that entry point is
 *   - the import chain(s) showing how the code is reached
 *
 * Usage:
 *   node bin/vulntrace.mjs --audit audit.json
 *   yarn audit --json > audit.json          # yarn 1.x (this repo)
 *   npm audit --json > audit.json           # npm (needs package-lock.json)
 *
 * Options:
 *   --audit <file>    Audit JSON to read. '-' reads stdin. Defaults to running the
 *                     audit itself, which needs network access to the registry.
 *   --entry <path>    Extra entry point (file or directory). Repeatable. Use
 *                     --entry <path>:<group> to tag it (group: web|cli|dev).
 *   --all             Also list advisories that are unreachable or not applicable.
 *   --paths <n>       Max import chains to print per advisory (default 2).
 *   --json            Emit machine-readable JSON instead of a report.
 *   --base <dir>      Project root (default: cwd).
 */

import {execFileSync} from 'node:child_process';
import {createRequire} from 'node:module';
import {readFileSync, readdirSync, statSync, existsSync} from 'node:fs';
import path from 'node:path';

const NM = 'node_modules/';

// ---------------------------------------------------------------------------
// Entry point groups.
//
// `exposure` drives the risk grade: how hostile is the input that reaches this
// code? 3 = remote/untrusted (anything serving HTTP or running in a browser),
// 1 = operator-run locally, 0 = build/test tooling only.
// ---------------------------------------------------------------------------

const ENTRY_GROUPS = [
    {
        id: 'web',
        label: 'Next.js pages + API routes (remote input, SSR & browser bundle)',
        exposure: 3,
        roots: ['.next/server/pages', '.next/server/app'],
        hint: 'no Next build found — run `yarn build` so web-facing code can be traced'
    },
    {
        id: 'cli',
        label: 'CLI tools in dist/bin (operator-run, local input)',
        exposure: 1,
        roots: ['dist/bin'],
        hint: 'no compiled CLI found — run `yarn bin:build` so CLI code can be traced'
    }
];

const SEVERITY_RANK = {critical: 4, high: 3, moderate: 2, medium: 2, low: 1, info: 0, none: 0};
const RISK_NAMES = ['NEGLIGIBLE', 'LOW', 'MEDIUM', 'HIGH', 'CRITICAL'];

// ---------------------------------------------------------------------------
// Argument parsing
// ---------------------------------------------------------------------------

function parseArgs(argv) {
    const opts = {audit: null, entries: [], all: false, paths: 2, json: false, base: process.cwd()};
    for (let i = 0; i < argv.length; i++) {
        const a = argv[i];
        if (a === '--audit') opts.audit = argv[++i];
        else if (a === '--entry') opts.entries.push(argv[++i]);
        else if (a === '--all') opts.all = true;
        else if (a === '--json') opts.json = true;
        else if (a === '--paths') opts.paths = Number(argv[++i]);
        else if (a === '--base') opts.base = path.resolve(argv[++i]);
        else if (a === '--help' || a === '-h') opts.help = true;
        else throw new Error(`unknown argument: ${a}`);
    }
    return opts;
}

// ---------------------------------------------------------------------------
// Dependency loading.
//
// Prefer a real @vercel/nft, but fall back to the copy Next.js vendors so this
// runs without adding a dependency. Same for semver.
// ---------------------------------------------------------------------------

function loadDeps(base) {
    const require = createRequire(path.join(base, 'noop.js'));
    let nft = null;
    let nftSource = null;
    for (const spec of ['@vercel/nft', 'next/dist/compiled/@vercel/nft/index.js']) {
        try {
            nft = require(spec);
            nftSource = spec;
            break;
        } catch {
            /* try next */
        }
    }
    if (!nft?.nodeFileTrace) {
        throw new Error('@vercel/nft not found. Install it (`yarn add -D @vercel/nft`) or run from a project with Next.js installed.');
    }
    let semver = null;
    try {
        semver = require('semver');
    } catch {
        /* version matching degrades to name-only */
    }
    return {nodeFileTrace: nft.nodeFileTrace, semver, nftSource};
}

// ---------------------------------------------------------------------------
// Entry discovery
// ---------------------------------------------------------------------------

const JS_EXT = new Set(['.js', '.mjs', '.cjs']);

function collectJsFiles(abs, out = []) {
    let st;
    try {
        st = statSync(abs);
    } catch {
        return out;
    }
    if (st.isFile()) {
        if (JS_EXT.has(path.extname(abs))) out.push(abs);
        return out;
    }
    if (!st.isDirectory()) return out;
    for (const e of readdirSync(abs, {withFileTypes: true})) {
        if (e.name === 'node_modules' || e.name.startsWith('.')) continue;
        collectJsFiles(path.join(abs, e.name), out);
    }
    return out;
}

function discoverEntries(base, extra) {
    const groups = [];
    for (const g of ENTRY_GROUPS) {
        const files = [];
        for (const root of g.roots) files.push(...collectJsFiles(path.join(base, root)));
        groups.push({...g, files: files.map((f) => path.relative(base, f))});
    }
    for (const spec of extra) {
        const sep = spec.lastIndexOf(':');
        const hasTag = sep > 1 && ['web', 'cli', 'dev'].includes(spec.slice(sep + 1));
        const target = hasTag ? spec.slice(0, sep) : spec;
        const tag = hasTag ? spec.slice(sep + 1) : 'cli';
        const files = collectJsFiles(path.resolve(base, target)).map((f) => path.relative(base, f));
        if (!files.length) {
            console.error(`warning: --entry ${spec} matched no .js/.mjs/.cjs files`);
            continue;
        }
        let group = groups.find((g) => g.id === tag);
        if (!group) {
            const exposure = tag === 'web' ? 3 : tag === 'cli' ? 1 : 0;
            group = {id: tag, label: `custom entries (${tag})`, exposure, files: []};
            groups.push(group);
        }
        group.files.push(...files);
    }
    return groups.filter((g) => g.files.length);
}

// ---------------------------------------------------------------------------
// Tracing
// ---------------------------------------------------------------------------

/** node_modules/foo/bar.js -> {name: 'foo', dir: 'node_modules/foo'} */
function packageOf(file) {
    const i = file.lastIndexOf(NM);
    if (i === -1) return null;
    const seg = file.slice(i + NM.length).split('/');
    const name = seg[0].startsWith('@') ? seg.slice(0, 2).join('/') : seg[0];
    return {name, dir: file.slice(0, i) + NM + name};
}

async function traceGroups(nodeFileTrace, base, groups) {
    const traced = new Map(); // package dir -> Set of group ids
    let warnings = 0;
    for (const g of groups) {
        const result = await nodeFileTrace(g.files, {base});
        g.trace = result;
        g.entrySet = new Set(g.files);
        warnings += result.warnings?.size ?? 0;
        for (const f of result.fileList) {
            const pkg = packageOf(f);
            if (!pkg) continue;
            if (!traced.has(pkg.dir)) traced.set(pkg.dir, new Set());
            traced.get(pkg.dir).add(g.id);
        }
    }
    return {traced, warnings};
}

/**
 * Shortest import chain from one of `entrySet` to any file inside `targetDir`.
 * nft's `reasons` maps each file to the files that imported it, so this is a
 * breadth-first walk backwards from the vulnerable package to an entry point.
 */
function chainToPackage(reasons, entrySet, targetDir) {
    const prefix = targetDir + '/';
    const inPackage = [...reasons.keys()].filter((f) => f.startsWith(prefix));

    const search = (starts) => {
        const queue = [...starts];
        const cameFrom = new Map(starts.map((f) => [f, null]));
        for (let head = 0; head < queue.length; head++) {
            const file = queue[head];
            if (entrySet.has(file)) {
                const chain = [];
                for (let c = file; c != null; c = cameFrom.get(c)) chain.push(c);
                return chain; // entry ... vulnerable file
            }
            const reason = reasons.get(file);
            if (!reason) continue;
            for (const parent of reason.parents) {
                if (cameFrom.has(parent)) continue;
                cameFrom.set(parent, file);
                queue.push(parent);
            }
        }
        return null;
    };

    // Prefer landing on real code: a chain ending at the package's own package.json
    // is usually shorter but says nothing about which code actually runs.
    const code = inPackage.filter((f) => JS_EXT.has(path.extname(f)));
    return (code.length && search(code)) || search(inPackage);
}

/** Collapse a file chain into one hop per package, keeping the file each hop enters at. */
function collapseChain(chain) {
    const hops = [];
    for (const file of chain) {
        const pkg = packageOf(file);
        const label = pkg ? pkg.name : file;
        const last = hops[hops.length - 1];
        if (last && last.label === label) continue;
        hops.push({label, file, isPackage: !!pkg});
    }
    return hops;
}

// ---------------------------------------------------------------------------
// Installed package index
// ---------------------------------------------------------------------------

function indexInstalled(base) {
    const index = new Map(); // name -> [{dir, version}]
    const walkTree = (nmAbs) => {
        let entries;
        try {
            entries = readdirSync(nmAbs, {withFileTypes: true});
        } catch {
            return;
        }
        for (const e of entries) {
            if (e.name === '.bin' || e.name.startsWith('.')) continue;
            const abs = path.join(nmAbs, e.name);
            if (e.name.startsWith('@')) {
                let scoped;
                try {
                    scoped = readdirSync(abs, {withFileTypes: true});
                } catch {
                    continue;
                }
                for (const s of scoped) if (!s.name.startsWith('.')) addPackage(path.join(abs, s.name));
                continue;
            }
            addPackage(abs);
        }
    };
    const addPackage = (abs) => {
        let manifest;
        try {
            manifest = JSON.parse(readFileSync(path.join(abs, 'package.json'), 'utf8'));
        } catch {
            return;
        }
        const name = manifest.name || path.relative(path.join(base, NM), abs).split(path.sep).join('/');
        if (!index.has(name)) index.set(name, []);
        index.get(name).push({dir: path.relative(base, abs).split(path.sep).join('/'), version: manifest.version ?? null});
        walkTree(path.join(abs, 'node_modules'));
    };
    walkTree(path.join(base, NM));
    return index;
}

// ---------------------------------------------------------------------------
// Audit input
// ---------------------------------------------------------------------------

function readAuditInput(base, auditArg) {
    if (auditArg === '-') return {text: readFileSync(0, 'utf8'), source: 'stdin'};
    if (auditArg) return {text: readFileSync(path.resolve(base, auditArg), 'utf8'), source: auditArg};

    const useYarn = existsSync(path.join(base, 'yarn.lock'));
    const cmd = useYarn ? ['yarn', ['audit', '--json']] : ['npm', ['audit', '--json']];
    try {
        // Both exit non-zero when they find vulnerabilities, so stdout is the payload either way.
        const text = execFileSync(cmd[0], cmd[1], {encoding: 'utf8', cwd: base, maxBuffer: 64 * 1024 * 1024, stdio: ['ignore', 'pipe', 'ignore']});
        return {text, source: `${cmd[0]} ${cmd[1].join(' ')}`};
    } catch (e) {
        const text = e.stdout;
        if (text && text.length > 200) return {text, source: `${cmd[0]} ${cmd[1].join(' ')}`};
        throw new Error(`could not run \`${cmd[0]} ${cmd[1].join(' ')}\` (it needs registry access).\n` + `Run it yourself and pass the result:\n` + `  ${cmd[0]} ${cmd[1].join(' ')} > audit.json\n` + `  node bin/vulntrace.mjs --audit audit.json`);
    }
}

function firstSentence(text, limit = 240) {
    if (!text) return null;
    const flat = text
        .replace(/\r/g, '')
        .replace(/^#+\s*/gm, '')
        .replace(/\s+/g, ' ')
        .trim();
    const cut = flat.search(/\.\s/);
    const out = cut > 40 ? flat.slice(0, cut + 1) : flat;
    return out.length > limit ? out.slice(0, limit - 1) + '…' : out;
}

/** npm audit --json (auditReportVersion 2) */
function parseNpmAudit(report) {
    const advisories = new Map();
    for (const [name, entry] of Object.entries(report.vulnerabilities ?? {})) {
        for (const via of entry.via ?? []) {
            // string entries mean "vulnerable via this dependency"; the advisory itself
            // is reported on that dependency's own record, so it is not lost here.
            if (typeof via !== 'object') continue;
            const key = String(via.source ?? `${via.name}:${via.title}`);
            if (!advisories.has(key)) {
                advisories.set(key, {
                    key,
                    module: via.name ?? name,
                    severity: via.severity ?? entry.severity,
                    title: via.title,
                    url: via.url,
                    cves: [],
                    cwe: via.cwe ?? [],
                    cvss: via.cvss?.score ?? null,
                    range: via.range ?? entry.range,
                    patched: null,
                    overview: null,
                    recommendation: typeof entry.fixAvailable === 'object' ? `Upgrade to ${entry.fixAvailable.name}@${entry.fixAvailable.version}` : null,
                    fixAvailable: !!entry.fixAvailable,
                    fixIsMajor: typeof entry.fixAvailable === 'object' ? !!entry.fixAvailable.isSemVerMajor : false,
                    devOnly: null, // npm's report does not mark this per-advisory
                    depPaths: new Set(),
                    nodes: new Set()
                });
            }
            const adv = advisories.get(key);
            for (const n of entry.nodes ?? []) if (packageOf(n + '/x')?.name === adv.module) adv.nodes.add(n);
            if (entry.isDirect && name === adv.module) adv.depPaths.add(adv.module);
        }
    }
    return [...advisories.values()];
}

/** yarn 1.x `yarn audit --json` — newline-delimited records */
function parseYarnAudit(lines) {
    const advisories = new Map();
    for (const line of lines) {
        let record;
        try {
            record = JSON.parse(line);
        } catch {
            continue;
        }
        if (record.type !== 'auditAdvisory') continue;
        const {advisory, resolution} = record.data;
        const key = String(advisory.github_advisory_id || advisory.id);
        if (!advisories.has(key)) {
            advisories.set(key, {
                key,
                module: advisory.module_name,
                severity: advisory.severity,
                title: advisory.title,
                url: advisory.url,
                cves: advisory.cves ?? [],
                cwe: Array.isArray(advisory.cwe) ? advisory.cwe : advisory.cwe ? [advisory.cwe] : [],
                cvss: advisory.cvss?.score ?? null,
                range: advisory.vulnerable_versions,
                patched: advisory.patched_versions,
                overview: firstSentence(advisory.overview),
                recommendation: firstSentence(advisory.recommendation, 160),
                fixAvailable: !!advisory.patched_versions && advisory.patched_versions !== '<0.0.0',
                fixIsMajor: false,
                devOnly: true,
                depPaths: new Set(),
                nodes: new Set()
            });
        }
        const adv = advisories.get(key);
        if (resolution && !resolution.dev) adv.devOnly = false;
        if (resolution?.path) adv.depPaths.add(resolution.path);
        for (const finding of advisory.findings ?? []) for (const p of finding.paths ?? []) adv.depPaths.add(p);
    }
    return [...advisories.values()];
}

function parseAudit(text) {
    const trimmed = text.trim();
    if (!trimmed) throw new Error('audit input was empty');
    if (trimmed.startsWith('{') && trimmed.includes('"vulnerabilities"') && !trimmed.includes('"type":"auditAdvisory"')) {
        try {
            const report = JSON.parse(trimmed);
            if (report.vulnerabilities && !Array.isArray(report.vulnerabilities)) return {advisories: parseNpmAudit(report), format: 'npm audit'};
        } catch {
            /* fall through to NDJSON */
        }
    }
    const lines = trimmed.split('\n').filter(Boolean);
    const advisories = parseYarnAudit(lines);
    if (!advisories.length && !lines.some((l) => l.includes('auditAdvisory'))) {
        throw new Error('could not recognise the audit format (expected `npm audit --json` or `yarn audit --json` output)');
    }
    return {advisories, format: 'yarn audit'};
}

// ---------------------------------------------------------------------------
// Correlation
// ---------------------------------------------------------------------------

function gradeRisk(advisory, exposure) {
    // Start from severity, then shift by how exposed the reachable entry point is.
    // Code only reachable from operator-run CLI tools is a step down from code
    // serving HTTP; code not reachable at runtime at all is build-time only.
    let score = SEVERITY_RANK[advisory.severity] ?? 2;
    if (exposure >= 3) score += 0;
    else if (exposure === 1) score -= 1;
    else score -= 2;
    return Math.max(0, Math.min(4, score));
}

function correlate(advisories, {traced, groups, installed, semver}) {
    const groupById = new Map(groups.map((g) => [g.id, g]));
    const results = [];

    for (const advisory of advisories) {
        const copies = installed.get(advisory.module) ?? [];

        // Which installed copies are actually in the vulnerable version range?
        const affected = copies.filter((c) => {
            if (!advisory.range || !semver || !c.version) return true; // cannot narrow it down, assume affected
            try {
                return semver.satisfies(c.version, advisory.range, {includePrerelease: true});
            } catch {
                return true;
            }
        });

        const reached = [];
        for (const copy of affected) {
            const hitGroups = traced.get(copy.dir);
            if (!hitGroups) continue;
            for (const id of hitGroups) reached.push({copy, group: groupById.get(id)});
        }

        let status;
        if (!copies.length) status = 'not-installed';
        else if (!affected.length) status = 'version-not-affected';
        else if (!reached.length) status = 'not-reachable';
        else status = 'reachable';

        const exposure = reached.length ? Math.max(...reached.map((r) => r.group.exposure)) : 0;
        const risk = status === 'reachable' ? gradeRisk(advisory, exposure) : 0;

        // Build one import chain per reached group, cheapest evidence first.
        const seenGroups = new Set();
        const paths = [];
        for (const {copy, group} of reached) {
            if (seenGroups.has(group.id)) continue;
            const chain = chainToPackage(group.trace.reasons, group.entrySet, copy.dir);
            if (!chain) continue;
            seenGroups.add(group.id);
            paths.push({group, copy, hops: collapseChain(chain), files: chain.length});
        }

        results.push({advisory, status, risk, exposure, copies, affected, reached, paths});
    }

    results.sort((a, b) => {
        const order = {reachable: 0, 'not-reachable': 1, 'version-not-affected': 2, 'not-installed': 3};
        if (order[a.status] !== order[b.status]) return order[a.status] - order[b.status];
        if (b.risk !== a.risk) return b.risk - a.risk;
        return (SEVERITY_RANK[b.advisory.severity] ?? 0) - (SEVERITY_RANK[a.advisory.severity] ?? 0);
    });
    return results;
}

// ---------------------------------------------------------------------------
// Reporting
// ---------------------------------------------------------------------------

const useColour = process.stdout.isTTY && !process.env.NO_COLOR;
const paint = (code, s) => (useColour ? `[${code}m${s}[0m` : s);
const bold = (s) => paint('1', s);
const dim = (s) => paint('2', s);
const RISK_COLOUR = {CRITICAL: '1;31', HIGH: '31', MEDIUM: '33', LOW: '36', NEGLIGIBLE: '2'};

function report(results, context, opts) {
    const {groups, warnings, auditSource, auditFormat, nftSource} = context;
    const reachable = results.filter((r) => r.status === 'reachable');
    const other = results.filter((r) => r.status !== 'reachable');

    console.log(bold('\nvulntrace') + dim(`  ·  ${auditFormat} via ${auditSource}  ·  nft: ${nftSource}`));
    console.log(dim('─'.repeat(78)));

    console.log(bold('Traced entry points'));
    for (const g of groups) {
        console.log(`  ${g.id.padEnd(4)} ${String(g.files.length).padStart(3)} entries   ${g.label}`);
    }
    for (const g of ENTRY_GROUPS) {
        if (!groups.some((x) => x.id === g.id)) console.log(`  ${dim(g.id.padEnd(4) + '   —  ' + g.hint)}`);
    }

    const byRisk = {};
    for (const r of reachable) byRisk[RISK_NAMES[r.risk]] = (byRisk[RISK_NAMES[r.risk]] ?? 0) + 1;
    const counts = RISK_NAMES.slice()
        .reverse()
        .filter((n) => byRisk[n])
        .map((n) => `${byRisk[n]} ${paint(RISK_COLOUR[n], n.toLowerCase())}`)
        .join(', ');

    console.log(`\n${bold('Summary')}  ${results.length} advisories reported · ` + `${bold(String(reachable.length))} reachable from traced code` + (counts ? ` (${counts})` : '') + `\n         ${other.filter((r) => r.status === 'not-reachable').length} installed but never imported · ` + `${other.filter((r) => r.status === 'version-not-affected').length} installed version already patched · ` + `${other.filter((r) => r.status === 'not-installed').length} not installed`);

    if (!reachable.length) {
        console.log(dim('\nNothing reachable from the traced entry points.'));
    }

    const shownPaths = new Map(); // chain signature -> advisory number it was printed under
    for (const [i, result] of reachable.entries()) {
        const {advisory} = result;
        const riskName = RISK_NAMES[result.risk];
        const versions = [...new Set(result.affected.map((c) => c.version))].join(', ');
        console.log(dim('\n' + '─'.repeat(78)));
        console.log(`${bold(`[${i + 1}]`)} ${paint(RISK_COLOUR[riskName], bold(riskName + ' RISK'))}  ${bold(`${advisory.module}@${versions}`)}  ${dim(`${advisory.severity} severity`)}`);
        console.log(`    ${advisory.title ?? '(no title)'}`);
        if (advisory.overview) console.log(dim(`    ${advisory.overview}`));

        const ids = [advisory.key, ...advisory.cves].filter(Boolean).join(' ');
        console.log(dim(`    ${ids}${advisory.cvss ? `  cvss ${advisory.cvss}` : ''}${advisory.url ? `  ${advisory.url}` : ''}`));
        console.log(`    ${dim('vulnerable:')} ${advisory.range ?? '?'}   ${dim('fix:')} ${advisory.patched ?? (advisory.fixAvailable ? 'available' : 'none published')}`);
        if (advisory.recommendation) console.log(`    ${dim('advice:')} ${advisory.recommendation}`);

        const why = result.exposure >= 3 ? 'reachable from code that handles remote requests' : 'reachable only from locally-run tooling';
        console.log(`    ${dim('risk:')} ${advisory.severity} severity, ${why}`);

        console.log(`\n    ${bold('Reached from:')}`);
        for (const p of result.paths.slice(0, opts.paths)) {
            // Several advisories commonly land on the same package (Next.js alone
            // accounts for a dozen), so print each distinct chain once and refer back.
            const signature = p.group.id + '|' + p.hops.map((h) => h.file).join('>');
            const shownAt = shownPaths.get(signature);
            if (shownAt) {
                console.log(`      ${p.group.id} — ${dim(`same path as [${shownAt}]`)}`);
                continue;
            }
            shownPaths.set(signature, i + 1);
            console.log(`      ${p.group.id} — ${dim(p.group.label)}`);
            p.hops.forEach((hop, depth) => {
                const indent = '        ' + '  '.repeat(Math.min(depth, 8));
                const arrow = depth === 0 ? '' : '└─ ';
                const detail = hop.isPackage ? dim(`  (${hop.file})`) : '';
                console.log(`${indent}${arrow}${hop.isPackage ? bold(hop.label) : hop.label}${detail}`);
            });
        }
        const hidden = result.paths.length - opts.paths;
        if (hidden > 0) console.log(dim(`      … and ${hidden} more entry group(s)`));
        if (!result.paths.length) console.log(dim('      (reachable, but no import chain could be reconstructed)'));
    }

    if (opts.all && other.length) {
        console.log(dim('\n' + '─'.repeat(78)));
        console.log(bold('Not applicable\n'));
        const reason = {
            'not-reachable': 'installed but not imported by any traced entry point (build/test only)',
            'version-not-affected': 'installed version is outside the vulnerable range',
            'not-installed': 'package not present in node_modules'
        };
        for (const r of other) {
            const v = r.copies.map((c) => c.version).join(', ') || '—';
            console.log(`  ${dim(r.advisory.severity.padEnd(8))} ${r.advisory.module}@${v}  ${dim(reason[r.status])}`);
        }
    } else if (other.length) {
        console.log(dim(`\n${other.length} advisories filtered out as not applicable — re-run with --all to see them.`));
    }

    if (warnings) {
        console.log(dim(`\nNote: nft reported ${warnings} unresolved dynamic import(s). Static tracing can miss edges built at runtime, so treat "not reachable" as strong evidence, not proof.`));
    }
    console.log('');
}

function toJson(results, context) {
    return {
        generatedFrom: {audit: context.auditSource, format: context.auditFormat, nft: context.nftSource},
        entryGroups: context.groups.map((g) => ({id: g.id, label: g.label, exposure: g.exposure, entries: g.files})),
        traceWarnings: context.warnings,
        advisories: results.map((r) => ({
            id: r.advisory.key,
            module: r.advisory.module,
            severity: r.advisory.severity,
            risk: RISK_NAMES[r.risk],
            status: r.status,
            title: r.advisory.title,
            summary: r.advisory.overview,
            url: r.advisory.url,
            cves: r.advisory.cves,
            vulnerableRange: r.advisory.range,
            patchedVersions: r.advisory.patched,
            recommendation: r.advisory.recommendation,
            installedVersions: r.copies.map((c) => ({dir: c.dir, version: c.version})),
            affectedVersions: r.affected.map((c) => ({dir: c.dir, version: c.version})),
            reachedFrom: r.paths.map((p) => ({
                group: p.group.id,
                exposure: p.group.exposure,
                entry: p.hops[0]?.file,
                chain: p.hops.map((h) => ({package: h.isPackage ? h.label : null, file: h.file}))
            })),
            dependencyPaths: [...r.advisory.depPaths].slice(0, 20)
        }))
    };
}

// ---------------------------------------------------------------------------

async function main() {
    const opts = parseArgs(process.argv.slice(2));
    if (opts.help) {
        const doc = readFileSync(new URL(import.meta.url), 'utf8').split('*/')[0];
        console.log(
            doc
                .replace(/^#![^\n]*\n/, '')
                .replace(/^\/\*\*\n/, '')
                .replace(/^ \* ?/gm, '')
                .trim()
        );
        return;
    }

    const {nodeFileTrace, semver, nftSource} = loadDeps(opts.base);
    if (!semver) console.error('warning: semver not found — advisories cannot be filtered by installed version');

    const groups = discoverEntries(opts.base, opts.entries);
    if (!groups.length) {
        throw new Error('no entry points found. Build the project (`yarn build`) or pass --entry <path>.');
    }

    const {text, source} = readAuditInput(opts.base, opts.audit);
    const {advisories, format} = parseAudit(text);

    const {traced, warnings} = await traceGroups(nodeFileTrace, opts.base, groups);
    const installed = indexInstalled(opts.base);
    const results = correlate(advisories, {traced, groups, installed, semver});

    const context = {groups, warnings, auditSource: source, auditFormat: format, nftSource};
    if (opts.json) console.log(JSON.stringify(toJson(results, context), null, 2));
    else report(results, context, opts);

    // Non-zero when something reachable was found, so this can gate CI.
    process.exitCode = results.some((r) => r.status === 'reachable' && r.risk >= 3) ? 1 : 0;
}

main().catch((e) => {
    console.error(`vulntrace: ${e.message}`);
    process.exitCode = 2;
});
