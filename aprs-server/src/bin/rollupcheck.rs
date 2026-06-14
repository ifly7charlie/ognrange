//! Cross-checks rollup consistency using the on-disk Arrow output files.
//!
//! Each accumulator period writes a snapshot Arrow file per station/layer:
//!   {station}.{day|month|year}.{file_id}{.layer}.arrow.gz
//! with file_id "YYYY-MM-DD" (day), "YYYY-MM" (month), "YYYY" (year).
//!
//! Because day, month and year all accumulate from the same `current`
//! observations, a month snapshot should equal the merge of every day in
//! that month, and a year snapshot the merge of every month in that year.
//! This tool sums the per-H3 `count` of the children and diffs it against the
//! parent. The interesting direction is CHILD-EXCESS (children hold more than
//! the parent) -> the parent under-counted, the signature of a dropped-read in
//! the rollup. PARENT-EXCESS (parent holds more) is usually benign: old child
//! files get pruned, so the children are simply incomplete.
//!
//! Only the primary packet `count` per H3 cell is compared (not the signal /
//! altitude aggregates, and not the global station map).
//!
//! Usage: rollupcheck [station] [--month-only] [--year-only] [--layer=NAME]
//!                    [--all] [--sample[=PCT]] [--limit=N]
//!   --month-only  only check day -> month   (default: both levels)
//!   --year-only   only check month -> year
//!   --layer=NAME  only this layer (combined, flarm, ...); default: all
//!   --period=P    only parents whose file_id starts with P, e.g.
//!                 --period=2025-01 checks the Jan-2025 month vs its days;
//!                 --period=2025 checks every 2025 month and the 2025 year.
//!   --all         print every parent, not just the mismatching ones
//!   --sample[=P]  randomly sample P% of the station dirs (default 5%); only
//!                 applies when no station is named. Use for a quick fleet read.
//!   --limit=N     only the first N station dirs (after sorting; not random)
//!   station       only this station
//!
//! Mismatching parents are buffered and printed sorted worst-first by `disc%`
//! (the absolute per-H3 count difference as a fraction of the parent's total),
//! so the worst offenders float to the top. Progress prints to stderr; results
//! to stdout. Read-only: opens no DB and writes nothing.

use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, Write};
use std::time::Instant;

use arrow::array::{Array, UInt32Array};
use arrow::ipc::reader::StreamReader;
use flate2::read::GzDecoder;

/// One accumulator snapshot file on disk.
struct Snap {
    layer: String,
    acc_type: String, // "day" | "month" | "year"
    file_id: String,  // "2026-06-14" | "2026-06" | "2026"
    path: String,
}

/// Parse `{station}.{acc_type}.{file_id}{.layer}.arrow.gz` into a Snap.
///
/// The station name may itself contain dots, so we anchor on the known
/// acc_type token rather than splitting from the left. file_id never contains
/// a dot (dashes only), so the part after the acc_type splits cleanly into
/// file_id and the optional layer suffix.
fn parse_snap(file_name: &str, full_path: &str) -> Option<Snap> {
    let stem = file_name.strip_suffix(".arrow.gz")?;
    for acc in ["day", "month", "year"] {
        let needle = format!(".{}.", acc);
        if let Some(pos) = stem.find(&needle) {
            let rest = &stem[pos + needle.len()..];
            let mut parts = rest.splitn(2, '.');
            let file_id = parts.next()?.to_string();
            // empty file_id would mean a "latest" pointer with no date; skip.
            if file_id.is_empty() {
                return None;
            }
            let layer = parts.next().unwrap_or("combined").to_string();
            return Some(Snap {
                layer,
                acc_type: acc.to_string(),
                file_id,
                path: full_path.to_string(),
            });
        }
    }
    None
}

/// Sum the per-H3 `count` column across all batches of one Arrow file, keyed
/// by the (h3lo, h3hi) index pair.
fn read_counts(path: &str) -> Option<HashMap<(u32, u32), u64>> {
    let file = File::open(path).ok()?;
    let reader = StreamReader::try_new(GzDecoder::new(BufReader::new(file)), None).ok()?;
    let mut out: HashMap<(u32, u32), u64> = HashMap::new();
    for batch in reader {
        let batch = batch.ok()?;
        let lo = batch.column_by_name("h3lo")?.as_any().downcast_ref::<UInt32Array>()?;
        let hi = batch.column_by_name("h3hi")?.as_any().downcast_ref::<UInt32Array>()?;
        let cnt = batch.column_by_name("count")?.as_any().downcast_ref::<UInt32Array>()?;
        for i in 0..batch.num_rows() {
            *out.entry((lo.value(i), hi.value(i))).or_insert(0) += cnt.value(i) as u64;
        }
    }
    Some(out)
}

/// Merge several children's count maps into one (per-cell sum).
fn merge(children: &[HashMap<(u32, u32), u64>]) -> HashMap<(u32, u32), u64> {
    let mut out: HashMap<(u32, u32), u64> = HashMap::new();
    for c in children {
        for (&k, &v) in c {
            *out.entry(k).or_insert(0) += v;
        }
    }
    out
}

/// Outcome of comparing one parent against its merged children.
struct Diff {
    parent_cells: usize,
    parent_count: u64,
    child_files: usize,
    child_span: (String, String),
    child_cells: usize,
    child_count: u64,
    child_only_cells: usize, // cells in children but not parent
    parent_only_cells: usize,
    child_excess: u64, // sum of max(0, child - parent) -> parent under-counted
    parent_excess: u64,
}

impl Diff {
    /// The suspicious direction: children hold coverage the parent dropped.
    fn suspicious(&self) -> bool {
        self.child_excess > 0 || self.child_only_cells > 0
    }
    fn clean(&self) -> bool {
        self.child_excess == 0
            && self.parent_excess == 0
            && self.child_only_cells == 0
            && self.parent_only_cells == 0
    }
    /// Absolute per-H3 count difference (child_excess and parent_excess are
    /// disjoint per cell) as a percentage of the larger total. A single
    /// magnitude for ranking; the direction is in the child/parent_excess
    /// fields.
    fn disc_pct(&self) -> f64 {
        let denom = self.parent_count.max(self.child_count).max(1) as f64;
        (self.child_excess + self.parent_excess) as f64 * 100.0 / denom
    }
}

/// Minimal xorshift64 PRNG (seeded from the clock) so `--sample` is random
/// without pulling in the `rand` crate.
fn xorshift(state: &mut u64) -> u64 {
    let mut x = *state;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    *state = x;
    x
}

fn compare(parent: &HashMap<(u32, u32), u64>, child: &HashMap<(u32, u32), u64>) -> (usize, usize, u64, u64) {
    let mut child_only = 0usize;
    let mut parent_only = 0usize;
    let mut child_excess = 0u64;
    let mut parent_excess = 0u64;
    for (k, &cv) in child {
        match parent.get(k) {
            None => {
                child_only += 1;
                child_excess += cv;
            }
            Some(&pv) => {
                if cv > pv {
                    child_excess += cv - pv;
                } else {
                    parent_excess += pv - cv;
                }
            }
        }
    }
    for (k, &pv) in parent {
        if !child.contains_key(k) {
            parent_only += 1;
            parent_excess += pv; // whole-cell deficit: parent has it, no child does
        }
    }
    (child_only, parent_only, child_excess, parent_excess)
}

fn output_path() -> String {
    let raw = std::env::var("OUTPUT_PATH").unwrap_or_else(|_| "./data".to_string());
    if raw.ends_with('/') { raw } else { format!("{}/", raw) }
}

fn main() {
    let _ = dotenvy::from_filename(".env.local");
    let args: Vec<String> = std::env::args().collect();
    let filter: Option<String> = args.iter().skip(1).find(|a| !a.starts_with('-')).cloned();
    let month_only = args.iter().any(|a| a == "--month-only");
    let year_only = args.iter().any(|a| a == "--year-only");
    let show_all = args.iter().any(|a| a == "--all");
    let layer_filter: Option<String> =
        args.iter().find_map(|a| a.strip_prefix("--layer=").map(|s| s.to_string()));
    // --period=P: only check parents whose file_id starts with P (e.g. a single
    // month "2025-01", or a whole year "2025").
    let period: Option<String> =
        args.iter().find_map(|a| a.strip_prefix("--period=").map(|s| s.to_string()));
    let limit: Option<usize> =
        args.iter().find_map(|a| a.strip_prefix("--limit=").and_then(|n| n.parse().ok()));
    // --sample / --sample=PCT: random fraction of the fleet (default 5%).
    let sample_pct: Option<f64> = args.iter().find_map(|a| {
        if a == "--sample" {
            Some(5.0)
        } else {
            a.strip_prefix("--sample=").and_then(|n| n.parse::<f64>().ok())
        }
    });

    let do_month = !year_only;
    let do_year = !month_only;

    let base = output_path();
    println!(
        "OUTPUT_PATH={}  levels={}{}  layer={}  period={}",
        base,
        if do_month { "day>month " } else { "" },
        if do_year { "month>year" } else { "" },
        layer_filter.as_deref().unwrap_or("all"),
        period.as_deref().unwrap_or("all"),
    );

    let mut stations: Vec<(String, String)> = Vec::new();
    if let Ok(entries) = std::fs::read_dir(&base) {
        for e in entries.flatten() {
            if e.file_type().map(|t| t.is_dir()).unwrap_or(false) {
                if let Some(n) = e.file_name().to_str() {
                    stations.push((n.to_string(), e.path().to_string_lossy().to_string()));
                }
            }
        }
    }
    stations.sort();
    if let Some(ref f) = filter {
        stations.retain(|(n, _)| n == f);
    }
    // Random sampling: only when no specific station was named. Assign a random
    // key to each, keep the lowest-keyed P% (an unbiased random subset).
    if filter.is_none() {
        if let Some(p) = sample_pct {
            let total_before = stations.len();
            let keep = (((total_before as f64) * p / 100.0).ceil() as usize)
                .clamp(1, total_before.max(1));
            let mut seed = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos() as u64)
                .unwrap_or(0x9e3779b97f4a7c15)
                | 1;
            let mut keyed: Vec<(u64, (String, String))> =
                stations.into_iter().map(|s| (xorshift(&mut seed), s)).collect();
            keyed.sort_by_key(|(k, _)| *k);
            keyed.truncate(keep);
            stations = keyed.into_iter().map(|(_, s)| s).collect();
            stations.sort(); // restore name order for stable progress output
            println!("Sampling {:.1}% -> {} of {} station dirs", p, keep, total_before);
        }
    }
    if let Some(n) = limit {
        stations.truncate(n);
    }
    println!("Checking {} station output dirs...\n", stations.len());

    let total = stations.len();
    let run_start = Instant::now();
    let mut last_tick = Instant::now();

    let mut stations_with_mismatch = 0usize;
    let mut suspicious_parents = 0usize;
    let mut benign_parents = 0usize;
    let mut tot_child_excess: u64 = 0;
    let mut tot_child_only: usize = 0;
    // Buffer flagged lines so we can print them sorted worst-first.
    // Each entry: (suspicious, disc_pct, line).
    let mut flagged: Vec<(bool, f64, String)> = Vec::new();

    for (i, (name, dir)) in stations.iter().enumerate() {
        if i == 0 || last_tick.elapsed().as_secs_f64() >= 2.0 {
            let el = run_start.elapsed().as_secs_f64().max(1e-6);
            let rate = i as f64 / el;
            let eta = if rate > 0.0 { (total - i) as f64 / rate } else { 0.0 };
            eprint!("\r[{}/{}] {:.0}s  {:.0} st/s  ETA {:.0}s  {:<24}", i, total, el, rate, eta, name);
            let _ = std::io::stderr().flush();
            last_tick = Instant::now();
        }

        // Enumerate this station's dated snapshot files (skip symlinks: the
        // "latest" pointers duplicate a dated file and have no file_id).
        let mut snaps: Vec<Snap> = Vec::new();
        let Ok(entries) = std::fs::read_dir(dir) else { continue };
        for e in entries.flatten() {
            if e.file_type().map(|t| t.is_symlink()).unwrap_or(false) {
                continue;
            }
            let fname = e.file_name();
            let Some(fname) = fname.to_str() else { continue };
            if !fname.ends_with(".arrow.gz") {
                continue;
            }
            if let Some(s) = parse_snap(fname, &e.path().to_string_lossy()) {
                if layer_filter.as_ref().map(|l| l == &s.layer).unwrap_or(true) {
                    snaps.push(s);
                }
            }
        }
        if snaps.is_empty() {
            continue;
        }

        // Index file_id -> path for each (layer, acc_type).
        let mut idx: HashMap<(String, String), HashMap<String, String>> = HashMap::new();
        for s in &snaps {
            idx.entry((s.layer.clone(), s.acc_type.clone()))
                .or_default()
                .insert(s.file_id.clone(), s.path.clone());
        }

        let mut station_flagged = false;
        let mut do_level = |layer: &str,
                            parent_type: &str,
                            child_type: &str,
                            child_prefix: &dyn Fn(&str) -> String| {
            let Some(parents) = idx.get(&(layer.to_string(), parent_type.to_string())) else {
                return;
            };
            let empty = HashMap::new();
            let children_idx = idx.get(&(layer.to_string(), child_type.to_string())).unwrap_or(&empty);

            let mut parent_ids: Vec<&String> = parents.keys().collect();
            parent_ids.sort();
            for pid in parent_ids {
                if let Some(ref per) = period {
                    if !pid.starts_with(per.as_str()) {
                        continue;
                    }
                }
                // Children whose file_id falls under this parent period.
                let prefix = child_prefix(pid);
                let mut kids: Vec<(&String, &String)> = children_idx
                    .iter()
                    .filter(|(cid, _)| cid.starts_with(&prefix))
                    .collect();
                if kids.is_empty() {
                    continue; // nothing to compare against
                }
                kids.sort_by(|a, b| a.0.cmp(b.0));

                let Some(parent_counts) = read_counts(&parents[pid]) else { continue };
                let mut child_maps = Vec::with_capacity(kids.len());
                for (_, path) in &kids {
                    if let Some(m) = read_counts(path) {
                        child_maps.push(m);
                    }
                }
                if child_maps.is_empty() {
                    continue;
                }
                let merged = merge(&child_maps);
                let (child_only, parent_only, child_excess, parent_excess) =
                    compare(&parent_counts, &merged);

                let diff = Diff {
                    parent_cells: parent_counts.len(),
                    parent_count: parent_counts.values().sum(),
                    child_files: kids.len(),
                    child_span: (kids.first().unwrap().0.clone(), kids.last().unwrap().0.clone()),
                    child_cells: merged.len(),
                    child_count: merged.values().sum(),
                    child_only_cells: child_only,
                    parent_only_cells: parent_only,
                    child_excess,
                    parent_excess,
                };

                if diff.clean() && !show_all {
                    continue;
                }
                if diff.suspicious() {
                    suspicious_parents += 1;
                    tot_child_excess += diff.child_excess;
                    tot_child_only += diff.child_only_cells;
                } else if !diff.clean() {
                    benign_parents += 1;
                }
                if !diff.clean() {
                    station_flagged = true;
                }

                let flag = if diff.suspicious() { "!! " } else { "   " };
                let line = format!(
                    "{}disc={:5.1}%  {} [{}] {} {}: parent {}cells/{}cnt | {} {} files {}..{} {}cells/{}cnt | child_only={} child_excess={} parent_only={} parent_excess={}",
                    flag, diff.disc_pct(), name, layer, parent_type, pid,
                    diff.parent_cells, diff.parent_count,
                    diff.child_files, child_type, diff.child_span.0, diff.child_span.1,
                    diff.child_cells, diff.child_count,
                    diff.child_only_cells, diff.child_excess,
                    diff.parent_only_cells, diff.parent_excess,
                );
                flagged.push((diff.suspicious(), diff.disc_pct(), line));
            }
        };

        // Determine which layers are present.
        let mut layers: Vec<String> = snaps.iter().map(|s| s.layer.clone()).collect();
        layers.sort();
        layers.dedup();

        for layer in &layers {
            if do_month {
                // day "2026-06-14" -> month "2026-06": children start "2026-06-".
                do_level(layer, "month", "day", &|pid| format!("{}-", pid));
            }
            if do_year {
                // month "2026-06" -> year "2026": children start "2026-".
                do_level(layer, "year", "month", &|pid| format!("{}-", pid));
            }
        }

        if station_flagged {
            stations_with_mismatch += 1;
        }
    }

    eprint!("\r{:80}\r", " ");

    // Worst-first: suspicious before benign/clean, then by disc% descending.
    flagged.sort_by(|a, b| {
        b.0.cmp(&a.0)
            .then(b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal))
    });
    for (_, _, line) in &flagged {
        println!("{}", line);
    }

    println!("\n=== ROLLUP-CHECK SUMMARY ({:.0}s) ===", run_start.elapsed().as_secs_f64());
    println!("Stations checked:            {}", total);
    println!("Stations with any mismatch:  {}", stations_with_mismatch);
    println!("Suspicious parents (!!):     {}  (children hold coverage the parent dropped)", suspicious_parents);
    println!("  total child-excess count:  {}", tot_child_excess);
    println!("  total child-only cells:    {}", tot_child_only);
    println!("Benign parents (parent>=children, likely pruned children): {}", benign_parents);
    if suspicious_parents == 0 {
        println!("\nNo parent under-counts its children: rollup aggregation is consistent.");
    } else {
        println!("\nSuspicious parents found: a parent snapshot is missing coverage its own children still hold.");
    }
}
