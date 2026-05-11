use std::collections::BTreeMap;
use std::io::BufRead;
use std::io::BufReader;
use std::path::Path;
use std::path::PathBuf;
use std::process::Command;
use std::process::ExitCode;
use std::process::Stdio;

fn main() -> ExitCode {
    let extra: Vec<String> = std::env::args().skip(1).collect();

    let save_baseline = flag_value(&extra, "--save-baseline");
    let load_baseline = flag_value(&extra, "--baseline");

    let mut cmd = Command::new(std::env::var("CARGO").unwrap_or_else(|_| "cargo".into()));
    cmd.args([
        "bench",
        "-p",
        "nominal-streaming",
        "--bench",
        "streaming",
        "--features",
        "bench",
    ]);
    if !extra.is_empty() {
        cmd.arg("--");
        cmd.args(&extra);
    }
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::inherit());

    let mut child = cmd.spawn().expect("failed to spawn cargo bench");
    let stdout = child.stdout.take().unwrap();

    let mut bench_stats: BTreeMap<String, WorkloadStats> = BTreeMap::new();

    for line in BufReader::new(stdout).lines().map_while(Result::ok) {
        println!("{line}");
        let trimmed = line.trim();
        if let Some(rest) = trimmed.strip_prefix("BENCH_STATS:") {
            if let Ok(v) = serde_json::from_str::<serde_json::Value>(rest) {
                if let Some(label) = v["label"].as_str() {
                    bench_stats.insert(
                        label.to_string(),
                        WorkloadStats {
                            total_elements: v["total_elements"].as_u64().unwrap_or(0),
                            mean_ms: 0.0,
                            bp_ms: v["bp_ms"].as_f64().unwrap_or(0.0),
                            disp_ms: v["disp_ms"].as_f64().unwrap_or(0.0),
                            session_ms: v["session_ms"].as_f64().unwrap_or(0.0),
                            p50_ms: v["p50_ms"].as_f64().unwrap_or(0.0),
                            p95_ms: v["p95_ms"].as_f64().unwrap_or(0.0),
                            p99_ms: v["p99_ms"].as_f64().unwrap_or(0.0),
                        },
                    );
                }
            }
        }
    }

    let status = child.wait().expect("failed to wait on cargo bench");

    // Merge criterion mean_ms into bench_stats before saving baseline.
    let timing = read_criterion_timings(Path::new("target/criterion"));
    for (label, stats) in bench_stats.iter_mut() {
        if let Some(e) = timing.get(label) {
            stats.mean_ms = e.mean_ms;
        }
    }

    if let Some(name) = &save_baseline {
        save_xtask_baseline(name, &bench_stats);
    }

    let baseline = load_baseline.as_deref().and_then(load_xtask_baseline);
    let p_values = load_baseline
        .as_deref()
        .map(|name| compute_p_values(Path::new("target/criterion"), name))
        .unwrap_or_default();
    print_table(&timing, &bench_stats, &p_values, baseline.as_ref());

    ExitCode::from(status.code().unwrap_or(0) as u8)
}

// --- baseline file ----------------------------------------------------------

fn baseline_path(name: &str) -> PathBuf {
    Path::new("target/xtask-baselines").join(format!("{name}.json"))
}

fn save_xtask_baseline(name: &str, stats: &BTreeMap<String, WorkloadStats>) {
    let path = baseline_path(name);
    std::fs::create_dir_all(path.parent().unwrap()).ok();
    let mut map = serde_json::Map::new();
    for (label, s) in stats {
        map.insert(
            label.clone(),
            serde_json::json!({
                "total_elements": s.total_elements,
                "mean_ms": s.mean_ms,
                "bp_ms": s.bp_ms,
                "disp_ms": s.disp_ms,
                "session_ms": s.session_ms,
                "p50_ms": s.p50_ms,
                "p95_ms": s.p95_ms,
                "p99_ms": s.p99_ms,
            }),
        );
    }
    if let Ok(json) = serde_json::to_string_pretty(&serde_json::Value::Object(map)) {
        std::fs::write(&path, json).ok();
        eprintln!("xtask: saved baseline '{name}' to {}", path.display());
    }
}

fn load_xtask_baseline(name: &str) -> Option<BTreeMap<String, WorkloadStats>> {
    let data = std::fs::read_to_string(baseline_path(name)).ok()?;
    let v: serde_json::Value = serde_json::from_str(&data).ok()?;
    let map = v.as_object()?;
    let mut out = BTreeMap::new();
    for (label, s) in map {
        out.insert(
            label.clone(),
            WorkloadStats {
                total_elements: s["total_elements"].as_u64().unwrap_or(0),
                mean_ms: s["mean_ms"].as_f64().unwrap_or(0.0),
                bp_ms: s["bp_ms"].as_f64().unwrap_or(0.0),
                disp_ms: s["disp_ms"].as_f64().unwrap_or(0.0),
                session_ms: s["session_ms"].as_f64().unwrap_or(0.0),
                p50_ms: s["p50_ms"].as_f64().unwrap_or(0.0),
                p95_ms: s["p95_ms"].as_f64().unwrap_or(0.0),
                p99_ms: s["p99_ms"].as_f64().unwrap_or(0.0),
            },
        );
    }
    Some(out)
}

// --- criterion JSON ---------------------------------------------------------

struct CriterionEntry {
    mean_ms: f64,
    std_ms: f64,
}

fn read_criterion_timings(criterion_dir: &Path) -> BTreeMap<String, CriterionEntry> {
    let mut out = BTreeMap::new();
    let Ok(groups) = std::fs::read_dir(criterion_dir) else {
        return out;
    };
    for group in groups.flatten() {
        if !group.file_type().is_ok_and(|t| t.is_dir()) {
            continue;
        }
        let Ok(benches) = std::fs::read_dir(group.path()) else {
            continue;
        };
        for bench in benches.flatten() {
            if !bench.file_type().is_ok_and(|t| t.is_dir()) {
                continue;
            }
            let estimates_path = bench.path().join("new/estimates.json");
            let Ok(data) = std::fs::read_to_string(&estimates_path) else {
                continue;
            };
            let Ok(v) = serde_json::from_str::<serde_json::Value>(&data) else {
                continue;
            };
            let mean_ms = v["mean"]["point_estimate"].as_f64().unwrap_or(0.0) / 1e6;
            let std_ms = v["std_dev"]["point_estimate"].as_f64().unwrap_or(0.0) / 1e6;
            let name = bench.file_name().to_string_lossy().into_owned();
            out.insert(name, CriterionEntry { mean_ms, std_ms });
        }
    }
    out
}

// --- p-value from criterion samples -----------------------------------------

fn compute_p_values(criterion_dir: &Path, baseline_name: &str) -> BTreeMap<String, f64> {
    let mut out = BTreeMap::new();
    let Ok(groups) = std::fs::read_dir(criterion_dir) else {
        return out;
    };
    for group in groups.flatten() {
        if !group.file_type().is_ok_and(|t| t.is_dir()) {
            continue;
        }
        let Ok(benches) = std::fs::read_dir(group.path()) else {
            continue;
        };
        for bench in benches.flatten() {
            if !bench.file_type().is_ok_and(|t| t.is_dir()) {
                continue;
            }
            let name = bench.file_name().to_string_lossy().into_owned();
            let cur = read_samples(&bench.path().join("new/sample.json"));
            let base = read_samples(&bench.path().join(format!("{baseline_name}/sample.json")));
            if let Some(p) = welch_p_value(&cur, &base) {
                out.insert(name, p);
            }
        }
    }
    out
}

fn read_samples(path: &Path) -> Vec<f64> {
    let Ok(data) = std::fs::read_to_string(path) else {
        return vec![];
    };
    let Ok(v) = serde_json::from_str::<serde_json::Value>(&data) else {
        return vec![];
    };
    let iters: Vec<f64> = v["iters"]
        .as_array()
        .map(|a| a.iter().map(|x| x.as_f64().unwrap_or(1.0)).collect())
        .unwrap_or_default();
    let times: Vec<f64> = v["times"]
        .as_array()
        .map(|a| a.iter().map(|x| x.as_f64().unwrap_or(0.0)).collect())
        .unwrap_or_default();
    iters
        .iter()
        .zip(times.iter())
        .filter(|(&i, _)| i > 0.0)
        .map(|(&i, &t)| t / i)
        .collect()
}

// Welch's t-test, two-tailed p-value.
fn welch_p_value(a: &[f64], b: &[f64]) -> Option<f64> {
    if a.len() < 2 || b.len() < 2 {
        return None;
    }
    let (mean_a, var_a) = mean_var(a);
    let (mean_b, var_b) = mean_var(b);
    let na = a.len() as f64;
    let nb = b.len() as f64;
    let se2_a = var_a / na;
    let se2_b = var_b / nb;
    let se = (se2_a + se2_b).sqrt();
    if se < 1e-30 {
        return Some(if (mean_a - mean_b).abs() < 1e-10 {
            1.0
        } else {
            0.0
        });
    }
    let t = (mean_a - mean_b).abs() / se;
    let df = (se2_a + se2_b).powi(2) / (se2_a.powi(2) / (na - 1.0) + se2_b.powi(2) / (nb - 1.0));
    // P(|T_df| >= t) = I_{df/(df+t²)}(df/2, 1/2)
    Some(reg_inc_beta(df / (df + t * t), df / 2.0, 0.5))
}

fn mean_var(xs: &[f64]) -> (f64, f64) {
    let n = xs.len() as f64;
    let mean = xs.iter().sum::<f64>() / n;
    let var = xs.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / (n - 1.0);
    (mean, var)
}

// Regularized incomplete beta function I_x(a, b) via continued fraction.
fn reg_inc_beta(x: f64, a: f64, b: f64) -> f64 {
    if x <= 0.0 {
        return 0.0;
    }
    if x >= 1.0 {
        return 1.0;
    }
    // Symmetry relation improves convergence when x is large.
    if x > (a + 1.0) / (a + b + 2.0) {
        return 1.0 - reg_inc_beta(1.0 - x, b, a);
    }
    let ln_beta = ln_gamma(a + b) - ln_gamma(a) - ln_gamma(b);
    let front = (ln_beta + a * x.ln() + b * (1.0 - x).ln()).exp() / a;
    front * beta_cf(x, a, b)
}

// Lentz's continued fraction for the incomplete beta function.
fn beta_cf(x: f64, a: f64, b: f64) -> f64 {
    const MAX_ITER: usize = 200;
    const EPS: f64 = 3e-15;
    let qab = a + b;
    let qap = a + 1.0;
    let qam = a - 1.0;
    let mut c = 1.0_f64;
    let mut d = 1.0 - qab * x / qap;
    if d.abs() < f64::MIN_POSITIVE {
        d = f64::MIN_POSITIVE;
    }
    d = 1.0 / d;
    let mut h = d;
    for m in 1..=MAX_ITER {
        let m = m as f64;
        let m2 = 2.0 * m;
        let aa = m * (b - m) * x / ((qam + m2) * (a + m2));
        d = 1.0 + aa * d;
        if d.abs() < f64::MIN_POSITIVE {
            d = f64::MIN_POSITIVE;
        }
        c = 1.0 + aa / c;
        if c.abs() < f64::MIN_POSITIVE {
            c = f64::MIN_POSITIVE;
        }
        d = 1.0 / d;
        h *= d * c;
        let aa = -(a + m) * (qab + m) * x / ((a + m2) * (qap + m2));
        d = 1.0 + aa * d;
        if d.abs() < f64::MIN_POSITIVE {
            d = f64::MIN_POSITIVE;
        }
        c = 1.0 + aa / c;
        if c.abs() < f64::MIN_POSITIVE {
            c = f64::MIN_POSITIVE;
        }
        d = 1.0 / d;
        let del = d * c;
        h *= del;
        if (del - 1.0).abs() < EPS {
            break;
        }
    }
    h
}

// Lanczos approximation for the log-gamma function.
fn ln_gamma(z: f64) -> f64 {
    let c = [
        0.999_999_999_999_809_93,
        676.520_368_121_885_1,
        -1259.139_216_722_402_8,
        771.323_428_777_653_13,
        -176.615_029_162_140_59,
        12.507_343_278_686_905,
        -0.138_571_095_265_720_12,
        9.984_369_578_019_572e-6,
        1.505_632_735_149_311_6e-7,
    ];
    let z = z - 1.0;
    let t = z + c.len() as f64 - 1.5;
    let ser: f64 = c[0]
        + c[1..]
            .iter()
            .enumerate()
            .map(|(i, &ci)| ci / (z + i as f64 + 1.0))
            .sum::<f64>();
    0.5 * (2.0 * std::f64::consts::PI).ln() + (z + 0.5) * t.ln() - t + ser.ln()
}

// --- workload stats ---------------------------------------------------------

struct WorkloadStats {
    total_elements: u64,
    mean_ms: f64,
    bp_ms: f64,
    disp_ms: f64,
    session_ms: f64,
    p50_ms: f64,
    p95_ms: f64,
    p99_ms: f64,
}

// --- table ------------------------------------------------------------------

fn print_table(
    timing: &BTreeMap<String, CriterionEntry>,
    stats: &BTreeMap<String, WorkloadStats>,
    p_values: &BTreeMap<String, f64>,
    baseline: Option<&BTreeMap<String, WorkloadStats>>,
) {
    if stats.is_empty() {
        return;
    }

    let has_base = baseline.is_some();

    eprintln!();
    eprintln!("=== Benchmark Summary ===");
    eprintln!();

    // Column layout (1-indexed):
    //   1–32:    Workload
    //   34–42:   Mean(ms)
    //   44–51:   ±σ(ms)
    //   53–68:   Throughput
    //   70–78:   BP(ms)
    //   80–91:   Disp(ms)
    //   93–99:   p50  ─╮
    //   101–107: p95   ├─ "Latency (ms)" centered at col 103 → pad 97
    //   109–115: p99  ─╯
    //   117–122: p-val
    //   [with baseline]
    //   124–130: ΔMean% ─╮
    //   132–138: ΔTP%    │
    //   140–147: ΔBP%    │
    //   149–159: ΔDisp%  ├─ "Δ vs baseline" centered at col 153 → 38 spaces after "Latency (ms)"
    //   161–167: Δp50%   │
    //   169–175: Δp95%   │
    //   177–183: Δp99%  ─╯

    let mut group_line = format!("{:97}Latency (ms)", "");
    if has_base {
        group_line.push_str(&format!("{:38}Δ vs baseline", ""));
    }
    eprintln!("{group_line}");

    let mut col_header = format!(
        "{:<32} {:>9} {:>8} {:>16} {:>9} {:>12} {:>7} {:>7} {:>7} {:>6}",
        "Workload",
        "Mean(ms)",
        "±σ(ms)",
        "Throughput",
        "BP(ms)",
        "Disp(ms)",
        "p50",
        "p95",
        "p99",
        "p-val"
    );
    if has_base {
        col_header.push_str(&format!(
            " {:>7} {:>7} {:>8} {:>11} {:>7} {:>7} {:>7}",
            "ΔMean%", "ΔTP%", "ΔBP%", "ΔDisp%", "Δp50%", "Δp95%", "Δp99%"
        ));
    }
    eprintln!("{col_header}");
    eprintln!("{}", "─".repeat(col_header.chars().count()));

    for (label, s) in stats {
        let (mean_ms, std_ms) = timing
            .get(label)
            .map(|e| (e.mean_ms, e.std_ms))
            .unwrap_or((0.0, 0.0));

        let throughput_str = if s.session_ms > 0.0 {
            fmt_throughput(s.total_elements as f64 / (s.session_ms / 1e3))
        } else {
            String::new()
        };

        let p_str = p_values
            .get(label)
            .map(|p| format!("{p:.2}"))
            .unwrap_or_else(|| "n/a".to_string());

        let mut row = format!(
            "{:<32} {:>9.3} {:>8.3} {:>16} {:>9.1} {:>12.1} {:>7.1} {:>7.1} {:>7.1} {:>6}",
            label,
            mean_ms,
            std_ms,
            throughput_str,
            s.bp_ms,
            s.disp_ms,
            s.p50_ms,
            s.p95_ms,
            s.p99_ms,
            p_str
        );

        if has_base {
            let d = |cur: f64, base: f64| -> String {
                if base == 0.0 {
                    return "n/a".to_string();
                }
                format!("{:>+.1}%", (cur - base) / base * 100.0)
            };
            let throughput = |st: &WorkloadStats| {
                if st.session_ms > 0.0 {
                    st.total_elements as f64 / (st.session_ms / 1e3)
                } else {
                    0.0
                }
            };
            if let Some(b) = baseline.and_then(|m| m.get(label)) {
                row.push_str(&format!(
                    " {:>7} {:>7} {:>8} {:>11} {:>7} {:>7} {:>7}",
                    d(mean_ms, b.mean_ms),
                    d(throughput(s), throughput(b)),
                    d(s.bp_ms, b.bp_ms),
                    d(s.disp_ms, b.disp_ms),
                    d(s.p50_ms, b.p50_ms),
                    d(s.p95_ms, b.p95_ms),
                    d(s.p99_ms, b.p99_ms)
                ));
            } else {
                row.push_str(&format!(
                    " {:>7} {:>7} {:>8} {:>11} {:>7} {:>7} {:>7}",
                    "—", "—", "—", "—", "—", "—", "—"
                ));
            }
        }

        eprintln!("{row}");
    }
    eprintln!();
}

fn fmt_throughput(elem_s: f64) -> String {
    if elem_s >= 1e9 {
        format!("{:.2} Gelem/s", elem_s / 1e9)
    } else if elem_s >= 1e6 {
        format!("{:.2} Melem/s", elem_s / 1e6)
    } else if elem_s >= 1e3 {
        format!("{:.2} Kelem/s", elem_s / 1e3)
    } else {
        format!("{:.2} elem/s", elem_s)
    }
}

// --- arg parsing ------------------------------------------------------------

fn flag_value(args: &[String], flag: &str) -> Option<String> {
    args.windows(2).find(|w| w[0] == flag).map(|w| w[1].clone())
}
