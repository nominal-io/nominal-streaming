use std::process::Command;

fn main() {
    let mut args = std::env::args_os().skip(1);
    let task = args.next().unwrap_or_else(|| {
        eprintln!("usage: cargo xtask <task> [args...]");
        std::process::exit(1);
    });

    let manifest_path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join(&task)
        .join("Cargo.toml");

    if !manifest_path.exists() {
        eprintln!("xtask: no task {:?}", task);
        std::process::exit(1);
    }

    let status = Command::new(std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into()))
        .arg("run")
        .arg("--manifest-path")
        .arg(&manifest_path)
        .arg("--")
        .args(args)
        .status()
        .expect("failed to run cargo");

    std::process::exit(status.code().unwrap_or(1));
}
