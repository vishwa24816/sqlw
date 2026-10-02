use std::process::exit;

fn main() {
    let cfg = match sqlw::service::Config::from_env() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("sqlw: {e}");
            exit(2);
        }
    };
    if let Err(e) = sqlw::service::run(cfg) {
        eprintln!("sqlw: {e}");
        exit(1);
    }
}
