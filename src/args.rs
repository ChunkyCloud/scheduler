use clap::Parser;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about=None)]
pub struct Args {
    /// Admin API key. Leave empty for no API key (testing only).
    pub admin_key: Option<String>,

    /// Mongo URL
    pub mongo: Option<String>,

    /// Set verbosity level.
    #[clap(short, long, parse(from_occurrences))]
    pub verbose: usize,
}
