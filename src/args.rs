use clap::Parser;

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about=None)]
pub struct Args {
    /// Admin API key. Leave empty for no API key (testing only).
    #[clap(long)]
    pub admin_key: Option<String>,

    /// Mongo URL
    #[clap(long)]
    pub mongo: Option<String>,

    /// Set verbosity level.
    #[clap(short, long, parse(from_occurrences))]
    pub verbose: usize,

    /// Set the listening address
    #[clap(long)]
    pub address: Option<String>,
}
