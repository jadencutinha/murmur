//! `murmur` — the cluster client CLI.
//!
//! A thin wrapper over [`Clerk`](murmur::clerk::Clerk): it connects to a murmur
//! cluster (any/all of the nodes' KV addresses), registers for a client id, and
//! runs a single key/value operation with exactly-once semantics — retrying
//! across nodes to find the leader, so you can point it at any node.
//!
//! Usage:
//!   murmur [--peers a:p,b:p,c:p] <get|put|append|del> <key> [value]
//!
//! Examples:
//!   murmur put color amber
//!   murmur get color
//!   murmur --peers 127.0.0.1:5001,127.0.0.1:5002 append log "hello "
//!   murmur del color
//!
//! With no `--peers`, it defaults to the three-node local demo cluster.

use murmur::clerk::Clerk;

const DEFAULT_PEERS: &str = "127.0.0.1:5001,127.0.0.1:5002,127.0.0.1:5003";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut peers = DEFAULT_PEERS.to_string();
    let mut rest: Vec<String> = Vec::new();

    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--peers" => peers = args.next().unwrap_or(peers),
            "-h" | "--help" => return usage(),
            // The first non-flag token is the command; everything after it is an
            // operand (keys/values may themselves look like anything).
            _ => {
                rest.push(arg);
                rest.extend(args.by_ref());
                break;
            }
        }
    }

    let endpoints: Vec<String> = peers.split(',').map(|s| s.trim().to_string()).collect();
    let command = match rest.first() {
        Some(c) => c.clone(),
        None => return usage(),
    };
    let operands = &rest[1..];

    let mut clerk = Clerk::connect(endpoints).await?;

    match (command.as_str(), operands) {
        ("get", [key]) => match clerk.get(key.clone().into_bytes()).await? {
            Some(value) => println!("{}", String::from_utf8_lossy(&value)),
            None => {
                eprintln!("(not found)");
                std::process::exit(1);
            }
        },
        ("put", [key, value]) => {
            clerk.put(key.clone().into_bytes(), value.clone().into_bytes()).await?;
            println!("OK");
        }
        ("append", [key, value]) => {
            let result = clerk.append(key.clone().into_bytes(), value.clone().into_bytes()).await?;
            println!("{}", String::from_utf8_lossy(&result));
        }
        ("del", [key]) => {
            clerk.delete(key.clone().into_bytes()).await?;
            println!("OK");
        }
        _ => return usage(),
    }
    Ok(())
}

fn usage() -> anyhow::Result<()> {
    eprintln!(
        "usage: murmur [--peers a:p,b:p,...] <command>\n\
         \x20 commands:\n\
         \x20   get <key>            read a value (exit 1 if absent)\n\
         \x20   put <key> <value>    write a value\n\
         \x20   append <key> <value> append to a value, printing the result\n\
         \x20   del <key>            delete a value"
    );
    anyhow::bail!("bad usage");
}
