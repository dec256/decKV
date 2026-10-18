use crate::sender;
use anyhow::Result;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

#[derive(Clone)]
pub struct Entry {
    pub key: String,
    pub value: String,
    pub index: i64,
    pub term: i64,
}

pub async fn commands(ip: String, all_ip: Vec<String>) -> Result<()> {
    let mut lines = BufReader::new(tokio::io::stdin()).lines();

    while let Some(command) = lines.next_line().await? {
        let command = command.trim();
        if command.is_empty() {
            continue;
        }

        let to_send = format!("{} KVQUEST", command);

        if let Err(e) = sender(&to_send, ip.clone(), all_ip.clone(), true).await {
            eprintln!("failed to send a vote request: {}", e);
        };
    }

    Ok(())
}

pub async fn command_recieve(
    cmd: &str,
    map: &Arc<std::sync::Mutex<Vec<Entry>>>,
    term: i64,
    ip: &str,
    sender_addr: &str,
    all_ip: Vec<String>,
) -> Result<()> {
    let parts: Vec<&str> = cmd.trim().split_whitespace().collect();

    let Some(command) = parts.first().map(|s| s.to_string()) else {
        return Ok(());
    };

    if command != "PULL" && command != "PUSH" {
        return Ok(());
    }

    let Some(key) = parts.get(1).map(|s| s.to_string()) else {
        eprintln!("malformed command, missing key: {}", cmd);
        return Ok(());
    };

    let value: String = if command == "PUSH" {
        match parts.get(2) {
            Some(v) => v.to_string(),
            None => {
                eprintln!("malformed PUSH, missing value: {}", cmd);
                return Ok(());
            }
        }
    } else {
        "NULL".to_string()
    };

    if command == "PUSH" {
        {
            let mut mapper = map.lock().unwrap();
            let index = mapper.len() as i64;
            mapper.push(Entry {
                key,
                value,
                index,
                term,
            });
        }

        if let Err(e) = sender(cmd, ip.to_string(), all_ip, false).await {
            eprintln!("failed to send a vote request: {}", e);
        };
    } else {
        let found = {
            let mapper = map.lock().unwrap();

            mapper
                .iter()
                .rev()
                .find(|e| e.key == key)
                .map(|e| e.value.clone())
        };

        let reply = match found {
            Some(v) => format!("PULL_RESULT {} {}", key, v),
            None => format!("PULL_RESULT {} NOTFOUND", key),
        };

        match TcpStream::connect(sender_addr).await {
            Ok(mut stream) => {
                let full = format!("{}> {}", ip, reply);
                if let Err(e) = stream.write_all(full.as_bytes()).await {
                    eprintln!("failed to send PULL result to {}: {}", sender_addr, e);
                }
            }
            Err(e) => {
                eprintln!(
                    "failed to connect back to {} with PULL result: {}",
                    sender_addr, e
                );
            }
        }
    }

    Ok(())
}
