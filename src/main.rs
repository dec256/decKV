use crate::States::{Candidate, Follower, Leader};
use anyhow::Result;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    fs,
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
};

enum States {
    Leader,
    Candidate,
    Follower,
}

#[tokio::main]
async fn main() -> Result<()> {
    let configuration: String = match fs::read_to_string("peers.conf").await {
        Ok(stuff) => stuff,
        Err(e) => {
            //if the person who made it didnt have the config
            eprintln!("make that peer.conf file: {}", e);
            std::process::exit(1); //exit ts so he can resart
        }
    };

    //grab the IPs out of that config file
    let config_ips: Vec<String> = configuration
        .lines()
        .map(|x| x.trim().to_string())
        .filter(|x| !x.is_empty())
        .collect();

    let listen_ip = match find_listener(config_ips.clone()).await {
        Some(x) => x,
        None => {
            eprintln!("no available IPs");
            std::process::exit(1);
        }
    };

    {
        let all_address = config_ips.clone();
        let ip_address = listen_ip.clone();
        tokio::spawn(async move {
            loop {
                let mut val = String::new();
                std::io::stdin().read_line(&mut val).unwrap();
                sender(&val, ip_address.clone(), all_address.clone())
                    .await
                    .unwrap();
            }
        });
    }

    let listener = TcpListener::bind(&listen_ip).await?;
    println!("listening on {}", &listen_ip);

    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let all_address = config_ips.clone();
                let ip_address = listen_ip.clone();
                tokio::spawn(async move {
                    let mut reader = BufReader::new(stream);
                    let mut buffer = Vec::new();
                    let _ = reader.read_until(b'\n', &mut buffer).await;

                    let request = String::from_utf8_lossy(&buffer).trim().to_string();
                    println!("recieved: {}", request);
                    let mut parts = request.splitn(2, '>');
                    let sender_addr = parts.next().unwrap_or("").trim();
                    let body = parts.next().unwrap_or("").trim();

                    if body.contains("beated") {
                        if let Err(e) = sender("beated", ip_address, all_address).await {
                            println!("failed to send beat: {e}");
                        };
                    }
                });
            }
            Err(e) => {
                eprintln!("failed to establish connection: {}", e);
            }
        }
    }
}

async fn find_listener(ips: Vec<String>) -> Option<String> {
    for i in ips {
        match TcpListener::bind(&i).await {
            Ok(_) => {
                return Some(i);
            }
            Err(_) => continue,
        }
    }
    None
}

async fn sender(message: &str, my_ip: String, ips: Vec<String>) -> Result<()> {
    let message = format!("{}> {}", my_ip, message);
    for ip in ips {
        if ip != my_ip {
            let mut stream = match TcpStream::connect(ip).await {
                Ok(ip) => ip,
                Err(_) => continue,
            };
            stream.write_all(message.as_bytes()).await?;
        }
    }
    Ok(())
}
