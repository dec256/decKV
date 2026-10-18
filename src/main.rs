mod kv;
use crate::{
    States::{Candidate, Follower, Leader},
    kv::{Entry, command_recieve, commands},
};
use anyhow::Result;
use rand::prelude::*;
use std::{
    sync::{Arc, Mutex},
    time::Instant,
};
use tokio::{
    fs,
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
    time::{Duration, sleep},
};

#[derive(PartialEq)]
enum States {
    Leader,
    Candidate,
    Follower,
}

#[tokio::main]
async fn main() -> Result<()> {
    let values = Arc::new(Mutex::new(Vec::<Entry>::new()));
    let state = Arc::new(Mutex::new(Follower));
    let time = Arc::new(Mutex::new(Instant::now()));
    let term = Arc::new(Mutex::new(0));
    let vote_elegible = Arc::new(Mutex::new(true));
    let votes = Arc::new(Mutex::new((0i32, 0usize)));

    let configuration: String = match fs::read_to_string("peers.conf").await {
        Ok(stuff) => stuff,
        Err(e) => {
            eprintln!("make that peer.conf file: {}", e);
            std::process::exit(1);
        }
    };

    let config_ips: Vec<String> = configuration
        .lines()
        .map(|x| x.trim().to_string())
        .filter(|x| !x.is_empty())
        .collect();

    let listen_ip = match find_listener(config_ips.clone()).await {
        Some(x) => x,
        _ => {
            eprintln!("no available IPs");
            std::process::exit(1);
        }
    };

    {
        let (ip, all_ip) = (listen_ip.clone(), config_ips.clone());
        tokio::spawn(async move {
            if let Err(e) = commands(ip, all_ip).await {
                eprintln!("failed to get input: {}", e);
                std::process::exit(1);
            };
        });
    }

    {
        let (
            all_address,
            ip_address,
            state_clone,
            time_clone,
            term_clone,
            vote_elegible_clone,
            votes_clone,
        ) = (
            config_ips.clone(),
            listen_ip.clone(),
            state.clone(),
            time.clone(),
            term.clone(),
            vote_elegible.clone(),
            votes.clone(),
        );
        let values_clone = values.clone();
        tokio::spawn(async move {
            let (state, time, term, vote_elegible, votes) = (
                Arc::clone(&state_clone),
                Arc::clone(&time_clone),
                Arc::clone(&term_clone),
                Arc::clone(&vote_elegible_clone),
                Arc::clone(&votes_clone),
            );
            let value = Arc::clone(&values_clone);
            loop {
                if *state.lock().unwrap() == Leader {
                    sleep(Duration::from_millis(50)).await;
                    continue;
                }
                let wait = rand::rng().random_range(200..=500);
                tokio::time::sleep(Duration::from_millis(wait)).await;
                {
                    let amount = *time.lock().unwrap();
                    let duration = amount.elapsed().as_secs();

                    if duration > 2 {
                        {
                            let mut current_state = state.lock().unwrap();
                            let mut vote_eleg = vote_elegible.lock().unwrap();
                            let mut current_term = term.lock().unwrap();
                            *current_state = Candidate;
                            *current_term += 1;
                            *votes.lock().unwrap() = (*current_term, 1);
                            *vote_eleg = false;
                            *time.lock().unwrap() = Instant::now();

                            let majority = (all_address.len() / 2) + 1;
                            if 1 >= majority {
                                *current_state = Leader;
                                println!(
                                    "elected leader for term {} (self-vote majority)",
                                    *current_term
                                );
                            }
                        }
                        println!("I am now candidate");

                        let to_send = {
                            let current_term = term.lock().unwrap();
                            let (last_index, last_term) = {
                                let vals = value.lock().unwrap();
                                match vals.last() {
                                    Some(e) => (e.index, e.term),
                                    None => (0, 0),
                                }
                            };

                            format!(
                                "VOTE_REQUEST {} {} {}",
                                *current_term, last_term, last_index
                            )
                        };

                        if let Err(e) =
                            sender(&to_send, ip_address.clone(), all_address.clone(), false).await
                        {
                            eprintln!("failed to send a vote request: {}", e);
                        };
                    }
                }
            }
        });
    }

    {
        let all_address = config_ips.clone();
        let ip_address = listen_ip.clone();
        let state_clone = state.clone();

        let term_clone = term.clone();
        tokio::spawn(async move {
            let state = Arc::clone(&state_clone);
            let term = Arc::clone(&term_clone);
            loop {
                if *state.lock().unwrap() == Leader {
                    let beat_term = *term.lock().unwrap();
                    let beat_message = format!("BEAT {}", beat_term);

                    if let Err(e) = sender(
                        &beat_message,
                        ip_address.clone(),
                        all_address.clone(),
                        false,
                    )
                    .await
                    {
                        eprintln!("failed to send BEAT: {e}");
                    };
                }
                sleep(Duration::from_millis(500)).await;
            }
        });
    }

    let listener = TcpListener::bind(&listen_ip).await?;
    println!("listening on {}", &listen_ip);

    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let clone_time = time.clone();
                let clone_state = state.clone();
                let all_address = config_ips.clone();
                let ip_address = listen_ip.clone();
                let term_clone = term.clone();
                let vote_clone = vote_elegible.clone();
                let votes_clone = votes.clone();
                let values_clone = values.clone();
                tokio::spawn(async move {
                    let (time, state, term, vote_elegible, votes, value) = (
                        Arc::clone(&clone_time),
                        Arc::clone(&clone_state),
                        Arc::clone(&term_clone),
                        Arc::clone(&vote_clone),
                        Arc::clone(&votes_clone),
                        Arc::clone(&values_clone),
                    );
                    let mut reader = BufReader::new(stream);
                    let mut buffer = Vec::new();
                    let _ = reader.read_until(b'\n', &mut buffer).await;

                    let request = String::from_utf8_lossy(&buffer).trim().to_string();
                    println!("recieved: {}", request);
                    let mut parts = request.splitn(2, '>');
                    let sender_addr = parts.next().unwrap_or("").trim();
                    let body = parts.next().unwrap_or("").trim();

                    let is_leader = match *state.lock().unwrap() {
                        Leader => true,
                        _ => false,
                    };

                    if is_leader == false {
                        if body.contains("BEAT") {
                            {
                                let their_term: i32 = body
                                    .split_whitespace()
                                    .nth(1)
                                    .and_then(|t| t.parse().ok())
                                    .unwrap_or(0);

                                let current_term = term.lock().unwrap();

                                if their_term < *current_term {
                                    return;
                                }
                            }

                            {
                                let mut current_time = time.lock().unwrap();
                                let mut current_state = state.lock().unwrap();
                                *current_state = Follower;
                                *current_time = Instant::now();
                            }
                        }
                    }

                    if body.starts_with("PULL_RESULT") {
                        let result_parts: Vec<&str> = body.splitn(3, ' ').collect();
                        if result_parts.len() == 3 {
                            println!("{} = {}", result_parts[1], result_parts[2]);
                        }
                        return;
                    }

                    if (body.contains("PUSH") || body.contains("PULL")) && body.contains("KVQUEST")
                    {
                        if is_leader == false {
                            eprintln!("rejected '{}': this node is not the leader", body);
                            return;
                        }

                        let current_term = { *term.lock().unwrap() as i64 };

                        if let Err(e) = command_recieve(
                            body,
                            &value,
                            current_term,
                            &ip_address,
                            sender_addr,
                            all_address.clone(),
                        )
                        .await
                        {
                            eprintln!("failed to send command: {}", e);
                            return;
                        }
                    }

                    if (body.contains("PUSH") || body.contains("PULL")) && !body.contains("KVQUEST")
                    {
                        let current_term = { *term.lock().unwrap() as i64 };

                        if let Err(e) = command_recieve(
                            body,
                            &value,
                            current_term,
                            &ip_address,
                            sender_addr,
                            all_address.clone(),
                        )
                        .await
                        {
                            eprintln!("failed to send command: {}", e);
                            return;
                        }
                    }

                    if body.contains("VOTE_REQUEST") {
                        let their_term: i32 = body
                            .split_whitespace()
                            .nth(1)
                            .and_then(|t| t.parse().ok())
                            .unwrap_or(0);

                        let can_vote = {
                            let mut current_term = term.lock().unwrap();
                            let mut vote_eleg = vote_elegible.lock().unwrap();
                            let mut current_state = state.lock().unwrap();

                            if their_term > *current_term {
                                *current_term = their_term;
                                *vote_eleg = true;
                                *current_state = Follower;
                            }

                            if their_term < *current_term {
                                return;
                            }
                            {
                                let their_last_term: i64 = body
                                    .split_whitespace()
                                    .nth(2)
                                    .and_then(|t| t.parse().ok())
                                    .unwrap_or(0);
                                let their_last_index: i64 = body
                                    .split_whitespace()
                                    .nth(3)
                                    .and_then(|t| t.parse().ok())
                                    .unwrap_or(0);

                                let (last_index, last_term) = {
                                    let vals = value.lock().unwrap();
                                    match vals.last() {
                                        Some(e) => (e.index, e.term),
                                        None => (0, 0),
                                    }
                                };
                                let candidate_is_up_to_date = their_last_term > last_term
                                    || (their_last_term == last_term
                                        && their_last_index >= last_index);

                                if !candidate_is_up_to_date {
                                    return;
                                }
                            }

                            their_term >= *current_term && *vote_eleg
                        };

                        if can_vote {
                            *vote_elegible.lock().unwrap() = false;
                            *time.lock().unwrap() = Instant::now();
                            let voted_term = *term.lock().unwrap();
                            let message = format!("{}> VOTED {}", ip_address.clone(), voted_term);
                            let mut stream = match TcpStream::connect(sender_addr).await {
                                Ok(s) => s,
                                Err(e) => {
                                    eprintln!("failed to connect to {}: {e}", sender_addr);
                                    return;
                                }
                            };

                            if let Err(e) = stream.write_all(message.as_bytes()).await {
                                eprintln!("failed to send vote: {e}");
                            }
                        }
                    }

                    if body.contains("VOTED") {
                        let their_vote_term: i32 = body
                            .split_whitespace()
                            .nth(1)
                            .and_then(|t| t.parse().ok())
                            .unwrap_or(-1);

                        let majority = (all_address.len() / 2) + 1;
                        let mut v = votes.lock().unwrap();

                        if v.0 == their_vote_term {
                            v.1 += 1;
                            if v.1 >= majority {
                                let mut cur_state = state.lock().unwrap();
                                if *cur_state == Candidate {
                                    *cur_state = Leader;
                                    println!("elected leader for term {}", their_vote_term);
                                }
                            }
                        }
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

async fn sender(message: &str, my_ip: String, ips: Vec<String>, include_self: bool) -> Result<()> {
    let message = format!("{}> {}", my_ip, message);
    for ip in ips {
        if ip == my_ip && !include_self {
            continue;
        }
        let mut stream = match TcpStream::connect(ip).await {
            Ok(ip) => ip,
            Err(_) => continue,
        };
        stream.write_all(message.as_bytes()).await?;
    }
    Ok(())
}
