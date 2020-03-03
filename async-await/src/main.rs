#[macro_use]
extern crate clap;

use futures::stream::StreamExt;
use std::collections::HashMap;
use std::thread::{self, ThreadId};
use std::time::{Duration, Instant};
use tokio::net::{UnixListener, UnixStream};
use tokio::prelude::*;
use tokio::sync::mpsc::{self, Receiver, Sender};
use tokio::task::JoinHandle;

mod unixstream_workaround;

const SOCKET_PATH: &str = "\0/tmp/rust_unix.socket";

#[derive(Debug)]
enum ScoreboardMsg {
    Hit { thread_id: ThreadId },
    Render,
}

#[tokio::main]
async fn main() {
    let matches = clap_app!(app =>
        (name: "client_server")
        (version: "0.1.0")
        (author: "A. Campbell")
        (@setting SubcommandRequired)
        (@subcommand server => (about: "Launch a server process"))
        (@subcommand client =>
            (about: "Launch a client process and workers")
            (@arg CLIENTS: +required "concurrent workers run by this process")
            (@arg INTERVAL: +required "interval in seconds between workers requests")
        )
    )
    .get_matches();

    if matches.subcommand_matches("server").is_some() {
        let mut listener = UnixListener::bind(SOCKET_PATH).unwrap();
        let mut incoming = listener.incoming();

        while let Some(conn) = incoming.next().await {
            server(conn.unwrap());
        }
    }

    if let Some(client_args) = matches.subcommand_matches("client") {
        let clients = value_t!(client_args, "CLIENTS", usize).unwrap();
        let interval = Duration::from_secs(value_t!(client_args, "INTERVAL", u64).unwrap());
        let (sender, receiver) = mpsc::channel::<ScoreboardMsg>(clients);

        let jh = scoreboard(sender.clone(), receiver);

        for _ in 0..clients {
            client(interval, sender.clone());
        }

        println!("clients started");
        jh.await.unwrap();
    }
}

fn scoreboard(
    mut sender: Sender<ScoreboardMsg>,
    mut receiver: Receiver<ScoreboardMsg>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        let interval = Duration::from_secs(1);

        loop {
            tokio::time::delay_for(interval).await;
            sender.send(ScoreboardMsg::Render).await.unwrap();
        }
    });

    tokio::spawn(async move {
        let mut thread_map: HashMap<ThreadId, usize> = HashMap::new();
        let mut last_render_time = Instant::now();

        while let Some(msg) = receiver.recv().await {
            match msg {
                ScoreboardMsg::Hit { thread_id } => {
                    let entry = thread_map.entry(thread_id).or_insert(0);
                    *entry += 1;
                }
                ScoreboardMsg::Render => {
                    let hits: usize = thread_map.values().sum();
                    let now = Instant::now();

                    println!(
                        "elapsed={} numThreads={} totalHits={}",
                        now.duration_since(last_render_time).as_secs(),
                        thread_map.len(),
                        hits
                    );

                    std::fs::read_to_string("/proc/self/status")
                        .unwrap()
                        .lines()
                        .filter(|line| line.starts_with("VmSize:") || line.starts_with("VmRSS:"))
                        .for_each(|line| {
                            println!(" - {}", line);
                        });

                    last_render_time = now;
                    thread_map.clear();
                }
            }
        }
    })
}

fn server(mut stream: UnixStream) {
    tokio::spawn(async move {
        let mut buf: [u8; 1] = [0];

        loop {
            stream.read_exact(&mut buf).await.unwrap();
            stream.write_all(&buf).await.unwrap();
        }
    });
}

fn client(interval: Duration, mut sender: Sender<ScoreboardMsg>) {
    tokio::spawn(async move {
        let mut stream = unixstream_workaround::connect(SOCKET_PATH).await.unwrap();
        let mut buf: [u8; 1] = [0];

        loop {
            tokio::time::delay_for(interval).await;
            stream.write_all(&buf).await.unwrap();
            stream.read_exact(&mut buf).await.unwrap();
            let thread_id = thread::current().id();
            sender.send(ScoreboardMsg::Hit { thread_id }).await.unwrap();
        }
    });
}

/*181*/
