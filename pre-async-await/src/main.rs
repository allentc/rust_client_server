#[macro_use]
extern crate clap;

#[macro_use]
extern crate quick_error;

use futures::future::{lazy, Loop};
use futures::sync::mpsc::{self, Sender};
use std::collections::HashMap;
use std::thread::{self, ThreadId};
use std::time::{Duration, Instant};
use tokio::io;
use tokio::net::{UnixListener, UnixStream};
use tokio::prelude::*;
use tokio_timer::Interval;

mod unixstream_workaround;

const SOCKET_PATH: &str = "\0/tmp/rust_unix.socket";

quick_error! {
    #[derive(Debug)]
    enum Error {
        IoErr(err: std::io::Error) {
            from()
        }
        TimerErr(err: tokio_timer::Error) {
            from()
        }
        SendErr(err: mpsc::SendError<ScoreboardMsg>) {
            from()
        }
    }
}

fn main() {
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

    // TODO: report Runtime/Executor metrics: max threads, etc.

    if matches.subcommand_matches("server").is_some() {
        tokio::run(
            UnixListener::bind(SOCKET_PATH)
                .unwrap()
                .incoming()
                .map_err(|err| eprintln!("listener failed: {:?}", err))
                .for_each(|socket| tokio::spawn(server(socket))),
        );
    }

    if let Some(client_args) = matches.subcommand_matches("client") {
        let clients = value_t!(client_args, "CLIENTS", usize).unwrap();
        let interval = Duration::from_secs(value_t!(client_args, "INTERVAL", u64).unwrap());

        tokio::run(lazy(move || {
            let sender = Scoreboard::start(clients);

            for n in 0..clients {
                let sender = sender.clone();
                tokio::spawn(lazy(move || client(n, interval, sender)));
            }

            Ok(())
        }));
    }
}

struct Scoreboard {}

impl Scoreboard {
    fn start(buffer: usize) -> Sender<ScoreboardMsg> {
        let (sender, receiver) = mpsc::channel::<ScoreboardMsg>(buffer);

        tokio::spawn(lazy(|| {
            let mut thread_map: HashMap<ThreadId, usize> = HashMap::new();
            let mut last_render_time = Instant::now();

            receiver.for_each(move |msg| {
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
                            .filter(|line| {
                                line.starts_with("VmSize:") || line.starts_with("VmRSS:")
                            })
                            .for_each(|line| {
                                println!(" - {}", line);
                            });

                        last_render_time = now;
                        thread_map.clear();
                    }
                }

                Ok(())
            })
        }));

        tokio::spawn(scoreboard_render_task(
            Interval::new_interval(Duration::from_secs(1)),
            sender.clone(),
        ));

        sender
    }
}

fn scoreboard_render_task(
    i: Interval,
    s: Sender<ScoreboardMsg>,
) -> impl Future<Item = (), Error = ()> {
    future::loop_fn((i.into_future(), s), |(i, s)| {
        i.and_then(|(_, i)| Ok((i, s)))
            .map_err(|(err, _)| Error::from(err))
            .and_then(|(i, s)| {
                s.send(ScoreboardMsg::Render)
                    .map_err(Error::from)
                    .map(|s| (i, s))
            })
            .and_then(|(i, s)| Ok(Loop::Continue((i.into_future(), s))))
    })
    .map_err(|err| eprintln!("scoreboard reader task failed: {:?}", err))
}

#[derive(Debug)]
enum ScoreboardMsg {
    Hit { thread_id: ThreadId },
    Render,
}

fn client(
    _n: usize,
    interval: Duration,
    sender: Sender<ScoreboardMsg>,
) -> impl Future<Item = (), Error = ()> {
    unixstream_workaround::connect(SOCKET_PATH)
        .map_err(Error::from)
        .and_then(move |socket| {
            let buf: [u8; 1] = [0];

            future::loop_fn((socket, buf, sender), move |(socket, buf, sender)| {
                tokio_timer::sleep(interval)
                    .map_err(Error::from)
                    .and_then(move |_| io::write_all(socket, buf).map_err(Error::from))
                    .and_then(|(socket, buf)| io::read_exact(socket, buf).map_err(Error::from))
                    .map(|(socket, buf)| (socket, buf, sender))
                    .and_then(|(socket, buf, sender)| {
                        sender
                            .send(ScoreboardMsg::Hit {
                                thread_id: thread::current().id(),
                            })
                            .map_err(Error::from)
                            .map(move |sender| (socket, buf, sender))
                    })
                    .and_then(|tup| Ok(Loop::Continue(tup)))
            })
        })
        .map_err(|err| eprintln!("client failed: {:?}", err))
}

fn server(socket: UnixStream) -> impl Future<Item = (), Error = ()> {
    let buf: [u8; 1] = [0; 1];

    future::loop_fn((socket, buf), |(socket, buf)| {
        io::read_exact(socket, buf)
            .and_then(|(socket, buf)| io::write_all(socket, buf))
            .map_err(Error::from)
            .and_then(|tup| Ok(Loop::Continue(tup)))
    })
    .map_err(|err| eprintln!("server failed: {:?}", err))
}
