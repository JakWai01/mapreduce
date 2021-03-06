use glob::glob;
use itertools::Itertools;
use map_reduce::coordinator_client::CoordinatorClient;
use map_reduce::{GetReduceCountArgs, ReportTaskArgs, RequestTaskArgs};
use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io::Write;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::process;
use std::thread;
use std::time;
mod server;

pub mod map_reduce {
    tonic::include_proto!("mapreduce");
}

#[derive(Debug)]
struct KVStore<T, U> {
    key: T,
    value: U,
}

impl<T, U> KVStore<T, U> {
    fn new(key: T, value: U) -> Self {
        KVStore { key, value }
    }
}

struct MRFile {
    name: String,
    file: fs::File,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = CoordinatorClient::connect("http://[::1]:50051").await?;

    let mut w = Worker { n_reduce: 0 };

    w.worker(&mut client).await?;

    loop {}
}

#[derive(Clone, Copy)]
struct Worker {
    n_reduce: i32,
}

impl Worker {
    async fn worker(
        &mut self,
        client: &mut CoordinatorClient<tonic::transport::Channel>,
    ) -> anyhow::Result<()> {
        self.n_reduce = client
            .get_reduce_count(tonic::Request::new(GetReduceCountArgs {}))
            .await?
            .into_inner()
            .reduce_count;

        loop {
            let reply = client
                .request_task(tonic::Request::new(RequestTaskArgs {
                    worker_id: (std::process::id() as i32),
                }))
                .await?
                .into_inner();

            if reply.task_type == server::EXITTASK {
                println!("All tasks are done, worker exiting.");
                process::exit(0);
            }

            let mut exit: bool = false;

            if reply.task_type == server::NOTASK {
                // the entire mr job not done, but all
                // map or reduce tasks are executing
            } else if reply.task_type == server::MAPTASK {
                self.do_map(&reply.task_file, reply.task_id);
                exit = client
                    .report_task(tonic::Request::new(ReportTaskArgs {
                        worker_id: std::process::id() as i32,
                        task_type: 0,
                        task_id: reply.task_id,
                    }))
                    .await?
                    .into_inner()
                    .can_exit;
            } else if reply.task_type == server::REDUCETASK {
                self.do_reduce(reply.task_id);
                exit = client
                    .report_task(tonic::Request::new(ReportTaskArgs {
                        worker_id: std::process::id() as i32,
                        task_type: 1,
                        task_id: reply.task_id,
                    }))
                    .await?
                    .into_inner()
                    .can_exit;
            }

            if exit {
                println!("Master exited or all tasks done, worker exiting");
                ()
            }

            let two_hundred_ms = time::Duration::from_millis(200);
            thread::sleep(two_hundred_ms);
        }
    }

    fn write_map_output(self, kva: Vec<KVStore<String, String>>, map_id: i32) {
        let prefix: String = format!("{}/mr-{}", "/tmp", map_id);
        let mut files: Vec<MRFile> = Vec::new();

        // create temp files, use pid to uniquely identify this worker
        for i in 0..self.n_reduce {
            let file_path: String = format!("{}-{}-{}", prefix, i, std::process::id());
            let file = std::fs::OpenOptions::new()
                .read(true)
                .create(true)
                .append(true)
                .open(&file_path)
                .unwrap();
            files.push(MRFile {
                name: file_path,
                file: file,
            });
        }

        for kv in kva {
            let idx: i32 = (i_hash(&kv.key) % self.n_reduce).abs();
            if let Err(e) = write!(
                files[idx as usize].file,
                "{}",
                format!("{}, {}\n", &kv.key, &kv.value)
            ) {
                eprintln!("Could not write to file: {}", e);
            }
        }

        // atomically rename temp files to ensure no one observes partial files
        for i in 0..files.len() {
            let new_path: String = format!("{}-{}", prefix, i);
            fs::rename(&files[i].name, new_path).expect("Could not rename file");
        }
    }

    fn do_map(self, file_path: &String, map_id: i32) {
        let path = Path::new(file_path);
        let s = fs::read_to_string(&path).expect("Could not read file");

        let kva = map(file_path, &s);
        self.write_map_output(kva, map_id);
    }
    fn write_reduce_output(self, kv_map: HashMap<String, Vec<String>>, reduce_id: i32) {
        let file_path: String = format!("{}/mr-out-{}-{}", "/tmp", reduce_id, std::process::id());
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .create(true)
            .append(true)
            .open(&file_path)
            .unwrap();
        for key in kv_map.keys().sorted() {
            if let Err(_) = write!(
                file,
                "{}",
                format!("{}, {}\n", key, reduce(&key, &kv_map[key]))
            ) {
                eprintln!("Could not write to file");
            }
        }

        let new_path: String = format!("/tmp/mr-out-{}", reduce_id);
        fs::rename(file_path, new_path).expect("Could not rename file");
    }

    fn do_reduce(self, reduce_id: i32) {
        let mut kv_map: HashMap<String, Vec<String>> = HashMap::new();

        for path in glob(format!("{}/mr-{}-{}", "/tmp", "*", reduce_id).as_str())
            .unwrap()
            .filter_map(Result::ok)
        {
            let file = File::open(path).unwrap();
            let reader = BufReader::new(file);

            for (_, line) in reader.lines().enumerate() {
                let line = line.unwrap();
                let split = line.split(",");
                let split_vec: Vec<&str> = split.collect();

                kv_map
                    .entry(String::from(split_vec[0]))
                    .or_insert(Vec::new())
                    .push(String::from(split_vec[1]));
            }
        }
        self.write_reduce_output(kv_map, reduce_id)
    }
}

fn map<'a>(_filename: &'a str, contents: &String) -> Vec<KVStore<String, String>> {
    let words = contents.split(" ");

    let mut kva: Vec<KVStore<String, String>> = Vec::new();
    for word in words {
        kva.push(KVStore::new(String::from(word), String::from("1")));
    }

    return kva;
}

fn reduce(_key: &String, values: &Vec<String>) -> String {
    values.len().to_string()
}

fn i_hash<T: Hash>(t: &T) -> i32 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish() as i32
}
