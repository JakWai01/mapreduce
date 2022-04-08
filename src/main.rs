use futures::{
    future::{self, Ready},
};
use itertools::Itertools;
use std::io::{BufReader, BufRead};
use std::collections::HashMap;
use std::fs::File;
use glob::glob;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::env;
use std::fs;
use std::path::Path;
use std::process;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time;
use std::io::Write;
use tarpc::{
    client, context,
    server::{self, Channel},
};

#[derive(Clone, Copy)]
struct Worker {
    n_reduce: i32
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

type TaskType = i32;
type TaskStatus = i32;

#[derive(Debug)]
struct GetReduceCountArgs {}

#[derive(Debug)]
struct GetReduceCountReply {
    reduce_count: i32,
}

#[derive(Debug)]
struct RequestTaskArgs {
    worker_id: i32,
}

#[derive(Debug)]
struct RequestTaskReply {
    task_type: TaskType,
    task_id: i32,
    task_file: String,
}

#[derive(Debug)]
struct ReportTaskArgs {
    worker_id: i32,
    task_type: TaskType,
    task_id: i32,
}

#[derive(Debug)]
struct ReportTaskReply {
    can_exit: bool,
}

#[derive(Clone)]
struct Task {
    typ: TaskType,
    status: TaskStatus,
    index: i32,
    file: String,
    worker_id: i32,
}

#[derive(Clone)]
struct Coordinator {
    mu: Arc<Mutex<Vec<Task>>>,
    map_tasks: Vec<Task>,
    reduce_tasks: Vec<Task>,
    n_map: i32,
    n_reduce: i32,
}

struct MRFile {
    name: String,
    file: fs::File,
}

// This is the service definition. It looks like a trait definition.
// It defines one RPC, hello, which takes one arg, name, and returns a String.
#[tarpc::service]
trait Protocol {
    async fn get_reduce_count(args: &'static GetReduceCountArgs) -> GetReduceCountReply;
    async fn request_task(args: RequestTaskArgs) -> RequestTaskReply;
    async fn report_task(args: ReportTaskArgs) -> ReportTaskReply;
}

impl Coordinator {
    fn new() -> Coordinator {
        Coordinator {
            mu: Arc::new(Mutex::new(Vec::new())),
            map_tasks: Vec::new(),
            reduce_tasks: Vec::new(),
            n_map: 0,
            n_reduce: 0,
        }
    }

    fn done(&self) -> bool {
        let _lock = self.mu.lock();

        return self.n_map == 0 && self.n_reduce == 0;
    }

    // Task status
    // 1 -> NotStarted
    // 2 -> Executing
    // -1 -> NoTask
    // 0 -> Finished

    // Typ
    // -1 NoTask
    // 0 MapTask
    // 1 ReduceTask
    fn select_task(&self, task_list: &Vec<Task>, worker_id: i32) -> Task {
        let mut task: Task;

        for i in 0..task_list.len() {
            if task_list[i].status == 1 {
                task = task_list[i].clone();
                task.status = 2;
                task.worker_id = worker_id;
                return task;
            }
        }

        Task {
            typ: -1,
            status: 0,
            index: -1,
            file: String::from(""),
            worker_id: -1,
        }
    }

    async fn wait_for_task(&self, mut task: Task) {
        if task.typ != 0 && task.typ != 1 {
            return;
        }

        let ten_seconds = time::Duration::from_millis(10000);
        thread::sleep(ten_seconds);

        let _lock = self.mu.lock().unwrap();

        // Timeout
        if task.status == 2 {
            task.status = 1;
            task.worker_id = -1;
        }
    }
}

impl Protocol for Coordinator {
    type GetReduceCountFut = Ready<GetReduceCountReply>;

    fn get_reduce_count(
        self,
        _: context::Context,
        args: &'static GetReduceCountArgs,
    ) -> Self::GetReduceCountFut {
        let _lock = self.mu.lock();

        future::ready(GetReduceCountReply {
            reduce_count: self.reduce_tasks.len() as i32,
        })
    }

    type RequestTaskFut = Ready<RequestTaskReply>;

    fn request_task(
        self,
        _: context::Context,
        args: RequestTaskArgs,
    ) -> Self::RequestTaskFut {
        let _lock = &self.mu.lock();

        let task: Task;
        if self.n_map > 0 {
            task = self.select_task(&self.map_tasks, args.worker_id);
        } else if self.n_reduce > 0 {
            task = self.select_task(&self.reduce_tasks, args.worker_id);
        } else {
            task = Task {
                typ: 2,
                status: 0,
                index: -1,
                file: String::from(""),
                worker_id: -1,
            }
        }

        let reply = RequestTaskReply {
            task_type: task.typ,
            task_id: task.index,
            task_file: task.file.clone(),
        };

        self.wait_for_task(task);

        future::ready(reply)
    }

    type ReportTaskFut = Ready<ReportTaskReply>;

    fn report_task(
        mut self,
        _: context::Context,
        args: ReportTaskArgs,
    ) -> Self::ReportTaskFut {
        let _lock = self.mu.lock();

        let mut task: Task;
        if args.task_type == 0 {
            task = self.map_tasks[args.task_id as usize].clone();
        } else if args.task_type == 1 {
            task = self.reduce_tasks[args.task_id as usize].clone();
        } else {
            println!(
                "{}",
                format!("Incorrect task type to report: {}", args.task_type)
            );
            process::exit(1)
        }

        if args.worker_id == task.worker_id && task.status == 2 {
            task.status = 0;
            if args.task_type == 0 && self.n_map > 0 {
                self.n_map -= 1;
            } else if args.task_type == 1 && self.n_reduce > 0 {
                self.n_reduce -= 1;
            }
        }

        if self.n_map == 0 && self.n_reduce == 0 {
            future::ready(ReportTaskReply { can_exit: true })
        } else {
            future::ready(ReportTaskReply { can_exit: false }) } }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        eprintln!("Usage: mrmaster inputfiles...");
        process::exit(1);
    }

    let (client_transport, server_transport) = tarpc::transport::channel::unbounded();

    let server = server::BaseChannel::with_defaults(server_transport);
    let coordinator: Coordinator = Coordinator::new();
    tokio::spawn(server.execute(coordinator.serve()));

    let client = ProtocolClient::new(client::Config::default(), client_transport).spawn();

    let mut w = Worker{
        n_reduce: 0,
    };

    w.worker(&client).await?;
    
    while coordinator.done() == false {
        let ten_seconds = time::Duration::from_secs(10);
        thread::sleep(ten_seconds);
    }
    Ok(())
}

impl Worker {
    async fn worker(&mut self, client: &ProtocolClient) -> anyhow::Result<()> {
        self.n_reduce = client.get_reduce_count(context::current(), &GetReduceCountArgs{}).await?.reduce_count;

        loop {
            let args = RequestTaskArgs{worker_id: std::process::id() as i32};
            let reply = client.request_task(context::current(), args).await?;

            if reply.task_type == 2 {
                println!("All tasks are done, worker exiting.")
            }

            let mut exit: bool = false;

            if reply.task_type == -1 {
                // the entire mr job not done, but all
                // map or reduce tasks are executing
            } else if reply.task_type == 0 {
                self.do_map(&reply.task_file, reply.task_id);
                exit = client.report_task(context::current(), ReportTaskArgs{task_id: std::process::id() as i32, task_type: 1, worker_id: reply.task_id}).await?.can_exit;
            } else if reply.task_type == 1 {
                do_reduce(reply.task_id);
                exit = client.report_task(context::current(), ReportTaskArgs{task_id: std::process::id() as i32, task_type: 2, worker_id: reply.task_id}).await?.can_exit;
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
            let file = std::fs::OpenOptions::new().read(true).create(true).append(true).open(&file_path).unwrap();
            files.push(MRFile{
                name: file_path,
                file: file
            });
        }

        for kv in kva {
            let idx: i32 = i_hash(&kv.key) % self.n_reduce;
            if let Err(e) = write!(files[idx as usize].file, "{}", format!("{}, {}\n", &kv.key, &kv.value)) {
                eprintln!("Could not write to file: {}", e);
            }
        }

        // atomically rename temp files to ensure no one observes partial files
        for i in 0..files.len() {
            let new_path: String = format!("{}-{}", prefix, i);
            fs::rename(&files[i].name, new_path);
        }
    }

    fn do_map(self, file_path: &String, map_id: i32) {
        let path = Path::new(file_path);
        let display = path.display();
        
        let mut s = fs::read_to_string(&path).expect("Could not read file");

        let kva = map(file_path, &s);
        self.write_map_output(kva, map_id);
    }
}

fn make_coordinator(files: Vec<String>, n_reduce: i32) -> Coordinator {
    let mut coordinator = Coordinator::new();
    let n_map = files.len();
    coordinator.n_map = n_map as i32;
    coordinator.n_reduce = n_reduce;
    coordinator.map_tasks = Vec::new();
    coordinator.reduce_tasks = Vec::new();

    for i in 0..n_map {
        let m_task = Task {
            typ: 0,
            status: 1,
            index: i as i32,
            file: files[i].clone(),
            worker_id: -1,
        };
        coordinator.map_tasks.push(m_task);
    }
    for i in 0..n_reduce {
        let r_task = Task {
            typ: 1,
            status: 1,
            index: i as i32,
            file: String::from(""),
            worker_id: -1,
        };
        coordinator.reduce_tasks.push(r_task);
    }

    for path in glob("mr-out*").unwrap().filter_map(Result::ok) {
        std::fs::remove_file(&path).expect(format!("Cannot remove file: {}", path.display()).as_str());
    }

    fs::remove_dir_all("/tmp").expect("Cannot remove temp directory");

    fs::create_dir_all(Path::new("/tmp")).expect("Cannot create temp directory");

    coordinator
}

fn map<'a>(_filename: &'a str, contents: &String) -> Vec<KVStore<String, String>> {
    let words = contents.split(" ");

    let mut kva: Vec<KVStore<String, String>> = Vec::new();
    for word in words {
        kva.push(KVStore::new(String::from(word), String::from("1")));
    }

    return kva;
}

fn reduce(_key: &String, values: &HashMap<String, String>) -> String {
    values.len().to_string()
}

fn i_hash<T: Hash>(t: &T) -> i32 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish() as i32
}

fn do_reduce(reduce_id: i32) {
    let mut kv_map: HashMap<String, String> = HashMap::new(); 
    let kv: KVStore<String, String>;

    for path in glob(format!("{}/mr-{}-{}", "/tmp", "*", reduce_id).as_str()).unwrap().filter_map(Result::ok) {
        let file = File::open(path).unwrap();
        let reader = BufReader::new(file);

        for (index, line) in reader.lines().enumerate() {
            let line = line.unwrap();

            let mut split = line.split(",");
            let split_vec: Vec<&str> = split.collect();

            kv_map.insert(String::from(split_vec[0]), String::from(split_vec[1]));
        }
    }

    write_reduce_output(kv_map, reduce_id)
}

fn write_reduce_output(kv_map: HashMap<String, String>, reduce_id: i32) {
    let file_path: String = format!("{}/mr-out-{}-{}", "/tmp", reduce_id, std::process::id());
    let mut file = std::fs::OpenOptions::new().read(true).create(true).append(true).open(&file_path).unwrap();

    for key in kv_map.keys().sorted() {
        if let Err(_) = write!(file, "{}", format!("{}, {}\n", key, reduce(&key, &kv_map))) {
            eprintln!("Could not write to file");
        }
    }

    let new_path: String = format!("mr-out-{}", reduce_id);
    fs::rename(file_path, new_path);
}