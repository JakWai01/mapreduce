use futures::{
    future::{self, Ready},
    prelude::*,
};
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
use tarpc::{
    client, context,
    server::{self, incoming::Incoming, Channel},
};

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
    async fn request_task(args: &'static RequestTaskArgs) -> RequestTaskReply;
    async fn report_task(args: &'static ReportTaskArgs) -> ReportTaskReply;
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
        args: &'static RequestTaskArgs,
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
        args: &'static ReportTaskArgs,
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

    // coordinator::make_coordinator(args, 12);
    let (client_transport, server_transport) = tarpc::transport::channel::unbounded();

    let server = server::BaseChannel::with_defaults(server_transport);
    tokio::spawn(server.execute(Coordinator::new().serve()));

    // WorldClient is generated by the #[tarpc::service] attribute. It has a constructor `new`
    // that takes a config and any Transport as input.
    let client = ProtocolClient::new(client::Config::default(), client_transport).spawn();

    worker(&client).await?;
    // The client has an RPC method for each RPC defined in the annotated trait. It takes the same
    // args as defined, with the addition of a Context, which is always the first arg. The Context
    // specifies a deadline and trace information which can be helpful in debugging requests.
    // let hello = client.hello(context::current(), "Stim".to_string()).await?;

    // println!("{hello}");

    Ok(())
}

async fn worker(client: &ProtocolClient) -> anyhow::Result<()> {
    // let hello = client.hello(context::current(), "Jakob".to_string()).await?;

    // println!("{hello}");
    Ok(())
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
        std::fs::remove_file(path).expect(format!("Cannot remove file: {}", path.display()));
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

fn reduce(_key: &String, values: HashMap<String, String>) -> String {
    values.len().to_string()
}

fn i_hash<T: Hash>(t: &T) -> i32 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish() as i32
}

fn do_map(file_path: &String, map_id: i32) {
    let path = Path::new(file_path);
    let display = path.display();
    
    let mut s = fs::read_to_string(&path).expect("Could not read file");

    let kva = map(file_path, &s);
    // write_map_output(kva, map_id);
}


// Just write the things to files, done
fn write_map_output(kva: Vec<KVStore<String, String>>, map_id: i32, n_reduce: i32) {
    let prefix: String = format!("{}/mr-{}", "/tmp", map_id);
    let files: Vec<MRFile> = Vec::new();

    // create temp files, use pid to uniquely identify this worker
    for i in 0..n_reduce {
        let file_path: String = format!("{}-{}-{}", prefix, i, std::process::id());
        let mut file = std::fs::OpenOptions::new().read(true).create(true).append(true).open(file_path).unwrap();
        files.push(MRFile{
            name: file_path,
            file: file
        });
    }

    // write map outputs to temp files
    for kv in kva {
        let idx: i32 = i_hash(&kv.key) % n_reduce;
        if let Err(e) = write!(files[idx as usize].file, "{}", format!("{}, {}\n", &kv.key, &kv.value)) {
            eprintln!("Could not write to file: {}", e);
        }
    }

    // atomically rename temp files to ensure no one observes partial files
    for i in 0..files.len() {
        let new_path: String = format!("{}-{}", prefix, i);
        fs::rename(files[i].name, new_path);
    }
}

fn do_reduce(reduce_id: String) {
    let kv_map: HashMap<String, String> = HashMap::new(); 
    let kv: KVStore<String, String>;

    for path in glob(format!("{}/mr-{}-{}", "/tmp", "*", reduce_id)).unwrap().filter_map(Result::ok) {
        let file = File::open(path).unwrap();
        let reader = BufReader::new(file);

        for (index, line) in reader.lines().enumerate() {
            let line = line.unwrap();

            let mut split = line.split(",");
            let split_vec: Vec<&str> = split.collect();

            kv_map.insert(String::from(split_vec[0]), String::from(split_vec[1]));
        }
    }

    // write_reduce_output(reducef, kv_map, reduce_id)
}

fn write_reduce_output(kv_map: HashMap<String, String>, reduce_id: i32) {
    // sort the kv_map by key
    let mut keys: Vec<String>;
    for (key, value) in kv_map.into_iter() {
        keys.push(key);
    }
    keys.sort();

    // Create temp file
    let file_path: String = format!("{}/mr-out-{}-{}", "/tmp", reduce_id, std::process::id());
    let mut file = std::fs::OpenOptions::new().read(true).create(true).append(true).open(file_path).unwrap();

    for key in keys {
        if let Err(e) = write!(file, "{}", format!("{}, {}\n", key, reduce(&key, kv_map))) {
            eprintln!("Could not write to file");
        }
    }

    let new_path: String = format!("mr-out-{}", reduce_id);
    fs::rename(file_path, new_path);
}