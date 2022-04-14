use tonic::{transport::Server, Request, Response, Status};

use map_reduce::coordinator_server::{Coordinator, CoordinatorServer};
use map_reduce::{
    GetReduceCountArgs, GetReduceCountReply, ReportTaskArgs, ReportTaskReply, RequestTaskArgs,
    RequestTaskReply,
};

use std::sync::{Arc, Mutex, RwLock};
use std::process;
use std::time;
use std::thread;
use glob::glob;
use std::fs;
use std::path::Path;
use std::env;

pub mod map_reduce {
    tonic::include_proto!("mapreduce");
}

#[derive(Clone, Debug)]
struct Task {
    typ: i32,
    status: i32,
    index: i32,
    file: String,
    worker_id: i32,
}

#[derive(Debug, Default, Clone)]
pub struct MRCoordinator {
    mu: Arc<Mutex<Vec<Task>>>,
    map_tasks: Arc<RwLock<Vec<Task>>>,
    reduce_tasks: Arc<RwLock<Vec<Task>>>,
    n_map: Arc<RwLock<i32>>,
    n_reduce: Arc<RwLock<i32>>,
}

#[tonic::async_trait]
impl Coordinator for MRCoordinator {
    async fn get_reduce_count(
        &self,
        request: Request<GetReduceCountArgs>,
    ) -> Result<Response<GetReduceCountReply>, Status> {
        println!("Got a request: {:?}", request);

        let _lock = self.mu.lock();

        let reply = map_reduce::GetReduceCountReply {
            reduce_count: self.reduce_tasks.read().unwrap().len() as i32,
        };

        Ok(Response::new(reply))
    }

    async fn request_task(
        &self,
        request: Request<RequestTaskArgs>,
    ) -> Result<Response<RequestTaskReply>, Status> {
        println!("Got a request: {:?}", request);

        let _lock = &self.mu.lock();

        let task: Task;
        println!("n_map {}", *self.n_map.read().unwrap());
        println!("n_reduce {}", *self.n_reduce.read().unwrap());
        let id = request.into_inner().worker_id;
        println!("My worker id {}", id);
        if *self.n_map.read().unwrap() > 0 {
            task = self.select_task(&mut *self.map_tasks.write().unwrap(), id);
        } else if *self.n_reduce.read().unwrap() > 0 {
            task = self.select_task(&mut *self.reduce_tasks.write().unwrap(), id);
        } else {
            task = Task {
                typ: 2,
                status: 0,
                index: -1,
                file: String::from(""),
                worker_id: -1,
            }
        }

        let reply = map_reduce::RequestTaskReply {
            task_type: task.typ,
            task_id: task.index,
            task_file: task.file.clone(),
        };

        self.wait_for_task(task);

        Ok(Response::new(reply))
    }

    async fn report_task(
        &self,
        request: Request<ReportTaskArgs>,
    ) -> Result<Response<ReportTaskReply>, Status> {
        println!("Got a request: {:?}", request);
        
        let req = request.into_inner();

        let task_id: i32 = req.task_type;
        let worker_id: i32 = req.worker_id;
        let task_type: i32 = req.task_type;
       
        let _lock = self.mu.lock();

        let mut task: Task;
        if task_id == 0 {
            println!("0");
            task = self.map_tasks.read().unwrap()[task_id as usize].clone();
        } else if req.task_type == 1 {
            println!("1");
            task = self.reduce_tasks.read().unwrap()[task_id as usize].clone();
        } else {
            println!(
                "{}",
                format!("Incorrect task type to report: {}", req.task_type)
            );
            process::exit(1)
        }
        
        println!("Worker ID {}", worker_id);
        println!("Task worker_id {}", task.worker_id);
        println!("Task status {} (should be 2)", task.status);
        if worker_id == task.worker_id && task.status == 2 {
            task.status = 0;
            if task_type == 0 && *self.n_map.read().unwrap() > 0 {
                let mut n_map = self.n_map.write().unwrap();
                println!("{}", *self.n_map.read().unwrap());
                *n_map -= 1; 
                println!("{}", *self.n_map.read().unwrap())
            } else if task_type == 1 && *self.n_reduce.read().unwrap() > 0 {
                let mut n_reduce = self.n_reduce.write().unwrap();
                *n_reduce -= 1;
            }
        }

        if *self.n_map.read().unwrap() == 0 && *self.n_reduce.read().unwrap() == 0 {
            println!("Can exit");
            let reply = map_reduce::ReportTaskReply { can_exit: true };
            Ok(Response::new(reply))
        } else {
            println!("Can't exit");
            let reply = map_reduce::ReportTaskReply { can_exit: false };
            Ok(Response::new(reply))
        }
    }
}

impl MRCoordinator {
    pub fn new() -> MRCoordinator {
        MRCoordinator {
            mu: Arc::new(Mutex::new(Vec::new())),
            map_tasks: Arc::new(RwLock::new(Vec::new())),
            reduce_tasks: Arc::new(RwLock::new(Vec::new())),
            n_map: Arc::new(RwLock::new(0)),
            n_reduce: Arc::new(RwLock::new(0)),
        }
    }

    fn done(&self) -> bool {
        let _lock = self.mu.lock();

        return *self.n_map.read().unwrap() == 0 && *self.n_reduce.read().unwrap() == 0;
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
    fn select_task(&self, task_list: &mut Vec<Task>, worker_id: i32) -> Task {

        println!("selectTask WorkerId {}", worker_id);

        for i in 0..task_list.len() {
            if task_list[i].status == 1 {
                println!("INSIDE");
                task_list[i].status = 2;
                task_list[i].worker_id = worker_id;
                return task_list[i].clone();
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args: Vec<String> = env::args().collect();

    if args.len() < 2 {
        eprintln!("Usage: mrmaster inputfiles...");
        process::exit(1);
    }

    args.remove(0);

    let addr = "[::1]:50051".parse()?;
    let coordinator: MRCoordinator = make_coordinator(args, 2); 
    Server::builder()
        .add_service(CoordinatorServer::new(coordinator))
        .serve(addr)
        .await?;

    Ok(())
}

pub fn make_coordinator(files: Vec<String>, n_reduce: i32) -> MRCoordinator {
    let mut coordinator = MRCoordinator::new();
    let n_map = files.len();
    *coordinator.n_map.write().unwrap() = n_map as i32;
    *coordinator.n_reduce.write().unwrap() = n_reduce;
    coordinator.map_tasks = Arc::new(RwLock::new(Vec::new()));
    coordinator.reduce_tasks = Arc::new(RwLock::new(Vec::new()));

    for i in 0..n_map {
        let m_task = Task {
            typ: 0,
            status: 1,
            index: i as i32,
            file: files[i].clone(),
            worker_id: -1,
        };
        coordinator.map_tasks.write().unwrap().push(m_task);
    }
    for i in 0..n_reduce {
        let r_task = Task {
            typ: 1,
            status: 1,
            index: i as i32,
            file: String::from(""),
            worker_id: -1,
        };
        coordinator.reduce_tasks.write().unwrap().push(r_task);
    }

    for path in glob("mr-out*").unwrap().filter_map(Result::ok) {
        std::fs::remove_file(&path).expect(format!("Cannot remove file: {}", path.display()).as_str());
    }

    // fs::remove_dir_all("/tmp").expect("Cannot remove temp directory");

    // fs::create_dir_all(Path::new("/tmp")).expect("Cannot create temp directory");

    coordinator
}