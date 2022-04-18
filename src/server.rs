use tonic::{transport::Server, Request, Response, Status};

use map_reduce::coordinator_server::{Coordinator, CoordinatorServer};
use map_reduce::{
    GetReduceCountArgs, GetReduceCountReply, ReportTaskArgs, ReportTaskReply, RequestTaskArgs,
    RequestTaskReply,
};

use glob::glob;
use std::env;
use std::process;
use std::sync::{Arc, Mutex, RwLock};
pub mod map_reduce {
    tonic::include_proto!("mapreduce");
}

// Task status
pub const NOTSTARTED: i32 = 1;
pub const EXECUTING: i32 = 2;
pub const FINISHED: i32 = 0;

// Task type
pub const MAPTASK: i32 = 0;
pub const REDUCETASK: i32 = 1;
pub const EXITTASK: i32 = 2;
pub const NOTASK: i32 = -1;

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
        let id = request.into_inner().worker_id;
        if *self.n_map.read().unwrap() > 0 {
            task = self.select_task(&mut *self.map_tasks.write().unwrap(), id);
        } else if *self.n_reduce.read().unwrap() > 0 {
            task = self.select_task(&mut *self.reduce_tasks.write().unwrap(), id);
        } else {
            task = Task {
                typ: EXITTASK,
                status: FINISHED,
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

        Ok(Response::new(reply))
    }

    async fn report_task(
        &self,
        request: Request<ReportTaskArgs>,
    ) -> Result<Response<ReportTaskReply>, Status> {
        println!("Got a request: {:?}", request);
        let req = request.into_inner();

        let task_id: i32 = req.task_id;
        let worker_id: i32 = req.worker_id;
        let task_type: i32 = req.task_type;
        let _lock = self.mu.lock();

        let mut task: Task;

        if task_type == MAPTASK {
            task = self.map_tasks.read().unwrap()[task_id as usize].clone();
        } else if task_type == REDUCETASK {
            task = self.reduce_tasks.read().unwrap()[task_id as usize].clone();
        } else {
            println!(
                "{}",
                format!("Incorrect task type to report: {}", req.task_type)
            );
            process::exit(1)
        }
        if worker_id == task.worker_id && task.status == EXITTASK {
            task.status = 0;
            if task_type == MAPTASK && *self.n_map.read().unwrap() > 0 {
                let mut n_map = self.n_map.write().unwrap();
                *n_map -= 1;
            } else if task_type == REDUCETASK && *self.n_reduce.read().unwrap() > 0 {
                let mut n_reduce = self.n_reduce.write().unwrap();
                *n_reduce -= 1;
            }
        }

        if *self.n_map.read().unwrap() == 0 && *self.n_reduce.read().unwrap() == 0 {
            let reply = map_reduce::ReportTaskReply { can_exit: true };
            Ok(Response::new(reply))
        } else {
            let reply = map_reduce::ReportTaskReply { can_exit: false };
            Ok(Response::new(reply))
        }
    }
}

impl MRCoordinator {
    fn new() -> MRCoordinator {
        MRCoordinator {
            mu: Arc::new(Mutex::new(Vec::new())),
            map_tasks: Arc::new(RwLock::new(Vec::new())),
            reduce_tasks: Arc::new(RwLock::new(Vec::new())),
            n_map: Arc::new(RwLock::new(0)),
            n_reduce: Arc::new(RwLock::new(0)),
        }
    }

    fn select_task(&self, task_list: &mut Vec<Task>, worker_id: i32) -> Task {
        for i in 0..task_list.len() {
            if task_list[i].status == NOTSTARTED {
                task_list[i].status = EXECUTING;
                task_list[i].worker_id = worker_id;
                return task_list[i].clone();
            }
        }

        Task {
            typ: -1,
            status: EXECUTING,
            index: -1,
            file: String::from(""),
            worker_id: -1,
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
    let coordinator: MRCoordinator = make_coordinator(args, 1);
    Server::builder()
        .add_service(CoordinatorServer::new(coordinator))
        .serve(addr)
        .await?;

    Ok(())
}

fn make_coordinator(files: Vec<String>, n_reduce: i32) -> MRCoordinator {
    let mut coordinator = MRCoordinator::new();
    let n_map = files.len();
    *coordinator.n_map.write().unwrap() = n_map as i32;
    *coordinator.n_reduce.write().unwrap() = n_reduce;
    coordinator.map_tasks = Arc::new(RwLock::new(Vec::new()));
    coordinator.reduce_tasks = Arc::new(RwLock::new(Vec::new()));

    for i in 0..n_map {
        let m_task = Task {
            typ: MAPTASK,
            status: NOTSTARTED,
            index: i as i32,
            file: files[i].clone(),
            worker_id: -1,
        };
        coordinator.map_tasks.write().unwrap().push(m_task);
    }
    for i in 0..n_reduce {
        let r_task = Task {
            typ: REDUCETASK,
            status: NOTSTARTED,
            index: i as i32,
            file: String::from(""),
            worker_id: -1,
        };
        coordinator.reduce_tasks.write().unwrap().push(r_task);
    }

    for path in glob("/tmp/mr*").unwrap().filter_map(Result::ok) {
        std::fs::remove_file(&path)
            .expect(format!("Cannot remove file: {}", path.display()).as_str());
    }

    coordinator
}
