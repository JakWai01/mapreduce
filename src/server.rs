use tonic::{transport::Server, Request, Response, Status};

use map_reduce::coordinator_server::{Coordinator, CoordinatorServer};
use map_reduce::{
    GetReduceCountArgs, GetReduceCountReply, ReportTaskArgs, ReportTaskReply, RequestTaskArgs,
    RequestTaskReply,
};

pub mod map_reduce {
    tonic::include_proto!("mapreduce");
}

#[derive(Debug, Default)]
pub struct MyCoordinator {}

#[tonic::async_trait]
impl Coordinator for MyCoordinator {
    async fn get_reduce_count(
        &self,
        request: Request<GetReduceCountArgs>,
    ) -> Result<Response<GetReduceCountReply>, Status> {
        println!("Got a request: {:?}", request);

        let reply = map_reduce::GetReduceCountReply { reduce_count: 42 };

        Ok(Response::new(reply))
    }

    async fn request_task(
        &self,
        request: Request<RequestTaskArgs>,
    ) -> Result<Response<RequestTaskReply>, Status> {
        println!("Got a request: {:?}", request);

        let reply = map_reduce::RequestTaskReply {
            task_type: 1,
            task_id: 2,
            task_file: String::from("testfile"),
        };

        Ok(Response::new(reply))
    }

    async fn report_task(
        &self,
        request: Request<ReportTaskArgs>,
    ) -> Result<Response<ReportTaskReply>, Status> {
        println!("Got a request: {:?}", request);

        let reply = map_reduce::ReportTaskReply { can_exit: true };

        Ok(Response::new(reply))
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let addr = "[::1]:50051".parse()?;
    let coordinator = MyCoordinator::default();

    Server::builder()
        .add_service(CoordinatorServer::new(coordinator))
        .serve(addr)
        .await?;

    Ok(())
}
