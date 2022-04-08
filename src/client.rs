use map_reduce::coordinator_client::CoordinatorClient;
use map_reduce::GetReduceCountArgs;

pub mod map_reduce {
    tonic::include_proto!("mapreduce");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = CoordinatorClient::connect("http://[::1]:50051").await?;

    let request = tonic::Request::new(GetReduceCountArgs {});

    let response = client.get_reduce_count(request).await?;

    println!("Response={:?}", response);

    Ok(())
}

