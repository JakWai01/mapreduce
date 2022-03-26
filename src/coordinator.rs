use futures::{
    future::{self, Ready},
    prelude::*,
};
use tarpc::{
    client, context,
    server::{self, incoming::Incoming, Channel},
};

// This is the service definition. It looks like a trait definition.
// It defines one RPC, hello, which takes one arg, name, and returns a String.
#[tarpc::service]
trait World {
    // Returns a greeting for name.
    async fn hello(name: String) -> String;
}

#[derive(Clone)]
struct HelloServer;

impl World for HelloServer {
    // Each defined rpc generates two items in trait, a fn that serves the RPC, and
    // and associated type representing the future output by the fn.

    type HelloFut = Ready<String>;

    fn hello(self, _: context::Context, name: String) -> Self::HelloFut {
        future::ready(format!("Hello, {name}!"))
    }
}


pub struct Coordinator {

}

impl Coordinator {

    // Example RPC handler 
    // the arguments and replies are defined in rpc.go
    pub fn Example(&self) {

    }

    async fn server(&self) -> anyhow::Result<()> {
       let (client_transport, server_transport) = tarpc::transport::channel::unbounded();
       
       let server = server::BaseChannel::with_defaults(server_transport);
       tokio::spawn(server.execute(HelloServer.serve()));

       let mut client = WorldClient::new(client::Config::default(), client_transport).spawn();

       let hello = client.hello(context::current(), "Stim".to_string()).await?;

       println!("{hello}");

       Ok(())
    }

    pub fn done(&self) -> bool {
        true
    } 

}

pub fn make_coordinator(files: Vec<String>, n_reduce: i32) -> Coordinator {
    let c = Coordinator {

    };
    c.server();
    c
}


