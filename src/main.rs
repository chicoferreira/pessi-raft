use log::error;
use std::time::Duration;

mod maelstrom;
mod raft;
mod transport;
mod stateright;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::builder()
        .target(env_logger::Target::Stderr)
        .filter_level(log::LevelFilter::Debug)
        .init();

    let mut server = maelstrom::MaelstromServer::new();
    let (node_id, node_ids) = server.init().await?;

    let mut node = raft::Node::new(node_id, node_ids);
    let mut ticker = tokio::time::interval(Duration::from_millis(50));

    loop {
        tokio::select! {
            msg = server.receive_message() => {
                if let Err(e) = server.handle_message(msg, &mut node).await {
                    error!("Error handling message: {:?}", e);
                }
            }
            _ = ticker.tick() => {
                if let Err(e) = node.tick_periodically(&mut server).await {
                    error!("Error handling timeout: {:?}", e);
                }
            }
            _ = tokio::signal::ctrl_c() => {
                break;
            }
        }
    }

    Ok(())
}
