use crate::node::NodeId;

pub trait MessageSender<M> {
    async fn send_node_message(&self, from: NodeId, to: NodeId, message: M) -> anyhow::Result<()>;
}
