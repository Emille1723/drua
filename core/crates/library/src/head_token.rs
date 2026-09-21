// My reasoning
// - Git remains the source of truth for replicas within the cluster
// - Hand rolled NOTIFY/LISTEN  mechanism should be standardised to use Obix for cross replica comms
// - Events emitted should be what replicas listen for to reconcile with the git repo
// - I'm leaning on persisted events considering:
//     - Replica restarts & replaying events for convergence
//     - One stop for ensuring notify/listen and event persistence to be used by said replica restarts
// - Single-row table that stores the current head of the git repo (Updated within the successful write & push flow)
// - Replicas should record their last event consumed
// - My Intended flow (so far):
//     - Writing Replica
//         - Successful write triggers push. Successful push to be acked with persisted event
//         - Published HEAD is persisted to table: 'latest_published_head'
//     - Reading Replica
//         - Event Consumption
//             - Check if the latest published head is contained locally
//             - If contained, do nothing
//             - Else pull upstream
//                 - Prevent reads until the pull upstream completes
//             - Update the record of the last consumed event by the replica
//         - Reads:
//             - Although this acts as the intended method for convergence across the replicas
//             - I like the idea of having the read do a verification before returning the content
//             - So the reads acts as a last checker to guarantee ryw: ensure local head parity with the remote head
//                 - Query the current remote head and check if the remote head is contained locally
//                 - Proceed with read as per usual if contained, else pull upstream before continuing to read
//             - The consideration is:
//                 - Consumed event: Signal to verify and converge if needed
//                 - Read trigger: Ensure gurantee of ryw before committing to returning blob response
//                 - Reads are hot so this is not intended to be the default way the replicas converge
//                     - It gives me more confidence in the ryw guarantee having a 'fail safe' at the read trigger
//     - Replica Restarts:
//         - On restart, consider pulling upstream immediately
//         - Continue to replaying events from last recorded consumed event + 1
//         - Consumption of replayed events until catch up to latest event now becomes:
//             - A verification instead and a pull upstream only if necessary
//             - Instead of (n replicas) * (n pulls from upstream)

use obix::{
    EventSequence,
    MailboxConfig,
    out::Outbox
};
use serde::{
    Serialize,
    Deserialize
};
use futures::stream::StreamExt;
use sqlx::PgPool;
use crate::LibraryError;
use tokio::task::JoinHandle;
use crate::GitEngine;
use std::sync::Arc;

// define event types
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
enum SpaceEvent {
    HeadChanged { new_head: String },
}

type BaseLibraryResult<T> = Result<T, LibraryError>;

pub struct HeadToken {
    outbox: Outbox<SpaceEvent>
}

impl HeadToken {
    pub async fn init(pool: PgPool) -> BaseLibraryResult<Self> {
        let outbox = Outbox::<SpaceEvent>::init(&pool, MailboxConfig::builder().build().expect("Couldn't build MailboxConfig")).await?;
        Ok(Self {
            outbox
        })
    }

    // use of obix should remove the need of the hand rolled notify/listen
    pub async fn start_listeners(&self, git_engine: Arc<GitEngine>) -> JoinHandle<()> {
        let mut listener = self.outbox.listen_persisted(None);

        tokio::spawn(async move {
            println!("LISTENER: started");

            loop {
                println!("LISTENER: waiting");

                match listener.next().await {
                    Some(Ok(evt)) => {
                        println!("LISTENER: received event");

                        match &evt.payload {
                            Some(SpaceEvent::HeadChanged { new_head }) => {
                                println!("EVENT RECEIVED: {new_head}");
                                git_engine.local_converge(Some(new_head.clone())).await;
                            }
                            None => {
                                println!("LISTENER: event had no payload");
                            }
                        }
                    }

                    Some(Err(err)) => {
                        eprintln!("LISTENER ERROR: {err}");
                    }

                    None => {
                        eprintln!("LISTENER: stream ended");
                        break;
                    }
                }
            }
        })
    }

    // Update the current remote head and emit the event for replicas to consume & converge
    pub async fn update_published_head() { }

    pub async fn publish_persisted_head(
        &self,
        head_token_hash: String,
    ) -> anyhow::Result<()> {
        println!("PUB: begin {head_token_hash}");

        let mut op = self.outbox.begin_op().await?;

        println!("PUB: publishing {head_token_hash}");

        self.outbox
            .publish_persisted_in_op(
                &mut op,
                SpaceEvent::HeadChanged {
                    new_head: head_token_hash.clone(),
                },
            )
            .await?;

        println!("PUB: committing {head_token_hash}");

        op.commit().await?;

        println!("PUB: committed {head_token_hash}");

        Ok(())
    }
}
