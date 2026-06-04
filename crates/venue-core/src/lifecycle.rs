use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Lifecycle {
    Connecting,
    Connected,
    Disconnecting,
    Disconnected,
    Ping,
    Pong,
    Subscription,
    Unsubscription,
    Heartbeat,
    SubscriptionStatus,
    Error,
    Warning,
    Info,
    Debug,
}