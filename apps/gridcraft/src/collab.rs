// Shared editing for GridCraft, over the CraftCollab relay.
//
// The merge and the socket live in craft_collab. This file maps a cell address to a CRDT
// text unit: cell.set is lock-free, everything else still takes a region lock. Desktop uses
// NativeHub; the browser crate includes this file and uses WebHub.
// Written as ordinary comments so include! from gridcraft-web is valid.

use std::sync::Arc;
use std::sync::mpsc::{Receiver, Sender, channel};

use craft_collab::{Live, Work};
#[cfg(not(target_arch = "wasm32"))]
use craft_collab::doc_from_token;
use gridcraft_engine::{CollabSink, Session as Engine};
use serde_json::{Value as Json, json};

#[cfg(not(target_arch = "wasm32"))]
use craft_collab::native::NativeHub;
#[cfg(target_arch = "wasm32")]
use craft_collab::web::WebHub;

/// Where the relay credential comes from.
pub trait TokenSource: Send {
    fn join(&self) -> Option<(String, String)>;
}

pub struct EnvToken;

#[cfg(not(target_arch = "wasm32"))]
fn env(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|s| !s.trim().is_empty())
}

#[cfg(not(target_arch = "wasm32"))]
impl TokenSource for EnvToken {
    fn join(&self) -> Option<(String, String)> {
        Some((env("COLLABCRAFT_RELAY")?, env("COLLABCRAFT_TOKEN")?))
    }
}

struct Outbound(Sender<(String, Json)>);

impl CollabSink for Outbound {
    fn on_local(&self, id: &str, params: &Json) {
        let _ = self.0.send((id.to_owned(), params.clone()));
    }
}

fn scope_of(params: &Json) -> Option<String> {
    for key in ["cell", "range", "ref"] {
        if let Some(s) = params.get(key).and_then(Json::as_str)
            && !s.is_empty()
        {
            return Some(s.to_owned());
        }
    }
    None
}

/// A live shared-editing link.
pub struct Link {
    #[cfg(not(target_arch = "wasm32"))]
    live: Live<NativeHub>,
    #[cfg(not(target_arch = "wasm32"))]
    _rt: tokio::runtime::Runtime,
    #[cfg(target_arch = "wasm32")]
    live: Live<WebHub>,
    local: Receiver<(String, Json)>,
    outbound: Sender<(String, Json)>,
    pub notices: Vec<String>,
    pub roster: Vec<(String, String)>,
}

impl Link {
    #[cfg(not(target_arch = "wasm32"))]
    pub fn spawn(src: impl TokenSource + 'static) -> Receiver<Link> {
        let (tx, rx) = channel();
        let _ = std::thread::Builder::new().name("collab-connect".into()).spawn(move || {
            if let Some(link) = Link::connect(&src) {
                let _ = tx.send(link);
            }
        });
        rx
    }

    #[cfg(not(target_arch = "wasm32"))]
    pub fn connect(src: &dyn TokenSource) -> Option<Link> {
        let (relay, token) = src.join()?;
        let Some(doc) = doc_from_token(&token) else {
            log::warn!("collab: the token carries no file id, so there is no room to join");
            return None;
        };

        let rt = match tokio::runtime::Builder::new_multi_thread().worker_threads(1).enable_all().build() {
            Ok(rt) => rt,
            Err(e) => {
                log::warn!("collab: no async runtime: {e}");
                return None;
            }
        };
        let connect = tokio::time::timeout(std::time::Duration::from_secs(5), NativeHub::connect(&relay, &token));
        let hub = match rt.block_on(connect) {
            Ok(Ok(h)) => h,
            Ok(Err(e)) => {
                log::warn!("collab: cannot reach the relay at {relay}: {e}");
                return None;
            }
            Err(_) => {
                log::warn!("collab: the relay at {relay} did not answer in time");
                return None;
            }
        };

        let (outbound, local) = channel();
        log::info!("collab: joining {doc} on {relay}");
        Some(Link {
            live: Live::new(hub, doc),
            _rt: rt,
            local,
            outbound,
            notices: Vec::new(),
            roster: Vec::new(),
        })
    }

    #[cfg(target_arch = "wasm32")]
    pub fn from_web(hub: WebHub, room: &str) -> Link {
        let (outbound, local) = channel();
        log::info!("collab: joining {room} from the browser");
        Link {
            live: Live::new(hub, room),
            local,
            outbound,
            notices: Vec::new(),
            roster: Vec::new(),
        }
    }

    pub fn bind(&mut self, engine: &mut Engine, uid: Option<u64>) {
        engine.collab = Some(Arc::new(Outbound(self.outbound.clone())));
        engine.collab_uid = uid;
        self.seed_from(engine);
    }

    fn seed_from(&mut self, engine: &Engine) {
        let Ok(doc) = engine.doc() else { return };
        let Some(sh) = doc.wb.active() else { return };
        for (cell, c) in sh.cells.iter() {
            let text = c.input_text();
            if !text.is_empty() {
                self.live.texts().seed(&cell.a1(), &text);
            }
        }
    }

    pub fn pump(&mut self, engine: &mut Engine) {
        while let Ok((id, params)) = self.local.try_recv() {
            if id == "cell.set" {
                if let Err(e) = self.send_cell(&params) {
                    self.live.notices.push(format!("that edit did not reach the others: {e}"));
                }
                continue;
            }
            let scope = scope_of(&params);
            if let Err(e) = self.live.submit(id, params, scope) {
                self.live.notices.push(format!("that edit did not reach the others: {e}"));
            }
        }

        for work in self.live.pump() {
            match work {
                Work::Text { scope, rendered } => {
                    if let Err(e) = engine.apply_remote("cell.set", json!({"cell": scope, "input": rendered})) {
                        log::warn!("collab: a remote crdt.text did not apply: {e}");
                        self.live.notices.push(format!("could not apply a change from another editor: {e}"));
                    }
                }
                Work::Command { command, params } => {
                    if let Err(e) = engine.apply_remote(&command, params) {
                        log::warn!("collab: a remote {command} did not apply: {e}");
                        self.live.notices.push(format!("could not apply a change from another editor: {e}"));
                    }
                }
            }
        }
        self.notices = std::mem::take(&mut self.live.notices);
        self.roster.clone_from(&self.live.roster);
    }

    fn send_cell(&mut self, params: &Json) -> craft_collab::Result<()> {
        let cell = params.get("cell").and_then(Json::as_str);
        let input = params.get("input").and_then(Json::as_str);
        match (cell, input) {
            (Some(cell), Some(input)) => {
                if self.live.texts().render(cell).is_none() {
                    self.live.texts().seed(cell, "");
                }
                self.live.replace_text(cell, input)
            }
            _ => self.live.submit("cell.set", params.clone(), scope_of(params)),
        }
    }
}
