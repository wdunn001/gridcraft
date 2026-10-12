//! Shared editing for GridCraft, over the CraftCollab relay.
//!
//! Three contracts keep this from hardcoding a vendor. The engine hands us a
//! local command through [`gridcraft_engine::CollabSink`] without knowing we
//! exist. The transport is [`craft_collab::Hub`]. A credential arrives through
//! [`TokenSource`]. Each has one implementation here, and another deployment
//! can supply its own without touching the engine or the UI.
//!
//! The relay never holds a document. It assigns a strictly increasing sequence
//! to every edit and fans it out, and each peer applies edits in that order.
//! That suits GridCraft because its commands already carry their own cell
//! address, so an edit means the same thing wherever it lands.

use std::sync::Arc;
use std::sync::mpsc::{Receiver, Sender, channel};

use craft_collab::native::NativeHub;
use craft_collab::{Effect, Hub, ServerMsg, Session as Wire};
use gridcraft_engine::{CollabSink, Session as Engine};
use serde_json::Value as Json;

/// Where the relay credential comes from.
///
/// A client never holds the signing secret. The relay verifies an HMAC that
/// the Nextcloud app produced, so all a client can do is present a token it
/// was handed. Anything able to mint tokens could forge any file id and any
/// permission bits, and that is why the key stays server-side.
pub trait TokenSource {
    /// The relay root, such as `http://192.168.1.88:9444`.
    fn relay(&self) -> Option<String>;
    /// An opaque signed token scoped to one file.
    fn token(&self) -> Option<String>;
    /// The name other peers see in the roster.
    fn display(&self) -> String;
}

/// Reads the environment.
///
/// This is the testing path and the one `craft-collab`'s `join` example uses,
/// so a token minted against the live Nextcloud works here unchanged. A
/// desktop login flow is a second implementation of the same trait.
pub struct EnvToken;

fn env(key: &str) -> Option<String> {
    std::env::var(key).ok().filter(|s| !s.trim().is_empty())
}

impl TokenSource for EnvToken {
    fn relay(&self) -> Option<String> {
        env("CRAFTCOLLAB_RELAY")
    }
    fn token(&self) -> Option<String> {
        env("CRAFTCOLLAB_TOKEN")
    }
    fn display(&self) -> String {
        env("CRAFTCOLLAB_DISPLAY").unwrap_or_else(|| "GridCraft".to_owned())
    }
}

/// The engine-facing half, held by the engine as an `Arc<dyn CollabSink>`.
///
/// It only forwards onto a channel. Doing network work here would run it
/// inside the command that triggered it and stall a frame.
struct Outbound(Sender<(String, Json)>);

impl CollabSink for Outbound {
    fn on_local(&self, id: &str, params: &Json) {
        // A closed channel means the link was dropped, which is not an error.
        let _ = self.0.send((id.to_owned(), params.clone()));
    }
}

/// The room name the relay uses, read out of the token.
///
/// The relay keys a room on the Nextcloud file id and never on a path, so a
/// rename or a move mid-session does not split the peers.
/// `craft_collab::Session` drops any message whose doc does not match its own,
/// so getting this wrong looks exactly like a relay that answers nothing.
///
/// The token is `base64url(payload).signature`. Reading the payload needs no
/// key and proves nothing. It is parsed here only to learn the file id, and
/// the relay still verifies the signature before honouring anything.
fn doc_from_token(token: &str) -> Option<String> {
    let payload = token.split('.').next()?;
    let json = base64url(payload)?;
    let v: Json = serde_json::from_slice(&json).ok()?;
    let fid = v.get("fid")?.as_u64()?;
    Some(format!("doc:{fid}"))
}

/// Decodes unpadded base64url, hand-rolled to avoid a dependency for one call.
fn base64url(s: &str) -> Option<Vec<u8>> {
    let mut out = Vec::with_capacity(s.len() * 3 / 4);
    let mut acc: u32 = 0;
    let mut bits: u32 = 0;
    for b in s.bytes() {
        let six: u8 = match b {
            b'A'..=b'Z' => b - b'A',
            b'a'..=b'z' => b - b'a' + 26,
            b'0'..=b'9' => b - b'0' + 52,
            b'-' => 62,
            b'_' => 63,
            b'=' => continue,
            _ => return None,
        };
        acc = (acc << 6) | u32::from(six);
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((acc >> bits) as u8);
        }
    }
    Some(out)
}

/// The lock scope an edit belongs to, or `None` when it is not region-shaped.
///
/// A scope is opaque to the relay and each app reads its own. For a
/// spreadsheet the natural unit is the address the command already carries,
/// so two people editing different cells never contend.
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
    // The socket runs on a task inside this runtime, so the runtime has to
    // outlive the hub. Dropping it first closes the connection underneath us.
    _rt: tokio::runtime::Runtime,
    hub: NativeHub,
    wire: Wire,
    local: Receiver<(String, Json)>,
    /// Lines for the user, drained by the app each frame.
    pub notices: Vec<String>,
    /// Who else is in the document, for the status bar.
    pub roster: Vec<String>,
}

impl Link {
    /// Connects and attaches to the engine.
    ///
    /// Returns `None` when no credential is configured or the relay cannot be
    /// reached, and the app then behaves exactly as it did before. Shared
    /// editing is an addition, never a precondition for opening a file.
    pub fn attach(engine: &mut Engine, src: &dyn TokenSource) -> Option<Link> {
        let relay = src.relay()?;
        let token = src.token()?;
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
        let hub = match rt.block_on(NativeHub::connect(&relay, &token)) {
            Ok(h) => h,
            Err(e) => {
                log::warn!("collab: cannot reach the relay at {relay}: {e}");
                return None;
            }
        };

        // Nothing is sent here on purpose. The relay admits a peer from the
        // token in the handshake and has no handler for a client-sent join.
        // Sending one raced socketioxide's namespace registration, lost, and
        // cost a duplicate session every time. The relay speaks first.
        let wire = Wire::new(doc.clone());

        let (tx, local) = channel();
        engine.collab = Some(Arc::new(Outbound(tx)));
        log::info!("collab: joining {doc} on {relay}");
        Some(Link { _rt: rt, hub, wire, local, notices: Vec::new(), roster: Vec::new() })
    }

    /// Whether the transport still believes it is connected.
    pub fn connected(&self) -> bool {
        self.hub.connected()
    }

    /// One frame's work: send what the user did, apply what arrived.
    ///
    /// Called from the app's per-frame hook. Everything here is a channel
    /// operation, so no IO happens on the UI thread.
    pub fn pump(&mut self, engine: &mut Engine) {
        // Outbound first, so a local edit is on its way before time goes on
        // applying remote ones.
        while let Ok((id, params)) = self.local.try_recv() {
            let scope = scope_of(&params);
            let msg = self.wire.submit(id, params, scope);
            if let Err(e) = self.hub.send(msg) {
                self.notices.push(format!("that edit did not reach the others: {e}"));
            }
        }

        for msg in self.hub.poll() {
            // The relay names the room, and a session discards every message
            // whose doc does not match its own. The token gave us a starting
            // guess so the first frame could already submit. The relay's own
            // answer replaces it, so a change in how rooms are named on that
            // side cannot silently mute this client.
            if matches!(msg, ServerMsg::Joined { .. }) {
                let named = msg.doc().to_owned();
                if named != self.wire.doc() {
                    log::info!("collab: the relay named the room {named}");
                    self.wire = Wire::new(named);
                }
            }
            let effects = self.wire.handle(msg);
            for effect in effects {
                self.act(engine, effect);
            }
        }
    }

    fn act(&mut self, engine: &mut Engine, effect: Effect) {
        match effect {
            // The whole point. Applied through `apply_remote` so the engine
            // does not hand it straight back as a local edit.
            Effect::Apply { command, params } => {
                if let Err(e) = engine.apply_remote(&command, params) {
                    log::warn!("collab: a remote {command} did not apply: {e}");
                    self.notices.push(format!("could not apply a change from another editor: {e}"));
                }
            }
            Effect::Confirmed { .. } => {}
            Effect::Replace { document, seq } => {
                // The relay holds no documents yet, so this carries null and
                // there is nothing to install. Logged rather than dropped,
                // because a non-null one means the relay grew a snapshot store
                // and this arm needs writing.
                if document.is_null() {
                    log::debug!("collab: resynced to seq {seq} with no snapshot");
                } else {
                    log::warn!("collab: a snapshot arrived at seq {seq} and GridCraft cannot install one yet");
                }
            }
            Effect::LockHeld { .. } | Effect::LockDropped { .. } | Effect::RemoteLocks { .. } => {}
            Effect::LockRefused { scope, holder } => {
                self.notices.push(format!("{holder} is editing {scope}"));
            }
            Effect::Roster { roster } => {
                self.roster = roster.into_iter().map(|p| p.display).collect();
            }
            Effect::Notice { detail } => {
                log::info!("collab: {detail}");
                self.notices.push(detail);
            }
            // The session asks for its own resyncs this way.
            Effect::Send { msg } => {
                if let Err(e) = self.hub.send(msg) {
                    log::warn!("collab: a resync request never left: {e}");
                }
            }
        }
    }
}
