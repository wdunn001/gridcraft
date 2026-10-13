//! Open and Save against cloud storage.
//!
//! GridCraft holds no provider credential and speaks no provider protocol.
//! Every call goes to the CloudCraft service. That service is the single place
//! a provider is configured and permissions are decided. Replacing Nextcloud
//! with something else changes nothing in this file.
//!
//! This is the interface half of the work. The host application supplies the
//! other half as [`crate::Services::cloud`], for two reasons. Every CloudCraft
//! call blocks, and egui redraws at frame rate. A blocking read of a twenty
//! megabyte workbook inside a frame would freeze the window for seconds. So
//! the interface hands the host a [`Job`], the host runs it on a worker
//! thread, and the answer arrives in an [`Inbox`] that [`pump`] drains on a
//! later frame. Nothing here opens a socket. The web build therefore still
//! compiles, and a test drives every flow with no service running.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

// Re-exported so a host application, and a test, can name the types these
// jobs carry without depending on the crate they come from.
pub use craft_cloud::{Capability, Entry, EntryKind};
use egui::{Align2, Rect, RichText, Sense, Stroke, Ui, pos2, vec2};
use gridcraft_engine::model::Workbook;
use serde_json::{Value as Json, json};

use crate::icons::{self, Icon};
use crate::theme::{self, Tokens};
use crate::{SheetApp, now_ms};

/// How long to wait between asking whether the user has finished linking an
/// account in their browser.
const LINK_POLL_MS: f64 = 1500.0;

/// What a command says when no cloud service is configured. Not a failure of
/// the service: there is no service to fail.
pub const UNAVAILABLE: &str = "no cloud service is configured";

/// Extensions GridCraft opens. The same set the desktop file picker offers.
const OPENS: [&str; 9] = ["xlsx", "xlsm", "xls", "xlsb", "ods", "csv", "tsv", "txt", "json"];

// ---------------------------------------------------------------- the contract with the host

/// Work for the host's worker thread. Each one answers with an [`Event`].
pub enum Job {
    /// Who is signed in, and what the provider behind the service can do.
    Account,
    /// Begin linking a cloud account. The answer carries a URL to open.
    LinkStart,
    /// Whether the user has finished in the browser yet.
    LinkPoll(String),
    /// One folder's contents. An empty path is the user's root.
    List(String),
    /// Everything other people have shared with this user.
    SharedWithMe,
    /// A document's bytes. `into` names the open document a reload replaces.
    Read { entry: Entry, into: Option<u64> },
    /// Save back over `entry`, refusing to overwrite a newer version.
    Write { uid: u64, entry: Entry, bytes: Vec<u8> },
    /// Save under a new name in `folder`.
    Create { uid: u64, folder: String, name: String, bytes: Vec<u8> },
}

/// What a worker reports back.
pub enum Event {
    Account(Result<Account, Fault>),
    /// A link or a sign-in needs the user's browser. `handle` is polled until
    /// they are done, when the service gave one.
    Browser { url: String, handle: Option<String> },
    /// `Ok(true)` means the account is linked now.
    Linked(Result<bool, Fault>),
    Listed { path: String, entries: Result<Vec<Entry>, Fault> },
    Shared(Result<Vec<Entry>, Fault>),
    Read { entry: Entry, into: Option<u64>, bytes: Result<Vec<u8>, Fault> },
    Saved { uid: u64, entry: Result<Entry, Fault> },
}

/// Answers waiting to be read by a frame.
pub type Inbox = Arc<Mutex<Vec<Event>>>;

/// The host's cloud worker, as the interface sees it.
pub struct CloudService {
    /// Hands a job to the worker. This returns at once. The answer arrives in
    /// `inbox` on a later frame.
    pub submit: Box<dyn Fn(Job)>,
    pub inbox: Inbox,
}

/// What the service said about the signed-in user.
///
/// Capabilities are reported by the service and never assumed here. A provider
/// without incoming shares hides that side of the Open page. An empty list
/// where a feature should be reads to a user as a broken window.
#[derive(Clone, Debug, Default)]
pub struct Account {
    pub display: String,
    pub capabilities: Vec<Capability>,
    /// Until this is true, Open and Save have nowhere to go.
    pub provider_linked: bool,
}

impl Account {
    pub fn can(&self, capability: Capability) -> bool {
        self.capabilities.contains(&capability)
    }
}

/// A call that did not succeed, in the shapes the interface acts on
/// differently.
///
/// The client crate reports a refused conditional write as
/// `Error::Refused { code: "conflict", .. }`. The host maps that onto
/// [`Fault::Conflict`] here. That one distinction is the point of this type.
/// A conflict must never be retried and must never be forced.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Fault {
    /// No cloud account is linked yet. This is a state with an obvious next
    /// step. It is never drawn as an error.
    NotLinked,
    /// Somebody else saved while this user was editing. The save was refused
    /// and their work is intact.
    Conflict,
    /// The session expired. The user signs in again.
    Unauthenticated,
    Unreachable(String),
    Refused(String),
}

impl Fault {
    /// One sentence for a person to read.
    pub fn text(&self) -> String {
        match self {
            Fault::NotLinked => "No cloud account is linked yet.".to_owned(),
            Fault::Conflict => "Somebody else saved this workbook while you were editing it.".to_owned(),
            Fault::Unauthenticated => "Your cloud session has expired. Sign in again.".to_owned(),
            Fault::Unreachable(detail) => format!("The cloud service could not be reached. {detail}"),
            Fault::Refused(detail) => detail.clone(),
        }
    }

    /// A short machine-readable name, for the control channel.
    pub fn code(&self) -> &'static str {
        match self {
            Fault::NotLinked => "not_linked",
            Fault::Conflict => "conflict",
            Fault::Unauthenticated => "unauthenticated",
            Fault::Unreachable(_) => "unreachable",
            Fault::Refused(_) => "refused",
        }
    }
}

// ---------------------------------------------------------------- interface state

/// One folder, as it is being browsed.
#[derive(Clone, Debug, Default)]
pub struct Folder {
    pub path: String,
    pub entries: Vec<Entry>,
    pub loading: bool,
    pub error: Option<Fault>,
    /// Answered at least once. An empty folder then reads as empty. Without
    /// this it would read as still loading.
    pub loaded: bool,
}

impl Folder {
    fn begin(&mut self, path: &str) {
        self.path = path.to_owned();
        self.entries.clear();
        self.loading = true;
        self.loaded = false;
        self.error = None;
    }

    fn fill(&mut self, answer: Result<Vec<Entry>, Fault>) {
        self.loading = false;
        self.loaded = true;
        match answer {
            Ok(entries) => {
                self.entries = entries;
                self.error = None;
            }
            Err(fault) => {
                self.entries.clear();
                self.error = Some(fault);
            }
        }
    }
}

/// Where the document in this window came from, and the version it came at.
///
/// `entry.id` is the opaque handle a save passes back. `entry.etag` is the
/// version the conditional write is made against. `entry.room` is neither of
/// those. It is the collaboration identity, and it survives a rename.
#[derive(Clone, Debug)]
pub struct Origin {
    pub entry: Entry,
}

/// What the interface is waiting for. Drawn as progress while it waits.
#[derive(Clone, Debug)]
pub enum Busy {
    Opening { name: String },
    Saving { name: String, bytes: usize },
}

impl Busy {
    pub fn text(&self) -> String {
        match self {
            Busy::Opening { name } => format!("Opening {name}…"),
            Busy::Saving { name, bytes } => format!("Saving {name} ({})…", size_text(Some(*bytes as u64))),
        }
    }
}

/// A save that was refused because the stored copy had moved on.
#[derive(Clone, Debug)]
pub struct Conflict {
    /// The document that could not be saved.
    pub uid: u64,
    pub entry: Entry,
}

/// Linking a cloud account, which takes a browser round trip.
#[derive(Clone, Debug)]
pub enum Link {
    /// Offered and not started. Never drawn as an error.
    Offered,
    Starting,
    /// The browser is open. `handle` is polled until the user is done.
    Waiting { url: String, handle: Option<String>, next_poll: f64 },
    Failed(Fault),
}

/// Which side of the Open page a selection came from.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Source {
    #[default]
    Mine,
    Shared,
}

/// Everything the interface keeps about cloud storage.
#[derive(Default)]
pub struct Cloud {
    pub account: Option<Account>,
    pub account_fault: Option<Fault>,
    /// The account is asked for once per run. A frame never asks again.
    asked_account: bool,
    /// The folder being browsed. Shared by the Open and Save As pages, which
    /// are never on screen together.
    pub mine: Folder,
    pub shared: Folder,
    pub source: Source,
    pub busy: Option<Busy>,
    pub link: Option<Link>,
    pub conflict: Option<Conflict>,
    /// Save As: the file name being typed.
    pub save_name: String,
    /// Documents that came from the cloud, by document uid.
    pub origins: HashMap<u64, Origin>,
    /// The workbook snapshot each save in flight encoded. A save that
    /// succeeds clears the dirty flag for exactly the version that reached the
    /// server. An edit made while the bytes were on the wire stays unsaved.
    sent: HashMap<u64, Arc<Workbook>>,
    /// The last thing that happened, for the line under the page title.
    pub status: Option<String>,
    /// Whether the transfer in flight was started by the share dialog, which
    /// is waiting to be told how it went.
    pub sharing_save: bool,
}

// ---------------------------------------------------------------- asking

/// Whether the host wired a cloud service. When it did not, nothing
/// cloud-shaped is offered anywhere in the interface.
pub fn available(app: &SheetApp) -> bool {
    app.services.cloud.is_some()
}

/// Hands a job to the host's worker.
pub fn submit(app: &SheetApp, job: Job) {
    if let Some(service) = &app.services.cloud {
        (service.submit)(job);
    }
}

/// Asks who is signed in, once per run, and lists the root folder.
pub fn start(app: &mut SheetApp) {
    if !available(app) {
        return;
    }
    if !app.cloud.asked_account {
        app.cloud.asked_account = true;
        submit(app, Job::Account);
    }
    if !app.cloud.mine.loaded && !app.cloud.mine.loading {
        browse(app, "");
    }
}

/// Lists one folder. An empty path is the user's root.
pub fn browse(app: &mut SheetApp, path: &str) {
    if !available(app) {
        return;
    }
    app.cloud.mine.begin(path);
    submit(app, Job::List(path.to_owned()));
}

/// Lists what other people have shared with this user.
pub fn load_shared(app: &mut SheetApp) {
    if !available(app) {
        return;
    }
    app.cloud.shared.begin("");
    submit(app, Job::SharedWithMe);
}

/// Begins linking a cloud account.
pub fn link(app: &mut SheetApp) {
    if !available(app) {
        return;
    }
    app.cloud.link = Some(Link::Starting);
    submit(app, Job::LinkStart);
}

/// The parent of a path. `None` at the root.
pub fn parent_of(path: &str) -> Option<String> {
    let trimmed = path.trim_end_matches('/');
    if trimmed.is_empty() {
        return None;
    }
    match trimmed.rfind('/') {
        Some(0) | None => Some(String::new()),
        Some(cut) => Some(trimmed[..cut].to_owned()),
    }
}

/// Whether GridCraft can open this entry, judged by its extension and by what
/// the provider says it is.
///
/// Both signals are read because neither is reliable alone. A WebDAV server
/// hands back `application/octet-stream` for a perfectly good `.xlsx`. A file
/// exported by a web application often has a correct content type and no
/// extension at all.
pub fn opens(entry: &Entry) -> bool {
    if entry.kind == EntryKind::Folder {
        return false;
    }
    let extension = entry.name.rsplit_once('.').map(|(_, e)| e.to_ascii_lowercase());
    let by_name = extension.is_some_and(|e| OPENS.contains(&e.as_str()));
    by_name || entry.mime.as_deref().is_some_and(mime_opens)
}

fn mime_opens(mime: &str) -> bool {
    let mime = mime.split(';').next().unwrap_or("").trim().to_ascii_lowercase();
    mime.starts_with("application/vnd.openxmlformats-officedocument.spreadsheetml")
        || mime.starts_with("application/vnd.ms-excel")
        || mime == "application/vnd.oasis.opendocument.spreadsheet"
        || mime == "text/csv"
        || mime == "text/tab-separated-values"
        || mime == "text/plain"
        || mime == "application/json"
}

// ---------------------------------------------------------------- opening

/// Reads a document and opens it. `into` names an open document the bytes
/// replace, which is how a reload after a conflict works.
pub fn open_entry(app: &mut SheetApp, entry: Entry, into: Option<u64>) {
    if !available(app) || app.cloud.busy.is_some() {
        return;
    }
    app.cloud.status = None;
    app.cloud.busy = Some(Busy::Opening { name: entry.name.clone() });
    submit(app, Job::Read { entry, into });
}

/// Reads the active document's stored copy again. Local edits are discarded.
pub fn reload_active(app: &mut SheetApp) -> Result<Json, String> {
    let Some(uid) = app.session.active().map(|d| d.uid) else { return Err("no workbook open".to_owned()) };
    let Some(origin) = app.cloud.origins.get(&uid).cloned() else { return Err("this workbook did not come from the cloud".to_owned()) };
    app.cloud.conflict = None;
    let name = origin.entry.name.clone();
    open_entry(app, origin.entry, Some(uid));
    Ok(json!({"reloading": name}))
}

// ---------------------------------------------------------------- saving

/// The cloud document the active workbook came from, when it came from one.
pub fn origin(app: &SheetApp) -> Option<&Origin> {
    let uid = app.session.active()?.uid;
    app.cloud.origins.get(&uid)
}

/// Whether Save has somewhere to go without asking. The Backstage view uses
/// this to choose between Save and the Save As page.
pub fn has_target(app: &SheetApp) -> bool {
    origin(app).is_some_and(|o| o.entry.writable)
}

/// Encodes the active workbook as xlsx. The snapshot that produced the bytes
/// comes back with them, so a successful save clears the dirty flag for
/// exactly that version.
fn encode(app: &SheetApp) -> Result<(Arc<Workbook>, Vec<u8>), String> {
    let doc = app.session.active().ok_or("no workbook open")?;
    let bytes = gridcraft_engine::io::save_bytes(&doc.wb, "workbook.xlsx").map_err(|e| e.to_string())?;
    Ok((doc.wb.clone(), bytes))
}

/// Save: writes back over the entry this workbook came from.
///
/// The etag the document was read at goes out as the condition. A server whose
/// copy has moved on refuses the write. That arrives as [`Fault::Conflict`]
/// and is put to the user. It is never retried.
pub fn save(app: &mut SheetApp) -> Result<Json, String> {
    let uid = app.session.active().map(|d| d.uid).ok_or("no workbook open")?;
    let origin = app.cloud.origins.get(&uid).cloned().ok_or("this workbook did not come from the cloud")?;
    if !origin.entry.writable {
        return Err("this document was shared with you for reading only. Save As keeps a copy of your own.".to_owned());
    }
    if app.cloud.busy.is_some() {
        return Err("a cloud transfer is already running".to_owned());
    }
    let (snapshot, bytes) = encode(app)?;
    let size = bytes.len();
    app.cloud.sent.insert(uid, snapshot);
    app.cloud.status = None;
    app.cloud.busy = Some(Busy::Saving { name: origin.entry.name.clone(), bytes: size });
    let answer = json!({"saving": origin.entry.name, "bytes": size, "ifMatch": origin.entry.etag});
    submit(app, Job::Write { uid, entry: origin.entry, bytes });
    Ok(answer)
}

/// Save As: writes a new document into `folder` under `name`.
pub fn save_as(app: &mut SheetApp, folder: &str, name: &str) -> Result<Json, String> {
    let uid = app.session.active().map(|d| d.uid).ok_or("no workbook open")?;
    let name = workbook_name(name);
    if name.is_empty() {
        return Err("type a file name".to_owned());
    }
    if app.cloud.busy.is_some() {
        return Err("a cloud transfer is already running".to_owned());
    }
    let (snapshot, bytes) = encode(app)?;
    let size = bytes.len();
    app.cloud.sent.insert(uid, snapshot);
    app.cloud.status = None;
    app.cloud.busy = Some(Busy::Saving { name: name.clone(), bytes: size });
    let answer = json!({"saving": name, "folder": folder, "bytes": size});
    submit(app, Job::Create { uid, folder: folder.to_owned(), name, bytes });
    Ok(answer)
}

/// A typed name as a workbook file name. Any folder separators are dropped and
/// a missing extension becomes `.xlsx`.
pub fn workbook_name(typed: &str) -> String {
    let base = typed.rsplit(['/', '\\']).next().unwrap_or("").trim();
    if base.is_empty() {
        return String::new();
    }
    let known = base.rsplit_once('.').is_some_and(|(stem, ext)| !stem.is_empty() && OPENS.contains(&ext.to_ascii_lowercase().as_str()));
    if known { base.to_owned() } else { format!("{base}.xlsx") }
}

// ---------------------------------------------------------------- reading the answers

/// Drains the host's inbox and keeps a link or a transfer moving. Called once
/// a frame, before anything is drawn.
pub fn pump(app: &mut SheetApp, ctx: &egui::Context) {
    if !available(app) {
        return;
    }
    let arrived: Vec<Event> = app
        .services
        .cloud
        .as_ref()
        .map(|c| std::mem::take(&mut *c.inbox.lock().unwrap_or_else(PoisonError::into_inner)))
        .unwrap_or_default();
    for event in arrived {
        apply(app, event);
    }
    poll_link(app);
    // A worker answering is not a local event. Without this the progress line
    // would sit frozen until the user moved the mouse.
    if app.cloud.busy.is_some() || matches!(app.cloud.link, Some(Link::Starting | Link::Waiting { .. })) {
        ctx.request_repaint_after(Duration::from_millis(120));
    }
}

fn apply(app: &mut SheetApp, event: Event) {
    match event {
        Event::Account(Ok(account)) => {
            let linked = account.provider_linked;
            app.cloud.account = Some(account);
            app.cloud.account_fault = None;
            if linked {
                app.cloud.link = None;
            } else if app.cloud.link.is_none() {
                app.cloud.link = Some(Link::Offered);
            }
        }
        // No account linked yet is a state with an obvious next step. It is an
        // offer to link, and never an error message.
        Event::Account(Err(Fault::NotLinked)) => {
            app.cloud.account_fault = None;
            if app.cloud.link.is_none() {
                app.cloud.link = Some(Link::Offered);
            }
        }
        Event::Account(Err(fault)) => app.cloud.account_fault = Some(fault),
        Event::Browser { url, handle } => {
            if let Some(open) = &app.services.open_url {
                open(&url);
            }
            app.cloud.link = Some(Link::Waiting { url, handle, next_poll: now_ms() + LINK_POLL_MS });
        }
        Event::Linked(Ok(true)) => {
            app.cloud.link = None;
            app.cloud.status = Some("Cloud account linked.".to_owned());
            app.cloud.asked_account = false;
            submit(app, Job::Account);
            browse(app, "");
            app.cloud.shared.loaded = false;
        }
        // The ordinary answer while the user is still in the browser.
        Event::Linked(Ok(false)) | Event::Linked(Err(Fault::NotLinked)) => {}
        Event::Linked(Err(fault)) => app.cloud.link = Some(Link::Failed(fault)),
        Event::Listed { path, entries } => {
            if path == app.cloud.mine.path {
                app.cloud.mine.fill(entries);
            }
        }
        Event::Shared(entries) => app.cloud.shared.fill(entries),
        Event::Read { entry, into, bytes } => {
            app.cloud.busy = None;
            match bytes {
                Ok(bytes) => opened(app, entry, into, bytes),
                Err(Fault::NotLinked) => app.cloud.link = Some(Link::Offered),
                Err(fault) => app.message = Some(("GridCraft".to_owned(), format!("{} could not be opened. {}", entry.name, fault.text()))),
            }
        }
        Event::Saved { uid, entry } => {
            app.cloud.busy = None;
            let snapshot = app.cloud.sent.remove(&uid);
            match entry {
                Ok(entry) => {
                    if let Some(snapshot) = snapshot {
                        clean(app, uid, snapshot);
                    }
                    stored(app, uid, entry);
                }
                // The one failure that must never be retried and never forced.
                Err(Fault::Conflict) => match app.cloud.origins.get(&uid).cloned() {
                    Some(origin) => app.cloud.conflict = Some(Conflict { uid, entry: origin.entry }),
                    None => app.message = Some(("GridCraft".to_owned(), Fault::Conflict.text())),
                },
                Err(Fault::NotLinked) => app.cloud.link = Some(Link::Offered),
                Err(fault) => app.message = Some(("GridCraft".to_owned(), format!("The workbook was not saved. {}", fault.text()))),
            }
        }
    }
}

/// Bytes arrived. They open through the ordinary open path, and the document
/// remembers where they came from.
fn opened(app: &mut SheetApp, entry: Entry, into: Option<u64>, bytes: Vec<u8>) {
    let encoded = gridcraft_engine::io::base64_encode(&bytes);
    match app.run("file.open", json!({"name": entry.name, "base64": encoded})) {
        Ok(result) => {
            app.backstage = None;
            app.show_open_warnings(&result);
            let now = app.session.active().map(|d| d.uid);
            if let Some(uid) = now {
                app.cloud.origins.insert(uid, Origin { entry: entry.clone() });
            }
            // A reload closes the document it replaced only once the new one
            // is open. A read that fails leaves the user's work where it was.
            let stale = into.filter(|old| Some(*old) != now);
            if let Some(old) = stale
                && let Some(index) = app.session.documents().iter().position(|d| d.uid == old)
            {
                app.cloud.origins.remove(&old);
                app.cloud.sent.remove(&old);
                app.session.close_document(index);
            }
            app.cloud.status = Some(format!("Opened {} from the cloud.", entry.name));
        }
        Err(error) if error.contains(gridcraft_engine::io::PASSWORD_REQUIRED) => {
            app.backstage = None;
            app.open_dialog("openPassword", json!({"name": entry.name, "base64": gridcraft_engine::io::base64_encode(&bytes)}));
        }
        Err(error) => app.message = Some(("GridCraft".to_owned(), crate::clean_error(&error))),
    }
}

/// A save landed. The document now belongs to the entry the server returned,
/// at the version it returned.
fn stored(app: &mut SheetApp, uid: u64, entry: Entry) {
    app.cloud.conflict = None;
    app.cloud.status = Some(format!("Saved {} to the cloud.", entry.name));
    app.toast = Some((format!("Saved {} to the cloud", entry.name), now_ms()));
    app.cloud.origins.insert(uid, Origin { entry });
}

/// Marks the version that reached the server as the saved one.
///
/// An edit made while the bytes were in flight keeps the workbook dirty, which
/// is what the title bar and the close prompt go by.
fn clean(app: &mut SheetApp, uid: u64, snapshot: Arc<Workbook>) {
    if let Some(doc) = app.session.document_mut(uid) {
        doc.saved = snapshot;
    }
}

/// Keeps asking whether the user has finished linking in their browser.
fn poll_link(app: &mut SheetApp) {
    let now = now_ms();
    let Some(Link::Waiting { handle: Some(handle), next_poll, .. }) = &mut app.cloud.link else { return };
    if now < *next_poll {
        return;
    }
    *next_poll = now + LINK_POLL_MS;
    let handle = handle.clone();
    submit(app, Job::LinkPoll(handle));
}

/// The whole of the cloud state, for the control channel and for tests.
pub fn status(app: &SheetApp) -> Json {
    let account = app.cloud.account.as_ref().map(|a| {
        json!({
            "display": a.display,
            "providerLinked": a.provider_linked,
            "capabilities": a.capabilities.iter().map(|c| serde_json::to_value(c).unwrap_or_default()).collect::<Vec<_>>(),
        })
    });
    let came_from = origin(app).map(|o| {
        json!({"id": o.entry.id, "room": o.entry.room, "name": o.entry.name, "etag": o.entry.etag, "writable": o.entry.writable})
    });
    json!({
        "available": available(app),
        "account": account,
        "accountError": app.cloud.account_fault.as_ref().map(Fault::code),
        "folder": app.cloud.mine.path,
        "entries": app.cloud.mine.entries.iter().map(|e| e.name.clone()).collect::<Vec<_>>(),
        "openable": app.cloud.mine.entries.iter().filter(|e| opens(e)).map(|e| e.name.clone()).collect::<Vec<_>>(),
        "shared": app.cloud.shared.entries.iter().map(|e| json!({"name": e.name, "sharedBy": e.shared_by, "writable": e.writable, "openable": opens(e)})).collect::<Vec<_>>(),
        "busy": app.cloud.busy.as_ref().map(Busy::text),
        "link": match &app.cloud.link {
            None => Json::Null,
            Some(Link::Offered) => json!("offered"),
            Some(Link::Starting) => json!("starting"),
            Some(Link::Waiting { url, .. }) => json!({"waiting": url}),
            Some(Link::Failed(fault)) => json!({"failed": fault.code()}),
        },
        "conflict": app.cloud.conflict.as_ref().map(|c| json!({"name": c.entry.name})),
        "origin": came_from,
        "status": app.cloud.status,
    })
}

// ---------------------------------------------------------------- drawing

fn dim(ui: &mut Ui, text: &str) {
    let t = Tokens::get(ui.ctx());
    ui.label(RichText::new(text).font(theme::ui_font(12.5)).color(t.text_dim));
}

/// Bytes as a person reads them.
fn size_text(bytes: Option<u64>) -> String {
    let Some(bytes) = bytes else { return String::new() };
    const UNITS: [&str; 4] = ["bytes", "KB", "MB", "GB"];
    let mut size = bytes as f64;
    let mut unit = 0;
    while size >= 1024.0 && unit + 1 < UNITS.len() {
        size /= 1024.0;
        unit += 1;
    }
    match UNITS.get(unit) {
        Some(name) if unit > 0 => format!("{size:.1} {name}"),
        _ => format!("{bytes} bytes"),
    }
}

/// One row in a file list, with a second line for the detail that matters on
/// that side of the page (a size for your own files, who shared it for
/// somebody else's).
fn entry_row(ui: &mut Ui, entry: &Entry, detail: &str) -> egui::Response {
    let t = Tokens::get(ui.ctx());
    let width = ui.available_width();
    let (rect, resp) = ui.allocate_exact_size(vec2(width, 40.0), Sense::click());
    let label = entry.name.clone();
    resp.widget_info(|| egui::WidgetInfo::labeled(egui::WidgetType::Button, true, &label));
    let p = ui.painter();
    if resp.hovered() {
        p.rect_filled(rect, 4.0, t.hover);
    }
    let icon = if entry.kind == EntryKind::Folder { Icon::Folder } else { Icon::Book };
    icons::paint(p, Rect::from_center_size(pos2(rect.left() + 18.0, rect.center().y), vec2(18.0, 18.0)), icon, t.text);
    let (name_y, detail_y) = if detail.is_empty() { (rect.center().y, rect.center().y) } else { (rect.center().y - 8.0, rect.center().y + 9.0) };
    p.text(pos2(rect.left() + 38.0, name_y), Align2::LEFT_CENTER, &entry.name, theme::ui_font(13.0), t.text);
    if !detail.is_empty() {
        p.text(pos2(rect.left() + 38.0, detail_y), Align2::LEFT_CENTER, detail, theme::ui_font(11.0), t.text_dim);
    }
    resp
}

/// A bordered column with a heading. This is how the two Open sources sit side
/// by side.
fn column(ui: &mut Ui, heading: &str, body: impl FnOnce(&mut Ui)) {
    let t = Tokens::get(ui.ctx());
    egui::Frame::NONE
        .fill(t.ribbon)
        .stroke(Stroke::new(1.0, t.ribbon_border))
        .corner_radius(6.0)
        .inner_margin(10)
        .show(ui, |ui| {
            ui.label(RichText::new(heading).font(theme::ui_bold(13.0)).color(t.text));
            ui.add_space(6.0);
            egui::ScrollArea::vertical().id_salt(heading.to_owned()).max_height(300.0).auto_shrink([false, false]).show(ui, body);
        });
}

/// The account line, or the reason there is nothing to show yet.
fn header(app: &mut SheetApp, ui: &mut Ui) {
    let lang = app.ui.language;
    let mut retry = false;
    match (&app.cloud.account, &app.cloud.account_fault) {
        (_, Some(fault)) => {
            let text = fault.text();
            ui.horizontal(|ui| {
                dim(ui, &text);
                if ui.button(lang.tr("Try again")).clicked() {
                    retry = true;
                }
            });
        }
        (Some(account), None) => {
            let line = if account.display.is_empty() { lang.tr("Signed in").to_owned() } else { account.display.clone() };
            dim(ui, &line);
        }
        (None, None) => dim(ui, lang.tr("Connecting to your cloud account…")),
    }
    if retry {
        app.cloud.account_fault = None;
        app.cloud.asked_account = false;
        start(app);
    }
}

/// The offer to link an account, and the wait while the browser is open.
/// Returns true when it drew, because there is nothing else to show until an
/// account exists.
///
/// None of this is an error. The whole state means one thing: there is no
/// account yet, and linking one is a button away.
fn link_panel(app: &mut SheetApp, ui: &mut Ui) -> bool {
    let lang = app.ui.language;
    let Some(state) = app.cloud.link.clone() else { return false };
    let t = Tokens::get(ui.ctx());
    let mut begin = false;
    let mut cancel = false;
    let mut browse_to: Option<String> = None;
    egui::Frame::NONE.fill(t.accent_soft).corner_radius(6.0).inner_margin(12).show(ui, |ui| match state {
        Link::Offered => {
            ui.label(RichText::new(lang.tr("No cloud account is linked yet.")).font(theme::ui_bold(13.0)).color(t.text));
            dim(ui, lang.tr("Link one to open and save workbooks in your cloud storage."));
            ui.add_space(8.0);
            begin = ui.button(lang.tr("Link a cloud account")).clicked();
        }
        Link::Starting => {
            ui.horizontal(|ui| {
                ui.add(egui::Spinner::new().size(14.0));
                dim(ui, lang.tr("Starting…"));
            });
        }
        Link::Waiting { url, .. } => {
            ui.label(RichText::new(lang.tr("Finish linking in your browser.")).font(theme::ui_bold(13.0)).color(t.text));
            ui.horizontal(|ui| {
                ui.add(egui::Spinner::new().size(14.0));
                dim(ui, lang.tr("Waiting for your browser…"));
            });
            ui.add_space(6.0);
            if ui.link(&url).clicked() {
                browse_to = Some(url.clone());
            }
            ui.add_space(8.0);
            cancel = ui.button(lang.tr("Cancel")).clicked();
        }
        Link::Failed(fault) => {
            ui.label(RichText::new(fault.text()).font(theme::ui_font(13.0)).color(t.text));
            ui.add_space(8.0);
            begin = ui.button(lang.tr("Try again")).clicked();
        }
    });
    if let Some(url) = browse_to
        && let Some(open) = &app.services.open_url
    {
        open(&url);
    }
    if begin {
        link(app);
    } else if cancel {
        app.cloud.link = Some(Link::Offered);
    }
    true
}

/// The progress line for a read or a write in flight.
pub fn progress(app: &SheetApp, ui: &mut Ui) {
    let Some(busy) = app.cloud.busy.clone() else { return };
    let t = Tokens::get(ui.ctx());
    ui.horizontal(|ui| {
        ui.add(egui::Spinner::new().size(15.0));
        ui.label(RichText::new(busy.text()).font(theme::ui_font(12.5)).color(t.text));
    });
}

/// File › Open, the cloud part. Your own files sit on one side and what other
/// people shared with you sits on the other.
pub fn open_page(app: &mut SheetApp, ui: &mut Ui) {
    start(app);
    header(app, ui);
    ui.add_space(8.0);
    if link_panel(app, ui) {
        return;
    }
    progress(app, ui);
    // Hidden where the provider has no incoming shares at all. An empty list
    // under that heading would read as a broken feature.
    let shared_offered = app.cloud.account.as_ref().is_some_and(|a| a.can(Capability::SharedWithMe));
    if shared_offered && !app.cloud.shared.loaded && !app.cloud.shared.loading {
        load_shared(app);
    }
    let mut pick: Option<(Entry, Source)> = None;
    let mut go: Option<String> = None;
    if shared_offered {
        ui.columns(2, |columns| {
            if let Some(ui) = columns.get_mut(0) {
                my_files(app, ui, &mut pick, &mut go);
            }
            if let Some(ui) = columns.get_mut(1) {
                shared_files(app, ui, &mut pick);
            }
        });
    } else {
        my_files(app, ui, &mut pick, &mut go);
    }
    if let Some(path) = go {
        browse(app, &path);
    } else if let Some((entry, source)) = pick {
        app.cloud.source = source;
        open_entry(app, entry, None);
    }
    if let Some(status) = app.cloud.status.clone() {
        ui.add_space(10.0);
        dim(ui, &status);
    }
}

/// Your own files: where you are, a way up, then the folders and the documents
/// GridCraft can open.
fn my_files(app: &mut SheetApp, ui: &mut Ui, pick: &mut Option<(Entry, Source)>, go: &mut Option<String>) {
    let lang = app.ui.language;
    let folder = app.cloud.mine.clone();
    column(ui, lang.tr("My files"), |ui| {
        let here = if folder.path.is_empty() { lang.tr("Home").to_owned() } else { folder.path.clone() };
        ui.horizontal(|ui| {
            if let Some(up) = parent_of(&folder.path)
                && ui.button(lang.tr("Up")).clicked()
            {
                *go = Some(up);
            }
            dim(ui, &here);
        });
        ui.add_space(4.0);
        if folder.loading {
            ui.horizontal(|ui| {
                ui.add(egui::Spinner::new().size(14.0));
                dim(ui, lang.tr("Loading…"));
            });
            return;
        }
        if let Some(fault) = &folder.error {
            dim(ui, &fault.text());
            return;
        }
        let mut shown = 0;
        for entry in &folder.entries {
            let is_folder = entry.kind == EntryKind::Folder;
            if !is_folder && !opens(entry) {
                continue;
            }
            shown += 1;
            let detail = if is_folder { String::new() } else { size_text(entry.size) };
            if entry_row(ui, entry, &detail).clicked() {
                if is_folder {
                    *go = Some(entry.path.clone());
                } else {
                    *pick = Some((entry.clone(), Source::Mine));
                }
            }
        }
        if shown == 0 && folder.loaded {
            dim(ui, lang.tr("Nothing here that GridCraft can open."));
        }
    });
}

/// What other people shared with you. One flat list, because an incoming share
/// has no folder of yours to sit in. Each row says who shared it.
fn shared_files(app: &mut SheetApp, ui: &mut Ui, pick: &mut Option<(Entry, Source)>) {
    let lang = app.ui.language;
    let shared = app.cloud.shared.clone();
    column(ui, lang.tr("Shared with me"), |ui| {
        if shared.loading {
            ui.horizontal(|ui| {
                ui.add(egui::Spinner::new().size(14.0));
                dim(ui, lang.tr("Loading…"));
            });
            return;
        }
        if let Some(fault) = &shared.error {
            dim(ui, &fault.text());
            return;
        }
        let mut shown = 0;
        for entry in shared.entries.iter().filter(|e| opens(e)) {
            shown += 1;
            let who = entry.shared_by.clone().unwrap_or_else(|| lang.tr("Shared with you").to_owned());
            let detail = if entry.writable { who } else { format!("{who} ({})", lang.tr("read-only")) };
            if entry_row(ui, entry, &detail).clicked() {
                *pick = Some((entry.clone(), Source::Shared));
            }
        }
        if shown == 0 && shared.loaded {
            dim(ui, lang.tr("Nobody has shared a workbook with you."));
        }
    });
}

/// File › Save As, the cloud destination. Pick a folder, name the file, save.
/// Returns true when a save started, so the Backstage view can return to the
/// sheet.
pub fn save_page(app: &mut SheetApp, ui: &mut Ui) -> bool {
    let lang = app.ui.language;
    start(app);
    // The destination above already names the account. This line is here to
    // say when something is wrong with it.
    if app.cloud.account.is_none() || app.cloud.account_fault.is_some() {
        header(app, ui);
        ui.add_space(8.0);
    }
    if link_panel(app, ui) {
        return false;
    }
    progress(app, ui);
    if app.cloud.save_name.is_empty() {
        let suggested = app.session.active().map(|d| d.display_title()).unwrap_or_else(|| "Book1".to_owned());
        app.cloud.save_name = workbook_name(&suggested);
    }
    let folder = app.cloud.mine.clone();
    let mut go: Option<String> = None;
    column(ui, lang.tr("Folder"), |ui| {
        let here = if folder.path.is_empty() { lang.tr("Home").to_owned() } else { folder.path.clone() };
        ui.horizontal(|ui| {
            if let Some(up) = parent_of(&folder.path)
                && ui.button(lang.tr("Up")).clicked()
            {
                go = Some(up);
            }
            dim(ui, &here);
        });
        ui.add_space(4.0);
        if folder.loading {
            ui.horizontal(|ui| {
                ui.add(egui::Spinner::new().size(14.0));
                dim(ui, lang.tr("Loading…"));
            });
            return;
        }
        if let Some(fault) = &folder.error {
            dim(ui, &fault.text());
            return;
        }
        let mut folders = 0;
        for entry in folder.entries.iter().filter(|e| e.kind == EntryKind::Folder) {
            folders += 1;
            if entry_row(ui, entry, "").clicked() {
                go = Some(entry.path.clone());
            }
        }
        if folders == 0 && folder.loaded {
            dim(ui, lang.tr("No folders here. The workbook saves in this one."));
        }
    });
    if let Some(path) = go {
        browse(app, &path);
    }
    ui.add_space(12.0);
    let mut asked = false;
    ui.horizontal(|ui| {
        ui.label(lang.tr("File name"));
        let typing = ui.add(egui::TextEdit::singleline(&mut app.cloud.save_name).desired_width(280.0));
        asked = typing.lost_focus() && ui.input(|i| i.key_pressed(egui::Key::Enter));
    });
    ui.add_space(10.0);
    let ready = app.cloud.busy.is_none() && !workbook_name(&app.cloud.save_name).is_empty();
    if ui.add_enabled(ready, egui::Button::new(lang.tr("Save to the cloud"))).clicked() {
        asked = true;
    }
    ui.add_space(8.0);
    dim(ui, lang.tr("The workbook is saved as an Excel workbook (.xlsx) in the folder above."));
    // A provider with no conditional writes cannot refuse one, so a save here
    // can land on top of somebody else's. Said plainly, because the user is
    // the only one who can decide whether that matters.
    if app.cloud.account.as_ref().is_some_and(|a| !a.can(Capability::Etags)) {
        ui.add_space(4.0);
        dim(ui, lang.tr("This cloud account cannot detect a save that would overwrite somebody else's changes."));
    }
    if asked && ready && app.commit_edit(0, 0, false, false) {
        let (folder, name) = (app.cloud.mine.path.clone(), app.cloud.save_name.clone());
        match save_as(app, &folder, &name) {
            Ok(_) => return true,
            Err(error) => app.message = Some(("GridCraft".to_owned(), error)),
        }
    }
    false
}

/// A save refused because the stored copy had moved on.
///
/// Three ways out, and none of them throws anything away without saying so.
/// Reloading is the one a user usually wants, so it comes first. The text says
/// plainly that it discards their edits.
pub fn conflict_modal(app: &mut SheetApp, ctx: &egui::Context) {
    let Some(conflict) = app.cloud.conflict.clone() else { return };
    let lang = app.ui.language;
    let mut reload = false;
    let mut copy = false;
    let mut dismiss = false;
    egui::Modal::new(egui::Id::new("cloud_conflict")).show(ctx, |ui| {
        ui.set_width(430.0);
        ui.label(RichText::new(lang.tr("This workbook was not saved")).font(theme::ui_bold(15.0)));
        ui.add_space(8.0);
        let what = format!("Somebody else saved {} while you were editing it. Nothing was written: saving now would overwrite their work.", conflict.entry.name);
        ui.add(egui::Label::new(what).wrap());
        ui.add_space(6.0);
        ui.add(egui::Label::new(lang.tr("Your own changes are still open here. Reloading replaces them with the stored copy.")).wrap());
        ui.add_space(12.0);
        ui.horizontal(|ui| {
            reload = ui.button(lang.tr("Reload from the cloud")).clicked();
            copy = ui.button(lang.tr("Save as a copy…")).clicked();
            dismiss = ui.button(lang.tr("Keep editing")).clicked();
        });
    });
    if reload {
        app.cloud.conflict = None;
        if let Err(error) = reload_active(app) {
            app.message = Some(("GridCraft".to_owned(), error));
        }
    } else if copy {
        app.cloud.conflict = None;
        app.cloud.save_name = workbook_name(&format!("{} (copy)", stem_of(&conflict.entry.name)));
        app.open_backstage(crate::backstage::Page::SaveAs);
        if let Some(backstage) = &mut app.backstage {
            backstage.save_to_cloud = Some(true);
        }
    } else if dismiss {
        app.cloud.conflict = None;
    }
}

/// A file name without its extension.
fn stem_of(name: &str) -> String {
    match name.rsplit_once('.') {
        Some((stem, _)) if !stem.is_empty() => stem.to_owned(),
        _ => name.to_owned(),
    }
}
