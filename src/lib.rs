use std::{collections::HashMap, ops::Deref, process::ExitStatus};

use tokio::{process::Command, select, sync::*};

/// A command runner launches external processes in the background and
/// keeps track of running processes.
/// A running process can the be awaited from any thread using a [handle](CommandHandle).
/// Once a process has finished, all awaiting thread will see the same [outcome](CommandOutcome).
///
/// Communication with the command runner is done by passing messages. [Handles](CommandRunnerHandle)
/// are used to send commands to the command runner from any thread. A single thread receives these messages
/// and launches or kills.
///
/// # Example
/// 
/// ```rust,no_run
/// use command_runner_poc::*;
/// 
/// 
/// #[tokio::main]
/// async fn main() {
///     let runner = CommandRunner::new();
///     let handle = runner.handle();
///
///     tokio::spawn(runner.run());
/// }
/// ```
pub struct CommandRunner {
    tx: mpsc::UnboundedSender<Msg>,
    rx: mpsc::UnboundedReceiver<Msg>,
    running_commands: HashMap<CommandId, RunningCommand>,
}

impl CommandRunner {
    pub fn new() -> CommandRunner {
        let (tx, rx) = mpsc::unbounded_channel();
        CommandRunner {
            tx,
            rx,
            running_commands: HashMap::new(),
        }
    }

    pub async fn run(mut self) {
        while let Some(msg) = self.rx.recv().await {
            match msg {
                Msg::Launch { command, reply } => self.launch(command, reply).await,
                Msg::List { reply } => self.list(reply),
                Msg::Finished(id, outcome) => self.finished(id, outcome),
                Msg::Kill { id, reply } => self.kill(id, reply),
            }
        }
    }

    pub fn handle(&self) -> CommandRunnerHandle {
        CommandRunnerHandle(self.tx.clone())
    }

    async fn launch(&mut self, mut command: Command, reply: oneshot::Sender<CommandHandle>) {
        let (outcome_tx, outcome_rx) = watch::channel(None);
        let (kill_tx, kill_rx) = oneshot::channel();
        let handle = self.handle();
        let id = CommandId::new();

        let running_command = RunningCommand {
            kill_switch: kill_tx,
            outcome_tx,
        };
        let running_command_handle = CommandHandle {
            id: id.clone(),
            outcome: outcome_rx,
        };

        self.running_commands.insert(id.clone(), running_command);

        tokio::spawn(async move {
            let mut child = command.spawn().unwrap();
            let outcome = select! {
                exit_status = child.wait() => CommandOutcome::from(exit_status.unwrap()),
                _ = kill_rx => CommandOutcome::Cancelled
            };
            handle.finished(id, outcome);
        });

        reply.send(running_command_handle).unwrap();
    }

    fn list(&self, reply: oneshot::Sender<Vec<CommandHandle>>) {
        let res = self
            .running_commands
            .iter()
            .map(CommandHandle::from)
            .collect();
        reply.send(res).unwrap()
    }

    fn finished(&mut self, id: CommandId, outcome: CommandOutcome) {
        if let Some(cmd) = self.running_commands.remove(&id) {
            cmd.outcome_tx.send(Some(outcome)).unwrap();
        }
    }

    fn kill(&mut self, id: CommandId, reply: oneshot::Sender<()>) {
        if let Some(cmd) = self.running_commands.remove(&id) {
            cmd.kill_switch.send(()).unwrap();
        }
        reply.send(()).unwrap();
    }
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Clone)]
pub struct CommandId(String);

impl CommandId {
    pub fn new() -> Self {
        Self(nanoid::nanoid!())
    }
}

struct RunningCommand {
    kill_switch: oneshot::Sender<()>,
    outcome_tx: watch::Sender<Option<CommandOutcome>>,
}

#[derive(PartialEq, Eq, Debug, Clone)]
pub enum CommandOutcome {
    Success,
    Failure,
    Cancelled,
}

impl From<ExitStatus> for CommandOutcome {
    fn from(status: ExitStatus) -> Self {
        if status.success() {
            CommandOutcome::Success
        } else {
            CommandOutcome::Failure
        }
    }
}

#[derive(Debug)]
enum Msg {
    Launch {
        command: Command,
        reply: oneshot::Sender<CommandHandle>,
    },
    List {
        reply: oneshot::Sender<Vec<CommandHandle>>,
    },
    Kill {
        id: CommandId,
        reply: oneshot::Sender<()>,
    },
    Finished(CommandId, CommandOutcome),
}

#[derive(Clone)]
pub struct CommandRunnerHandle(mpsc::UnboundedSender<Msg>);

impl CommandRunnerHandle {
    fn finished(&self, id: CommandId, outcome: CommandOutcome) {
        self.0.send(Msg::Finished(id, outcome)).unwrap();
    }

    pub async fn list(&self) -> Vec<CommandHandle> {
        let (reply, rx) = oneshot::channel();
        self.0.send(Msg::List { reply }).unwrap();
        rx.await.unwrap()
    }

    pub async fn launch(&self, command: Command) -> CommandHandle {
        let (reply, rx) = oneshot::channel();
        self.0.send(Msg::Launch { command, reply }).unwrap();
        rx.await.unwrap()
    }

    pub async fn kill(&self, command_id: CommandId) {
        let (reply, rx) = oneshot::channel();
        self.0
            .send(Msg::Kill {
                id: command_id,
                reply,
            })
            .unwrap();
        rx.await.unwrap()
    }
}

#[derive(Clone, Debug)]
pub struct CommandHandle {
    pub id: CommandId,
    outcome: watch::Receiver<Option<CommandOutcome>>,
}

impl From<(&CommandId, &RunningCommand)> for CommandHandle {
    fn from(cmd: (&CommandId, &RunningCommand)) -> Self {
        Self {
            id: cmd.0.clone(),
            outcome: cmd.1.outcome_tx.subscribe(),
        }
    }
}

impl CommandHandle {
    pub async fn kill(&self, runner: &CommandRunnerHandle) {
        runner.kill(self.id.clone()).await;
    }

    pub async fn wait(&mut self) -> CommandOutcome {
        let mut out = self.outcome.borrow().deref().clone();
        while let None = out {
            self.outcome.changed().await.unwrap();
            out = self.outcome.borrow().deref().clone();
        }
        out.unwrap()
    }
}
