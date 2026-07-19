use serde::{Deserialize, Serialize};

pub mod agents;
pub mod common;
pub mod events;
pub mod integrations;
pub mod panes;
pub mod peers;
pub mod plugins;
pub mod response;
pub mod server;
pub mod session;
pub mod tabs;
pub mod workspaces;
pub mod worktrees;

pub use agents::*;
pub use common::*;
pub use events::*;
pub use integrations::*;
pub use panes::*;
pub use peers::*;
pub use plugins::*;
pub use response::*;
pub use server::*;
pub use session::*;
pub use tabs::*;
pub use workspaces::*;
pub use worktrees::*;

fn is_false(value: &bool) -> bool {
    !*value
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
pub struct Request {
    pub id: String,
    #[serde(flatten)]
    pub method: Method,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, schemars::JsonSchema)]
#[serde(tag = "method", content = "params")]
// Request enums are short-lived wire values; keeping variants direct preserves
// the simple serde shape and avoids boxing churn across every caller.
#[allow(clippy::large_enum_variant)]
pub enum Method {
    #[serde(rename = "ping")]
    Ping(PingParams),
    #[serde(rename = "server.stop")]
    ServerStop(EmptyParams),
    #[serde(rename = "server.live_handoff")]
    ServerLiveHandoff(ServerLiveHandoffParams),
    #[serde(rename = "server.reload_config")]
    ServerReloadConfig(EmptyParams),
    #[serde(rename = "server.agent_manifests")]
    ServerAgentManifests(EmptyParams),
    #[serde(rename = "server.reload_agent_manifests")]
    ServerReloadAgentManifests(EmptyParams),
    #[serde(rename = "notification.show")]
    NotificationShow(NotificationShowParams),
    #[serde(rename = "client.window_title.set")]
    ClientWindowTitleSet(ClientWindowTitleSetParams),
    #[serde(rename = "client.window_title.clear")]
    ClientWindowTitleClear(EmptyParams),
    #[serde(rename = "session.snapshot")]
    SessionSnapshot(EmptyParams),
    #[serde(rename = "workspace.create")]
    WorkspaceCreate(WorkspaceCreateParams),
    #[serde(rename = "workspace.list")]
    WorkspaceList(EmptyParams),
    #[serde(rename = "workspace.get")]
    WorkspaceGet(WorkspaceTarget),
    #[serde(rename = "workspace.focus")]
    WorkspaceFocus(WorkspaceTarget),
    #[serde(rename = "workspace.rename")]
    WorkspaceRename(WorkspaceRenameParams),
    #[serde(rename = "workspace.move")]
    WorkspaceMove(WorkspaceMoveParams),
    #[serde(rename = "workspace.close")]
    WorkspaceClose(WorkspaceTarget),
    #[serde(rename = "worktree.list")]
    WorktreeList(WorktreeListParams),
    #[serde(rename = "worktree.create")]
    WorktreeCreate(WorktreeCreateParams),
    #[serde(rename = "worktree.open")]
    WorktreeOpen(WorktreeOpenParams),
    #[serde(rename = "worktree.remove")]
    WorktreeRemove(WorktreeRemoveParams),
    #[serde(rename = "tab.create")]
    TabCreate(TabCreateParams),
    #[serde(rename = "tab.list")]
    TabList(TabListParams),
    #[serde(rename = "tab.get")]
    TabGet(TabTarget),
    #[serde(rename = "tab.focus")]
    TabFocus(TabTarget),
    #[serde(rename = "tab.rename")]
    TabRename(TabRenameParams),
    #[serde(rename = "tab.move")]
    TabMove(TabMoveParams),
    #[serde(rename = "tab.close")]
    TabClose(TabTarget),
    #[serde(rename = "agent.list")]
    AgentList(AgentListParams),
    #[serde(rename = "agent.get")]
    AgentGet(AgentTarget),
    #[serde(rename = "agent.read")]
    AgentRead(AgentReadParams),
    #[serde(rename = "agent.explain")]
    AgentExplain(AgentTarget),
    #[serde(rename = "agent.send")]
    AgentSend(AgentSendParams),
    #[serde(rename = "agent.rename")]
    AgentRename(AgentRenameParams),
    #[serde(rename = "agent.focus")]
    AgentFocus(AgentTarget),
    #[serde(rename = "agent.start")]
    AgentStart(AgentStartParams),
    #[serde(rename = "agent.attach.prepare")]
    AgentAttachPrepare(AgentAttachPrepareParams),
    #[serde(rename = "peer.agent.list")]
    PeerAgentList(EmptyParams),
    #[serde(rename = "peer.agent.get")]
    PeerAgentGet(AgentTarget),
    #[serde(rename = "peer.agent.read")]
    PeerAgentRead(AgentReadParams),
    #[serde(rename = "peer.agent.explain")]
    PeerAgentExplain(AgentTarget),
    #[serde(rename = "peer.agent.send")]
    PeerAgentSend(AgentSendParams),
    #[serde(rename = "peer.agent.rename")]
    PeerAgentRename(AgentRenameParams),
    #[serde(rename = "peer.agent.start")]
    PeerAgentStart(AgentStartParams),
    #[serde(rename = "terminal.delegate.create")]
    TerminalDelegateCreate(TerminalDelegateCreateParams),
    #[serde(rename = "terminal.delegate.claim")]
    TerminalDelegateClaim(TerminalDelegateClaimParams),
    #[serde(rename = "terminal.delegate.status")]
    TerminalDelegateStatus(TerminalDelegationTarget),
    #[serde(rename = "terminal.delegate.terminate")]
    TerminalDelegateTerminate(TerminalDelegationTarget),
    #[serde(rename = "terminal.delegate.handoff")]
    TerminalDelegateHandoff(TerminalDelegateHandoffParams),
    #[serde(rename = "pane.split")]
    PaneSplit(PaneSplitParams),
    #[serde(rename = "pane.swap")]
    PaneSwap(PaneSwapParams),
    #[serde(rename = "pane.move")]
    PaneMove(PaneMoveParams),
    #[serde(rename = "pane.zoom")]
    PaneZoom(PaneZoomParams),
    #[serde(rename = "pane.layout")]
    PaneLayout(PaneLayoutParams),
    #[serde(rename = "pane.process_info")]
    PaneProcessInfo(PaneProcessInfoParams),
    #[serde(rename = "layout.export")]
    LayoutExport(LayoutExportParams),
    #[serde(rename = "layout.apply")]
    LayoutApply(LayoutApplyParams),
    #[serde(rename = "layout.set_split_ratio")]
    LayoutSetSplitRatio(LayoutSetSplitRatioParams),
    #[serde(rename = "pane.neighbor")]
    PaneNeighbor(PaneNeighborParams),
    #[serde(rename = "pane.edges")]
    PaneEdges(PaneEdgesParams),
    #[serde(rename = "pane.focus_direction")]
    PaneFocusDirection(PaneFocusDirectionParams),
    #[serde(rename = "pane.resize")]
    PaneResize(PaneResizeParams),
    #[serde(rename = "pane.list")]
    PaneList(PaneListParams),
    #[serde(rename = "pane.current")]
    PaneCurrent(PaneCurrentParams),
    #[serde(rename = "pane.get")]
    PaneGet(PaneTarget),
    #[serde(rename = "pane.focus")]
    PaneFocus(PaneTarget),
    #[serde(rename = "pane.rename")]
    PaneRename(PaneRenameParams),
    #[serde(rename = "pane.send_text")]
    PaneSendText(PaneSendTextParams),
    #[serde(rename = "pane.send_keys")]
    PaneSendKeys(PaneSendKeysParams),
    #[serde(rename = "pane.send_input")]
    PaneSendInput(PaneSendInputParams),
    #[serde(rename = "pane.read")]
    PaneRead(PaneReadParams),
    #[serde(rename = "pane.report_agent")]
    PaneReportAgent(PaneReportAgentParams),
    #[serde(rename = "pane.report_agent_session")]
    PaneReportAgentSession(PaneReportAgentSessionParams),
    #[serde(rename = "pane.report_metadata")]
    PaneReportMetadata(PaneReportMetadataParams),
    #[serde(rename = "pane.clear_agent_authority")]
    PaneClearAgentAuthority(PaneClearAgentAuthorityParams),
    #[serde(rename = "pane.release_agent")]
    PaneReleaseAgent(PaneReleaseAgentParams),
    #[serde(rename = "pane.close")]
    PaneClose(PaneTarget),
    #[serde(rename = "events.subscribe")]
    EventsSubscribe(EventsSubscribeParams),
    #[serde(rename = "events.wait")]
    EventsWait(EventsWaitParams),
    #[serde(rename = "peer.register")]
    PeerRegister(PeerRegisterParams),
    #[serde(rename = "peer.connect_ssh")]
    PeerConnectSsh(PeerConnectSshParams),
    #[serde(rename = "peer.disconnect_ssh")]
    PeerDisconnectSsh(PeerDisconnectSshParams),
    #[serde(rename = "peer.keepalive_ssh")]
    PeerKeepaliveSsh(PeerKeepaliveSshParams),
    #[serde(rename = "remote.resume")]
    RemoteResume(RemoteResumeParams),
    #[serde(rename = "peer.presentation.activate")]
    PeerPresentationActivate(TerminalDelegationClaim),
    #[serde(rename = "peer.unregister")]
    PeerUnregister(PeerTarget),
    #[serde(rename = "peer.list")]
    PeerList(EmptyParams),
    #[serde(rename = "peer.health")]
    PeerHealth(PeerTarget),
    #[serde(rename = "pane.wait_for_output")]
    PaneWaitForOutput(PaneWaitForOutputParams),
    #[serde(rename = "integration.install")]
    IntegrationInstall(IntegrationInstallParams),
    #[serde(rename = "integration.uninstall")]
    IntegrationUninstall(IntegrationUninstallParams),
    #[serde(rename = "plugin.link")]
    PluginLink(PluginLinkParams),
    #[serde(rename = "plugin.list")]
    PluginList(PluginListParams),
    #[serde(rename = "plugin.unlink")]
    PluginUnlink(PluginUnlinkParams),
    #[serde(rename = "plugin.enable")]
    PluginEnable(PluginSetEnabledParams),
    #[serde(rename = "plugin.disable")]
    PluginDisable(PluginSetEnabledParams),
    #[serde(rename = "plugin.action.list")]
    PluginActionList(PluginActionListParams),
    #[serde(rename = "plugin.action.invoke")]
    PluginActionInvoke(PluginActionInvokeParams),
    #[serde(rename = "plugin.log.list")]
    PluginLogList(PluginLogListParams),
    #[serde(rename = "plugin.pane.open")]
    PluginPaneOpen(PluginPaneOpenParams),
    #[serde(rename = "plugin.pane.focus")]
    PluginPaneFocus(PluginPaneFocusParams),
    #[serde(rename = "plugin.pane.close")]
    PluginPaneClose(PluginPaneCloseParams),
}

#[cfg(test)]
mod tests;
