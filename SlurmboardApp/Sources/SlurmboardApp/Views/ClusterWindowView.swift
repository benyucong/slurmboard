import SwiftUI
import WebKit
import AppKit

struct ClusterWindowView: View {
    @EnvironmentObject private var manager: ConnectionManager
    let connectionID: UUID

    var body: some View {
        Group {
            if let service = manager.service(for: connectionID) {
                DashboardWorkspace(service: service)
            } else {
                ContentUnavailableView(
                    "Connection closed",
                    systemImage: "bolt.slash",
                    description: Text("This cluster connection is no longer active.")
                )
            }
        }
        .frame(minWidth: 1420, minHeight: 680)
    }
}

private struct DashboardWorkspace: View {
    @EnvironmentObject private var manager: ConnectionManager
    @ObservedObject var service: DashboardService
    @State private var password = ""
    @State private var jumpPassword = ""
    @State private var rememberPassword = true

    var body: some View {
        ZStack {
            if let url = service.dashboardURL {
                DashboardWebView(url: url)
            } else {
                switch service.state {
                case .connecting:
                    statusView(title: "Connecting…",
                               detail: "Starting the remote dashboard on \(service.host.alias)…")
                case .failed(let message):
                    failedConnectionView(message: message)
                case .disconnected:
                    statusView(title: "Disconnected", detail: nil)
                case .connected:
                    statusView(title: "Loading dashboard…", detail: nil)
                }
            }
        }
    }

    private func failedConnectionView(message: String) -> some View {
        VStack(spacing: 12) {
            Image(systemName: "exclamationmark.triangle.fill")
                .font(.largeTitle).foregroundStyle(.red)
            Text("Connection failed").font(.headline)
            Text("Host: \(service.host.alias)")
                .font(.subheadline.weight(.medium))
            ScrollView {
                Text(message)
                    .font(.system(.callout, design: .monospaced))
                    .foregroundStyle(.secondary)
                    .frame(maxWidth: .infinity, alignment: .leading)
                    .textSelection(.enabled)
            }
            .frame(maxWidth: 680, maxHeight: 180)
            .padding(12)
            .background(Color.secondary.opacity(0.08), in: RoundedRectangle(cornerRadius: 8))
            Text("Check the SSH host or alias, network/VPN, credentials, and ~/.ssh/config, then retry.")
                .font(.footnote).foregroundStyle(.secondary)
                .multilineTextAlignment(.center)

            VStack(alignment: .leading, spacing: 10) {
                Text("SSH credentials").font(.subheadline.weight(.semibold))
                SecureField("Destination password for \(service.host.alias) (optional)", text: $password)
                    .textFieldStyle(.roundedBorder)
                    .onSubmit { connectWithPassword() }
                if let jumpHost = service.host.effectiveProxyJump {
                    SecureField("Jump-host password for \(jumpHost) (optional)", text: $jumpPassword)
                        .textFieldStyle(.roundedBorder)
                        .onSubmit { connectWithPassword() }
                    Text("The jump-host password is used only for SSH prompts from \(jumpHost).")
                        .font(.caption).foregroundStyle(.secondary)
                }
                Toggle("Save entered passwords in macOS Keychain", isOn: $rememberPassword)
                    .toggleStyle(.checkbox)
                Text("Passwords are supplied to the system SSH client and are never added to the command or host file.")
                    .font(.caption).foregroundStyle(.secondary)
            }
            .padding(14)
            .frame(maxWidth: 420)
            .background(Color.secondary.opacity(0.06))
            .clipShape(RoundedRectangle(cornerRadius: 12))

            HStack {
                Button("Retry") { manager.retryConnection(service.id) }
                    .buttonStyle(.bordered)
                Button("Connect with Password") { connectWithPassword() }
                    .buttonStyle(.borderedProminent)
                    .disabled(password.isEmpty && jumpPassword.isEmpty)
                Button("Copy Error") {
                    NSPasteboard.general.clearContents()
                    NSPasteboard.general.setString(message, forType: .string)
                }
            }
        }
        .padding(32).frame(maxWidth: .infinity, maxHeight: .infinity)
    }

    private func connectWithPassword() {
        guard !password.isEmpty || !jumpPassword.isEmpty else { return }
        let submittedPassword = password
        let submittedJumpPassword = jumpPassword
        password = ""
        jumpPassword = ""
        manager.retryConnection(service.id,
                                password: submittedPassword.isEmpty ? nil : submittedPassword,
                                jumpPassword: submittedJumpPassword.isEmpty ? nil : submittedJumpPassword,
                                rememberPassword: rememberPassword)
    }

    private func statusView(title: String, detail: String?) -> some View {
        VStack(spacing: 14) {
            ProgressView()
            Text(title).font(.headline)
            if let detail {
                Text(detail).font(.callout).foregroundStyle(.secondary)
                    .multilineTextAlignment(.center).textSelection(.enabled)
            }
            if service.state == .disconnected {
                Button("Retry") { manager.retryConnection(service.id) }.buttonStyle(.borderedProminent)
            }
        }
        .padding(32).frame(maxWidth: .infinity, maxHeight: .infinity)
    }
}

private struct DashboardWebView: NSViewRepresentable {
    let url: URL

    final class Coordinator { var loadedURL: URL? }

    func makeCoordinator() -> Coordinator { Coordinator() }

    func makeNSView(context: Context) -> WKWebView {
        let configuration = WKWebViewConfiguration()
        configuration.websiteDataStore = .nonPersistent()
        let view = WKWebView(frame: .zero, configuration: configuration)
        view.allowsMagnification = true
        load(in: view, coordinator: context.coordinator)
        return view
    }

    func updateNSView(_ view: WKWebView, context: Context) {
        load(in: view, coordinator: context.coordinator)
    }

    private func load(in view: WKWebView, coordinator: Coordinator) {
        guard coordinator.loadedURL != url else { return }
        coordinator.loadedURL = url
        view.load(URLRequest(url: url, cachePolicy: .reloadIgnoringLocalCacheData))
    }
}
