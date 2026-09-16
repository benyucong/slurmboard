import Foundation
import Security

enum CredentialStore {
    private static let destinationService = "fi.aalto.slurmboard.ssh-password"
    private static let jumpService = "fi.aalto.slurmboard.ssh-jump-password"

    static func password(for hostID: String) -> String? {
        password(for: hostID, service: destinationService)
    }

    static func jumpPassword(for hostID: String) -> String? {
        password(for: hostID, service: jumpService)
    }

    private static func password(for hostID: String, service: String) -> String? {
        let query: [String: Any] = [
            kSecClass as String: kSecClassGenericPassword,
            kSecAttrService as String: service,
            kSecAttrAccount as String: hostID,
            kSecReturnData as String: true,
            kSecMatchLimit as String: kSecMatchLimitOne,
        ]
        var result: CFTypeRef?
        guard SecItemCopyMatching(query as CFDictionary, &result) == errSecSuccess,
              let data = result as? Data else { return nil }
        return String(data: data, encoding: .utf8)
    }

    static func setPassword(_ password: String, for hostID: String) {
        setPassword(password, for: hostID, service: destinationService)
    }

    static func setJumpPassword(_ password: String, for hostID: String) {
        setPassword(password, for: hostID, service: jumpService)
    }

    private static func setPassword(_ password: String, for hostID: String, service: String) {
        let data = Data(password.utf8)
        let key: [String: Any] = [
            kSecClass as String: kSecClassGenericPassword,
            kSecAttrService as String: service,
            kSecAttrAccount as String: hostID,
        ]
        let update = [kSecValueData as String: data]
        if SecItemUpdate(key as CFDictionary, update as CFDictionary) == errSecItemNotFound {
            var item = key
            item[kSecValueData as String] = data
            item[kSecAttrAccessible as String] = kSecAttrAccessibleAfterFirstUnlock
            SecItemAdd(item as CFDictionary, nil)
        }
    }

    static func deletePassword(for hostID: String) {
        deletePassword(for: hostID, service: destinationService)
    }

    static func deleteJumpPassword(for hostID: String) {
        deletePassword(for: hostID, service: jumpService)
    }

    private static func deletePassword(for hostID: String, service: String) {
        let query: [String: Any] = [
            kSecClass as String: kSecClassGenericPassword,
            kSecAttrService as String: service,
            kSecAttrAccount as String: hostID,
        ]
        SecItemDelete(query as CFDictionary)
    }
}
