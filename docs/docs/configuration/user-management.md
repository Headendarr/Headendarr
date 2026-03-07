---
title: User Management
---

# User Management

Headendarr allows you to create multiple user accounts to control access to the web interface and streaming resources. As an administrator, you can manage users from the **Settings** -> **Users** page.

## Creating a New User

1.  Navigate to the **Users** page.
2.  Click the **ADD USER** button to open the "Create User" dialog.
3.  Fill in the user's details:
    - **Username**: The name the user will use to log in.
    - **Password**: The password for the user account.
4.  Assign permissions to the user (see below).
5.  Click **Save**.

## Permissions

Headendarr utilises a role-based access control system, offering either an "Admin" or "Streamer" role.

- **Admin**: Provides full administrative access to both the Headendarr platform and the integrated TVHeadend backend. Users with this role can manage all application settings, other users, and have complete control over the system.
- **Streamer**: Offers a highly limited subset of permissions, primarily for accessing available streams. Users with this role cannot modify settings or manage other users.

In addition to roles, you can configure granular access controls for how clients utilise Headendarr's streaming capabilities and DVR functions:

- **DVR Access**: Controls the user's ability to interact with DVR functionalities through client applications. Options include:
  - **No DVR access**: The user cannot access DVR features or make recordings.
  - **Access own recordings only**: The user can access and manage their own recordings and make new recordings. (Suitable for a "Streamer" role if DVR access is desired.)
  - **Grant access to everyone's recordings**: The user has full access to view and manage all recordings in the system.
- **Retention Policy**: (Available if DVR Access is granted) A dropdown to select how long recordings are retained for this user before automatic deletion.
  See [Retention Policy Values](./application-settings#retention-policy-values) for what each option means and how it affects cleanup behaviour.

## Editing Users and Resetting Login Passwords

- To **edit a user's permissions** or active status, click the **Edit** icon (pencil) next to their name in the user list.
- To **change a user's main login password** (for the web interface), click the **Reset password** button (key icon). A dialog will appear where you can enter the new password.
- To **delete a user**, click the **Delete** icon.

## The Streaming Key: Your Universal Client Password

Each user in Headendarr is automatically assigned a unique **Streaming Key**. This key is the most important piece of information for connecting any client application.

Think of it this way:

- Your **Main Password** is only for logging into the Headendarr web interface.
- Your **Streaming Key** is the password for everything else.

### How the Streaming Key is Used

The streaming key is the password for **all** client access methods:

- **TVHeadend HTSP**: When your client asks for a username and password, you use your Headendarr username and your **Streaming Key** as the password.
- **Xtream Codes (XC) API**: For clients like TiviMate, you use your Headendarr username and your **Streaming Key** as the password.
- **M3U Playlist URLs**: The key is used as a parameter in the URL to authenticate (`...playlist.m3u?key=YOUR_STREAM_KEY`).
- **HDHomeRun Emulation**: The key is part of the URL used to identify the tuner to clients like Plex.

This unified system means you don't need to manage separate passwords in TVHeadend or other parts of the application. Users created in Headendarr are automatically synced to the TVHeadend backend, with their streaming key set as their password.

### Finding and Resetting Your Streaming Key

You can find each user's stream key on the **Users** page.

If you suspect a user's stream key has been compromised, you can easily reset it:

1.  Click the **Reset Key** icon (a refresh/rotate icon) next to the user's stream key in the list.
2.  A new random key will be generated for the user.

:::warning
Resetting a stream key will immediately invalidate the old one. You will need to update the password in **any and all** client applications that were using the old key.
:::

## OIDC Single Sign-On

Headendarr can use OpenID Connect (OIDC) for SSO logins and automatic user provisioning.

OIDC settings are backend-only and must be provided as container environment variables. These values are not editable in the UI.

### Core Variables

| Variable                      | Description                                 | Default                       | Example                                                     |
| ----------------------------- | ------------------------------------------- | ----------------------------- | ----------------------------------------------------------- |
| `TIC_AUTH_OIDC_ENABLED`       | Enables OIDC login support.                 | `false`                       | `true`                                                      |
| `TIC_AUTH_OIDC_ISSUER_URL`    | OIDC issuer base URL used for discovery.    | _none_                        | `https://auth.example.com`                                  |
| `TIC_AUTH_OIDC_CLIENT_ID`     | OIDC client ID registered in your provider. | _none_                        | `headendarr`                                                |
| `TIC_AUTH_OIDC_CLIENT_SECRET` | OIDC client secret for token exchange.      | _none_                        | `replace-me`                                                |
| `TIC_AUTH_OIDC_REDIRECT_URI`  | Callback URL registered in your provider.   | _none_                        | `https://headendarr.example.com/tic-api/auth/oidc/callback` |
| `TIC_AUTH_OIDC_SCOPES`        | Comma-separated scopes requested at login.  | `openid,profile,email,groups` | `openid,profile,email,groups`                               |

### Claim and Role Mapping Variables

| Variable                        | Description                                    | Default              | Example                            |
| ------------------------------- | ---------------------------------------------- | -------------------- | ---------------------------------- |
| `TIC_AUTH_OIDC_USERNAME_CLAIM`  | Claim used for local username generation.      | `preferred_username` | `preferred_username`               |
| `TIC_AUTH_OIDC_EMAIL_CLAIM`     | Claim used for local email capture.            | `email`              | `email`                            |
| `TIC_AUTH_OIDC_GROUPS_CLAIM`    | Claim used for group/role mapping.             | `groups`             | `groups`                           |
| `TIC_AUTH_OIDC_ADMIN_GROUPS`    | Comma-separated groups that map to `admin`.    | _empty_              | `headendarr-admin,tvh-admin`       |
| `TIC_AUTH_OIDC_STREAMER_GROUPS` | Comma-separated groups that map to `streamer`. | _empty_              | `headendarr-streamer,tvh-streamer` |
| `TIC_AUTH_OIDC_DEFAULT_ROLE`    | Fallback role when no groups match.            | `none`               | `none` or `streamer`               |

:::info Identity Linking
OIDC users are linked by issuer and subject (`iss` + `sub`) and are re-used on future logins.
:::

### Behaviour and Security Variables

| Variable                            | Description                                                            | Default                                                     | Example            |
| ----------------------------------- | ---------------------------------------------------------------------- | ----------------------------------------------------------- | ------------------ |
| `TIC_AUTH_OIDC_AUTO_PROVISION`      | Creates local users on first successful OIDC login (JIT provisioning). | `true`                                                      | `true`             |
| `TIC_AUTH_OIDC_SYNC_ROLES_ON_LOGIN` | Re-syncs mapped roles at each OIDC login.                              | `true`                                                      | `true`             |
| `TIC_AUTH_OIDC_VERIFY_TLS`          | Enables TLS certificate verification for OIDC HTTP calls.              | `true`                                                      | `true`             |
| `TIC_AUTH_OIDC_CLOCK_SKEW_SECONDS`  | JWT time-claim leeway for minor clock drift.                           | `60`                                                        | `60`               |
| `TIC_AUTH_LOCAL_LOGIN_ENABLED`      | Explicitly enables/disables username/password login.                   | Derived from `TIC_AUTH_OIDC_DISABLE_LOCAL_LOGIN` when unset | `true`             |
| `TIC_AUTH_OIDC_DISABLE_LOCAL_LOGIN` | Convenience flag to disable local login when using OIDC.               | `false`                                                     | `false`            |
| `TIC_AUTH_OIDC_BUTTON_LABEL`        | Login-page label for the OIDC button.                                  | `Sign in with SSO`                                          | `Sign in with SSO` |

:::important Stream Keys
Users provisioned through OIDC are still assigned stream keys for playlist and client access.
:::

### Role Mapping Behaviour

- If a user matches any value in `TIC_AUTH_OIDC_ADMIN_GROUPS`, they receive `admin`.
- If a user matches any value in `TIC_AUTH_OIDC_STREAMER_GROUPS`, they receive `streamer`.
- If neither mapping matches:
  - `TIC_AUTH_OIDC_DEFAULT_ROLE=streamer` grants streamer access.
  - `TIC_AUTH_OIDC_DEFAULT_ROLE=none` denies login.
- When `TIC_AUTH_OIDC_SYNC_ROLES_ON_LOGIN=true`, role assignments are refreshed from claim mapping at each login.
