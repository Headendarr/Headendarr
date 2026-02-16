---
title: User Management
---

# User Management

Headendarr allows you to create multiple user accounts to control access to the web interface and streaming resources. As an administrator, you can manage users from the **Settings** -> **Users** page.

## Creating a New User

1.  Navigate to the **Users** page.
2.  Click the **ADD USER** button to open the "Create User" dialog.
3.  Fill in the user's details:
    -   **Username**: The name the user will use to log in.
    -   **Password**: The password for the user account.
4.  Assign permissions to the user (see below).
5.  Click **Save**.

## Permissions

Headendarr utilises a role-based access control system, offering either an "Admin" or "Streamer" role.

-   **Admin**: Provides full administrative access to both the Headendarr platform and the integrated TVHeadend backend. Users with this role can manage all application settings, other users, and have complete control over the system.
-   **Streamer**: Offers a highly limited subset of permissions, primarily for accessing available streams. Users with this role cannot modify settings or manage other users.

In addition to roles, you can configure granular access controls for how clients utilise Headendarr's streaming capabilities and DVR functions:

-   **DVR Access**: Controls the user's ability to interact with DVR functionalities through client applications. Options include:
    *   **No DVR access**: The user cannot access DVR features or make recordings.
    *   **Access own recordings only**: The user can access and manage their own recordings and make new recordings. (Suitable for a "Streamer" role if DVR access is desired.)
    *   **Grant access to everyone's recordings**: The user has full access to view and manage all recordings in the system.
-   **Retention Policy**: (Available if DVR Access is granted) A dropdown to select how long recordings are retained for this user before automatic deletion.

## Editing Users and Resetting Login Passwords

-   To **edit a user's permissions** or active status, click the **Edit** icon (pencil) next to their name in the user list.
-   To **change a user's main login password** (for the web interface), click the **Reset password** button (key icon). A dialog will appear where you can enter the new password.
-   To **delete a user**, click the **Delete** icon.

## The Streaming Key: Your Universal Client Password

Each user in Headendarr is automatically assigned a unique **Streaming Key**. This key is the most important piece of information for connecting any client application.

Think of it this way:
-   Your **Main Password** is only for logging into the Headendarr web interface.
-   Your **Streaming Key** is the password for everything else.

### How the Streaming Key is Used

The streaming key is the password for **all** client access methods:

-   **TVHeadend HTSP**: When your client asks for a username and password, you use your Headendarr username and your **Streaming Key** as the password.
-   **Xtream Codes (XC) API**: For clients like TiviMate, you use your Headendarr username and your **Streaming Key** as the password.
-   **M3U Playlist URLs**: The key is used as a parameter in the URL to authenticate (`...playlist.m3u?key=YOUR_STREAM_KEY`).
-   **HDHomeRun Emulation**: The key is part of the URL used to identify the tuner to clients like Plex.

This unified system means you don't need to manage separate passwords in TVHeadend or other parts of the application. Users created in Headendarr are automatically synced to the TVHeadend backend, with their streaming key set as their password.

### Finding and Resetting Your Streaming Key

You can find each user's stream key on the **Users** page.

If you suspect a user's stream key has been compromised, you can easily reset it:

1.  Click the **Reset Key** icon (a refresh/rotate icon) next to the user's stream key in the list.
2.  A new random key will be generated for the user.

:::warning
Resetting a stream key will immediately invalidate the old one. You will need to update the password in **any and all** client applications that were using the old key.
:::
