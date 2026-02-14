# TIC Shared UI Components (Draft)

This folder contains the first-pass standardized UI building blocks for TIC.

## Buttons

- `buttons/TicButton.vue`: common action button wrapper (`filled`, `outline`, `flat`, `text` variants).
- `buttons/TicActionButton.vue`: standard icon-only list action button.
- `buttons/TicListActions.vue`: responsive list action group (desktop icon cluster, mobile overflow menu).
- `buttons/TicButtonDropdown.vue`: standardized dropdown action button (outline + secondary).
- Shared dropdown menu styling is centralized in `.tic-dropdown-menu` (defined in `frontend/src/css/app.scss`).

## Dialogs

- `dialogs/TicDialogWindow.vue`: right-side window dialog with standardized sticky header, action buttons, back/close
  controls, plus unsaved-close protection support via `persistent`, `preventClose`, and `close-request`.
- `dialogs/TicDialogPopup.vue`: centered popup dialog with standardized sticky header and footer actions slot.

## Lists

- `lists/TicListItemCard.vue`: compact list card shell with a shared header/body structure:
  - `header-left` slot for contextual content (drag/status/title/channel)
  - `header-actions` slot for action buttons
  - default slot for row body content

## Inputs

- `inputs/TicSearchInput.vue`: standard search input with search icon.
- `inputs/TicSelectInput.vue`: standard select input with optional local search filtering and multi-select support.
- `inputs/TicTextInput.vue`: standard text input wrapper for form fields.
- `inputs/TicTextareaInput.vue`: standard multi-line text input wrapper.
- `inputs/TicNumberInput.vue`: standard numeric input wrapper.
- `inputs/TicToggleInput.vue`: standard boolean toggle wrapper with label + description block.

## Export Barrel

- `index.js` exports all components from this folder.
