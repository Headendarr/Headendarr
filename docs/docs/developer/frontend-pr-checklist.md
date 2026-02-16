# Frontend PR Checklist

Use this checklist for all frontend PRs.

## Required

- [ ] I used shared components from `frontend/src/components/ui/` for dialogs/buttons/inputs where applicable.
- [ ] I did not introduce ad-hoc replacements for existing shared UI patterns.
- [ ] Mobile behaviour is verified at `<1024px` and `<600px`.
- [ ] List/table action controls collapse appropriately on small screens.
- [ ] Top-page toolbars (add/search/filter/actions) stack to single-item full-width rows at `<600px`.
- [ ] Labels/tooltips/messages are clear and consistent.
- [ ] I avoided regressions in existing flows.
- [ ] I verified the UI in both light and dark themes (no one-theme-only styling regressions).

## Dialogs

- [ ] Settings/edit dialogs use `TicDialogWindow` unless there is a documented exception.
- [ ] Popup dialogs use `TicDialogPopup` unless there is a documented exception.
- [ ] Confirmation dialogs use `TicConfirmDialog` (no ad-hoc confirm popups).
- [ ] Header actions and close/back behaviour follow shared dialog API.
- [ ] Editable dialogs implement dirty-state save pattern (pulsing save action + close/discard protection).

## Inputs

- [ ] Search controls use `TicSearchInput` where applicable.
- [ ] Select controls use `TicSelectInput` where applicable.
- [ ] Text fields use `TicTextInput` / `TicTextareaInput` where applicable.
- [ ] Numeric fields use `TicNumberInput` where applicable.
- [ ] Boolean settings use `TicToggleInput` (not `q-checkbox`).
- [ ] Settings inputs/toggles include clear labels and description text where guidance is required.
- [ ] Multi-select/searchable select behaviour is consistent.
- [ ] Settings/edit forms apply `24px` field spacing via a form-level layout class (not per-input component spacing).

## Actions

- [ ] Row action icons use shared list action components.
- [ ] Row/list action icons and colours match the standard mapping in `frontend-ui-standards.md`.
- [ ] Desktop uses inline action buttons; mobile uses overflow action menu.
- [ ] Shared dropdown/overflow menus use `.tic-dropdown-menu` (no ad-hoc menu shadows/borders).
- [ ] Save/apply actions use `positive`; destructive actions use `negative`.

## Docs and Standards

- [ ] If I added or changed a shared UI API, I updated:
  - `docs/docs/development/frontend-ui-standards.md`
- [ ] If I intentionally deviated from standards, I documented the reason and proposed a reusable follow-up.
