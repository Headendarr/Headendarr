# Frontend UI Standards

These standards define the default UI patterns for Headendarr frontend work.  
Use shared components first. Do not introduce ad-hoc UI patterns unless there is a documented exception.

## Scope

- Applies to all Vue/Quasar frontend changes in `frontend/src/`.
- Shared UI components live in `frontend/src/components/ui/`.
- This document is the source of truth for dialogs, buttons, and inputs.

## Strict Rules

1. Use shared `ui` components by default.
2. Do not create one-off dialog headers/footers in feature components when an existing shared dialog component can be used.
3. Do not create one-off list action button groups; use the shared list action components.
4. Do not add custom search/select wrappers in pages/components if existing shared input components cover the need.
5. Use `TicButtonDropdown` for standard action dropdown menus instead of raw `q-btn-dropdown`.
6. If a shared component is missing required functionality:
   - extend the shared component first, then reuse it.
   - document the new API in this file in the same change.
7. In Vue script blocks (`<script setup>` / `<script>`), prefer single-quoted strings.
8. Action colour semantics:
   - Save/apply/confirm success actions use `positive`.
   - Destructive actions (delete/remove) use `negative`.
   - Other actions keep existing contextual colours.
9. Row/list action icon + colour mappings must use the standard mapping table below unless a documented exception is added.
10. Settings/edit forms must use a form-level spacing rule of `24px` between fields (do not hardcode spacing inside shared input components).
11. Dropdown menus for shared button/action components must use the shared `tic-dropdown-menu` class for consistent border/shadow/theme behaviour.
12. Border radius must use shared radius tokens from `frontend/src/css/app.scss` (`--tic-radius-sm`, `--tic-radius-md`, `--tic-radius-lg`), not ad-hoc values.
13. Elevated surfaces (dropdowns, popup cards) must use shared border/shadow tokens (`--tic-elevated-border`, `--tic-elevated-shadow`) so dark/light theme rendering stays consistent.
14. For list views that already provide search/filter controls, prefer lazy loading (infinite scroll) over footer pagination controls/counters.
15. Use checkboxes (not toggles) for multi-select list/table workflows (for example stream selectors and bulk-edit selectors). Reserve toggles for boolean settings fields in forms.
16. Any new or updated UI must be theme-aware for both light and dark themes. Do not hardcode one-theme colours in feature components.
17. Section-level toolbar controls (search/filter/sort/action rows) must use shared alignment classes:
   - `section-toolbar-field` for shared input/select controls in the toolbar row.
   - `section-toolbar-btn` for toolbar action buttons.
18. Page content padding must be responsive:
   - for screens `>= 600px`, use `<q-page><div class='q-pa-md'>...`
   - for screens `< 600px`, use `q-pa-none` at the page content wrapper.

## Shared Component Catalog

### Buttons

- `TicButton` (`frontend/src/components/ui/buttons/TicButton.vue`)
  - Variants: `filled`, `outline`, `flat`, `text`
  - Use for general actions in forms/dialogs/pages.
- `TicActionButton` (`frontend/src/components/ui/buttons/TicActionButton.vue`)
  - Standard icon-only row action (`flat dense round`, `size="12px"`).
- `TicListActions` (`frontend/src/components/ui/buttons/TicListActions.vue`)
  - Desktop: icon action cluster.
  - Mobile (`<600px`): collapses to overflow menu.
  - Mobile overflow menu uses shared `tic-dropdown-menu` class.
- `TicButtonDropdown` (`frontend/src/components/ui/buttons/TicButtonDropdown.vue`)
  - Standardised dropdown button style for action menus.
  - Menu content uses shared `tic-dropdown-menu` class.

### Dropdown Menu Style Rules (Strict)

1. Shared dropdown menus must use `.tic-dropdown-menu` for theme-consistent menu chrome.
2. Do not use ad-hoc per-component menu shadows/borders for shared action/dropdown menus.
3. If dropdown styling changes, update `frontend/src/css/app.scss` `.tic-dropdown-menu` and do not fork styles locally.

### Border Radius Rules (Strict)

1. Do not hardcode new border-radius values in feature components unless there is a documented exception.
2. Use shared radius tokens from `frontend/src/css/app.scss`:
   - `--tic-radius-sm: 3px`
   - `--tic-radius-md: 4px`
   - `--tic-radius-lg: 6px`
3. For modal/popup card chrome (for example `TicDialogPopup`), default to `--tic-radius-sm` for subtle rounding.
4. If a new global radius size is needed, add a token first and document it here in the same change.

### Elevated Surface Rules (Strict)

1. Elevated menu/dialog chrome must use `--tic-elevated-border` and `--tic-elevated-shadow`.
2. Do not hardcode separate dark-mode shadow values in each component.
3. If shadow style changes, update the global tokens in `frontend/src/css/app.scss`.

### Theme Awareness Rules (Strict)

1. All UI work must be visually validated in both light and dark themes.
2. Use Quasar/theme tokens (`var(--q-*)`) and shared Headendarr tokens from `frontend/src/css/app.scss` for colours, borders, and shadows.
3. Do not hardcode hex/rgb colours in feature components unless there is a documented exception.
4. If new theme tokens are required, add them centrally in `frontend/src/css/app.scss` and document them here in the same change.

### Dialogs

- `TicDialogWindow` (`frontend/src/components/ui/dialogs/TicDialogWindow.vue`)
  - Side window style, sticky header, standard close/back/actions.
  - For settings/edit dialogs (e.g. source/EPG/channel settings).
  - Supports unsaved-change protection with `persistent`, `preventClose`, and `close-request`.
- `TicDialogPopup` (`frontend/src/components/ui/dialogs/TicDialogPopup.vue`)
  - Popup modal style with standard header/actions.
  - For confirmation/details/modal forms.
  - `actions` slot is optional. Omit it when the popup does not need footer actions.
  - Use `:show-actions='false'` when a popup should force-hide footer actions.
- `TicConfirmDialog` (`frontend/src/components/ui/dialogs/TicConfirmDialog.vue`)
  - Standard confirm/destructive confirmation dialog template.
  - Use for delete/remove/discard confirmations via `$q.dialog({ component: TicConfirmDialog, ... })`.

## Confirm Dialog Rules (Strict)

1. Do not use ad-hoc `$q.dialog({ title, message, cancel: true })` confirms in feature code.
2. Use `TicConfirmDialog` for all confirmation dialogs so copy/layout/actions stay consistent.
3. Destructive confirmations must:
   - use `confirmColor='negative'`
   - use `confirmIcon='delete'` (or another documented destructive icon when delete is not semantically correct)
   - include explicit irreversible wording (e.g. "This action is final and cannot be undone.").
4. Non-destructive confirmations should use contextual icon/colours but still use `TicConfirmDialog`.

## Dialog Save/Dirty Rules (Strict)

1. Editable dialogs must track dirty state (`isDirty`) against initial loaded state.
2. Editable dialogs must provide a header `Save` action via `TicDialogWindow` actions.
3. Save action must pulse when `isDirty` is true.
4. Dirty dialogs must prevent accidental close:
   - set `persistent` and `preventClose`
   - handle `@close-request` and require discard confirmation
5. New/refactored settings dialogs must follow this pattern unless explicitly documented as read-only.

## Page Settings Auto-Save Rules (Strict)

1. Page-level settings forms (non-dialog pages) must auto-save. Do not require a manual Save button.
2. Auto-save must be debounced (default: 3 seconds) for normal input changes.
3. Text and number inputs should trigger an immediate save on `blur` in addition to debounced saves.
4. Auto-save must flush pending changes before navigation/unmount:
   - implement `beforeRouteLeave` and await a flush function
   - implement `beforeUnmount` and flush pending changes
5. Show notification toasts only for failed saves (negative). Do not show positive "Saved" toasts for background auto-save.
6. Keep dialog edit forms on explicit Save actions. Do not apply this auto-save pattern to editable dialogs that follow the dialog dirty-state pattern above.

### Inputs

- `TicSearchInput` (`frontend/src/components/ui/inputs/TicSearchInput.vue`)
  - Search icon + labeled search control.
- `TicSelectInput` (`frontend/src/components/ui/inputs/TicSelectInput.vue`)
  - Standard select with optional local search and multi-select, plus optional `description`.
  - `clearable` is opt-in (default `false`).
- `TicTextInput` (`frontend/src/components/ui/inputs/TicTextInput.vue`)
  - Standard single-line text input wrapper with `label` and optional `description`.
- `TicTextareaInput` (`frontend/src/components/ui/inputs/TicTextareaInput.vue`)
  - Standard multiline textarea wrapper with `label` and optional `description`.
- `TicNumberInput` (`frontend/src/components/ui/inputs/TicNumberInput.vue`)
  - Standard numeric input wrapper with `label` and optional `description` (for settings like padding values).
- `TicToggleInput` (`frontend/src/components/ui/inputs/TicToggleInput.vue`)
  - Standard boolean toggle wrapper with title-style `label` and supporting `description`.
  - Uses input-like underline, and theme-aware row hover highlight.

## Input Rules (Strict)

1. Use `TicTextInput` for normal text fields.
2. Use `TicTextareaInput` for multi-line fields (do not hand-roll textarea usage repeatedly).
3. Use `TicNumberInput` for numeric fields.
4. Use `TicToggleInput` for boolean settings controls with label + description content.
5. Do not use `q-checkbox` for settings toggles.
6. Keep toolbar search and filter controls on `TicSearchInput` / `TicSelectInput` unless there is a documented exception.
7. For settings forms, inputs should provide a label and description text where guidance is needed (e.g., host fields, padding fields, routing toggles).
8. All shared form inputs use outlined style in Headendarr.
9. In forms, spacing between fields must be managed at the form container level with `24px` spacing.
10. Use `TicToggleInput` only for boolean form controls; never for selecting items in a result list.

## List Loading + Selection Rules (Strict)

1. If a list supports search and/or filter controls, use lazy loading (`q-infinite-scroll`) as the default loading model.
2. Do not add classic page-number pagination controls to these searchable/filterable lists unless there is a documented performance or UX exception.
3. Multi-select result lists must use checkbox selection patterns:
   - row checkbox
   - `Select page` checkbox in actions/toolbar
   - selection banner that can escalate from page selection to "select all matching".
4. For compact/mobile list dialogs, keep filters/sort/search in a sticky actions area and allow collapsing that area while scrolling.
5. Sort and filter configuration should use popup dialogs (`TicDialogPopup`) with explicit `Clear` and `Apply` actions.

### Form Spacing Standard (Strict)

Use a form container class, not per-input margin hacks:

```css
.tic-form-layout > *:not(:last-child) {
  margin-bottom: 24px;
}
```

Do not add fixed bottom spacing inside `ui/inputs` components for form layout concerns.

### Section Toolbar Alignment (Strict)

Use shared global utility classes for top-of-section toolbars:

- `section-toolbar-field`
  - Applies consistent vertical spacing/alignment for `TicSearchInput`, `TicSelectInput`, `TicTextInput`, and `TicNumberInput` inside toolbar rows.
- `section-toolbar-btn`
  - Applies consistent control height for toolbar action buttons.
- On `<600px`, filter/sort button pairs must:
  - stay on one row (`col-6` each),
  - keep `Filters` on the left and `Sort` on the right,
  - use compact dense styling via `section-toolbar-btn--compact`,
  - use `section-toolbar-split-left` / `section-toolbar-split-right` for alignment.

Example:

```vue
<div class='row q-col-gutter-sm items-end'>
  <div class='col-12 col-sm-4'>
    <TicSearchInput class='section-toolbar-field' v-model='search' label='Search' />
  </div>
  <div class='col-auto'>
    <TicButton class='section-toolbar-btn' label='Sort' icon='sort' color='secondary' />
  </div>
</div>
```

## Nested Settings Rules (Strict)

1. When settings are conditionally nested under a parent toggle/select, wrap them in a `.sub-setting` container.
2. `.sub-setting` must provide left border + indent (visual parent/child relationship).
3. Do not use ad-hoc nested spacing patterns for this scenario.
4. Example use-cases:
   - XC account fields nested under `Source Type = XC`
   - custom HLS proxy fields nested under `Use HLS proxy`

## Usage Examples

### Dialog

```vue
<TicDialogWindow
  v-model="showDialog"
  title="Channel Settings"
  :persistent="isDirty"
  :prevent-close="isDirty"
  :actions="[
    { id: 'save', icon: 'save', label: 'Save', color: 'primary', class: isDirty ? 'save-action-pulse' : '' }
  ]"
  @action="onDialogAction"
  @close-request="confirmDiscard"
>
  <q-form class="tic-form-layout">
    <!-- form fields -->
  </q-form>
</TicDialogWindow>
```

### List Actions

```vue
<TicListActions
  :actions="[
    { id: 'edit', icon: 'edit', label: 'Edit', color: 'primary' },
    { id: 'delete', icon: 'delete', label: 'Delete', color: 'negative' }
  ]"
  @action="onRowAction"
/>
```

### Compact List Card Layout (`<1024px`)

Use `TicListItemCard` for compact list rows that need a header with left content + right action buttons and a separate body section.

`TicListItemCard` supports optional parent-driven visual accents via props:
- `accent-color`
- `surface-color`
- `header-color`
- `text-color`

This keeps `TicListItemCard` generic while allowing each page to pass theme-token-backed colours for status-based highlighting.
Current list-card theme token groups are:
- `--tic-list-card-healthy-*`
- `--tic-list-card-disabled-*`
- `--tic-list-card-issues-*`
- `--tic-list-card-error-*`

```vue
<TicListItemCard
  accent-color="var(--tic-list-card-healthy-border)"
  surface-color="var(--tic-list-card-healthy-bg)"
  header-color="var(--tic-list-card-healthy-header)"
>
  <template #header-left>
    <div class="text-caption text-grey-7">Reorder</div>
  </template>
  <template #header-actions>
    <TicActionButton icon="edit" color="primary" tooltip="Edit" />
    <TicActionButton icon="delete" color="negative" tooltip="Delete" />
  </template>

  <div>Row body content goes here.</div>
</TicListItemCard>
```

## Standard Row Action Mapping (Strict)

Use these defaults for all list/table row actions:

- `add`: `icon='add'`, `color='primary'`
- `edit`: `icon='edit'`, `color='primary'`
- `configure`: `icon='tune'`, `color='grey-8'`
- `update/refresh/sync`: `icon='update'`, `color='info'`
- `play/preview`: `icon='play_arrow'`, `color='primary'`
- `copy`: `icon='content_copy'`, `color='grey-8'`
- `delete/remove`: `icon='delete'`, `color='negative'`
- `warning/fix-attention`: `icon='warning'`, `color='warning'`

If an action is not listed, choose the closest semantic icon/colour and document it here in the same PR.

### Search + Select

```vue
<TicSearchInput v-model="search" label="Search channels" />

<TicSelectInput
  v-model="selectedTags"
  :options="tagOptions"
  label="Categories"
  multiple
  searchable
/>
```

### Standard Form Inputs

```vue
<TicTextInput
  v-model="ticHost"
  label="Headendarr Host"
  description="Hostname/IP used by Headendarr for internal and generated service URLs."
/>
<TicNumberInput
  v-model="prePaddingMinutes"
  label="Pre-recording padding (minutes)"
  description="Minutes added before a recording starts."
/>
<TicTextareaInput
  v-model="logoUrl"
  label="Logo URL"
  description="Optional channel logo source URL."
/>
<TicToggleInput
  v-model="routeViaTvh"
  label="Route playlists & HDHomeRun through TVHeadend"
  description="Enable to force stream routing through TVH proxy and network/mux/service mapping."
/>
```

## Responsive Rules

- `>=1024px`: full desktop layouts and inline action groups.
- `<1024px`: compact spacing and controls where needed.
- `<600px`: mobile layout rules apply, including collapsed row actions and mobile dialog behaviour.
- For top-page toolbars (e.g. add button + search/filter controls), use full-width single-item rows at `<600px`.
- At `<600px`, each toolbar control should occupy one row (`col-12`) and primary actions should be full width.
- Prefer Quasar responsive utility classes (`gt-sm`, `lt-md`, `gt-md`, etc.) for simple show/hide behaviour in templates.
- Avoid `$q.screen` in template conditionals when CSS utility classes can express the same visibility rule; keep `$q.screen` for logic/layout decisions that cannot be handled by classes alone.
- Do not use `$q.platform.is.mobile` for responsive layout decisions; use Quasar visibility classes for template visibility and `$q.screen`/`useMobile` only when JavaScript logic is required.
- Use `frontend/src/composables/useMobile.js` for component-level mobile/desktop logic.
- Do not re-implement mobile detection logic per component.

## Exceptions Process

If you must deviate:

1. Add a short comment in the component explaining why shared UI cannot be used.
2. Add/adjust a shared component if reusable.
3. Update this document with the new pattern.

If steps 1-3 are not done, the change is not standards-compliant.
