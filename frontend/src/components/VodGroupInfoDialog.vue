<template>
  <TicDialogWindow
    v-model="isOpen"
    title="VOD Category Settings"
    width="1023px"
    :persistent="isDirty"
    :prevent-close="isDirty"
    :close-tooltip="closeTooltip"
    :actions="dialogActions"
    @action="onDialogAction"
    @close-request="onCloseRequest"
    @hide="onDialogHide"
  >
    <div class="q-pa-lg q-gutter-md">
      <q-form class="tic-form-layout" @submit.prevent="save">
        <template v-if="loading">
          <q-skeleton type="QToggle" />
          <q-skeleton type="QInput" />
          <q-skeleton type="QInput" />
        </template>

        <template v-else>
          <TicToggleInput
            v-model="enabled"
            label="Enabled"
            description="Enable this VOD category for presentation to downstream XC clients."
          />

          <q-separator />

          <h5 class="q-my-none">Category Details</h5>

          <TicTextInput
            v-model="name"
            label="Category Name"
            description="Display name for this curated VOD category across Headendarr and downstream XC clients."
          />

          <TicToggleInput
            v-model="generateStrmFiles"
            label="Generate .strm Files"
            description="Enable per-user `.strm` export generation for this VOD category. Headendarr writes files under `/library/<username>/<category-type>/<category-name>/...` for users who currently have access to this VOD content and are configured to create VOD `.strm` files."
          />

          <AdmonitionBanner v-if="generateStrmFiles && contentType === 'series'" type="warning" class="q-mt-sm">
            Generating `.strm` files for TV series requires additional episode lookups for each series so Headendarr
            can build season and episode file names. This background process can take some time for large categories.
          </AdmonitionBanner>

          <AdmonitionBanner v-if="showNoEligibleStrmUsersWarning" type="warning" class="q-mt-sm">
            `.strm` generation for this category will do nothing unless at least one user is configured to create VOD
            `.strm` files. In most cases, enabling this for a single user is enough.
          </AdmonitionBanner>

          <TicTextInput
            v-if="generateStrmFiles"
            v-model="strmBaseUrl"
            label="Library Base URL"
            description="Required. Set the external base URL used inside generated `.strm` files for this category. Example: `http://192.168.1.117:9985`."
          />

          <q-separator />

          <h5 class="q-my-none">Upstream Source Categories</h5>

          <q-list bordered separator class="rounded-borders">
            <draggable
              v-model="selectedCategories"
              group="vod-upstream-categories"
              item-key="local_key"
              handle=".handle"
              :component-data="{tag: 'ul', name: 'flip-list', type: 'transition'}"
              v-bind="dragOptions"
            >
              <template #item="{element: category, index}">
                <q-item
                  :key="category.local_key"
                  class="q-px-none rounded-borders channel-source-row vod-category-row"
                >
                  <q-item-section avatar class="handle q-px-sm q-mx-sm">
                    <q-avatar rounded>
                      <q-icon name="format_line_spacing" style="max-width: 30px; cursor: grab">
                        <q-tooltip class="bg-white text-primary">Drag to set source category priority</q-tooltip>
                      </q-icon>
                    </q-avatar>
                  </q-item-section>

                  <q-separator inset vertical class="gt-xs" />

                  <q-item-section side class="q-px-sm q-mx-sm" style="max-width: 60px">
                    <q-item-label lines="1" class="text-left">
                      <span class="text-weight-medium">{{ index + 1 }}</span>
                    </q-item-label>
                  </q-item-section>

                  <q-separator inset vertical class="gt-xs" />

                  <q-item-section top class="q-mx-md">
                    <q-item-label lines="1" class="text-left">
                      <span class="text-weight-medium">{{ category.name }}</span>
                    </q-item-label>
                    <q-item-label caption class="row items-center justify-between no-wrap q-gutter-sm">
                      <span class="ellipsis">{{ category.playlist_name }}</span>
                      <q-btn
                        dense
                        flat
                        no-caps
                        color="primary"
                        class="source-settings-toggle"
                        :label="isCategoryConfigOpen(category) ? 'Hide settings' : 'Category Settings'"
                        :icon-right="isCategoryConfigOpen(category) ? 'expand_less' : 'expand_more'"
                        @click="toggleCategoryConfig(category)"
                      />
                    </q-item-label>

                    <div v-if="isCategoryConfigOpen(category)" class="sub-setting source-config-panel q-mt-md">
                      <TicTextInput
                        v-model="category.strip_title_prefixes"
                        label="Strip Title Prefixes"
                        description="Optional. Separate multiple values with commas. Example: `EN:`, `EN ★`, `FR ★`. Use this so matching movies and TV series from multiple XC sources can deduplicate correctly."
                      />

                      <TicTextInput
                        v-model="category.strip_title_suffixes"
                        label="Strip Title Suffixes"
                        description="Optional. Separate multiple values with commas. Example: `- 2025`, `[4K]`. Use this when providers append labels that would otherwise stop cross-source deduplication."
                      />
                    </div>
                  </q-item-section>

                  <q-item-section side class="q-mr-md channel-source-actions">
                    <TicListActions :actions="categoryActions"
                                    @action="(action) => onCategoryAction(action, category)" />
                  </q-item-section>
                </q-item>
              </template>
            </draggable>
          </q-list>

          <div class="row q-gutter-sm justify-end">
            <TicButton icon="add" label="Add Category" color="primary" @click="selectCategoriesFromList" />
          </div>
        </template>
      </q-form>
    </div>
  </TicDialogWindow>
</template>

<script>
import axios from 'axios';
import draggable from 'vuedraggable';
import VodSourceCategorySelectorDialog from 'components/VodSourceCategorySelectorDialog.vue';
import {
  AdmonitionBanner,
  TicButton,
  TicConfirmDialog,
  TicDialogWindow,
  TicListActions,
  TicTextInput,
  TicToggleInput,
} from 'components/ui';

export default {
  name: 'VodCategoryInfoDialog',
  components: {
    AdmonitionBanner,
    draggable,
    TicButton,
    TicDialogWindow,
    TicListActions,
    TicTextInput,
    TicToggleInput,
  },
  emits: ['ok', 'hide'],
  data() {
    return {
      isOpen: false,
      loading: false,
      saving: false,
      closeResult: null,
      initialStateSignature: '',
      editingCategoryId: null,
      contentType: 'movie',
      enabled: true,
      name: '',
      sortOrder: 0,
      profileId: null,
      generateStrmFiles: false,
      strmBaseUrl: '',
      selectedCategories: [],
      availableCategories: [],
      profileDefinitions: [],
      profileSettings: {},
      existingCategoryNames: [],
      eligibleStrmUserCount: 0,
      nextCategoryKey: 1,
      categoryActions: [
        {id: 'remove', icon: 'delete', label: 'Remove category', color: 'negative'},
      ],
    };
  },
  computed: {
    dragOptions() {
      return {
        animation: 100,
        group: 'vod-upstream-categories',
        ghostClass: 'ghost',
        direction: 'vertical',
        delay: 200,
        delayOnTouchOnly: true,
      };
    },
    isDirty() {
      if (!this.initialStateSignature) {
        return false;
      }
      return this.currentStateSignature() !== this.initialStateSignature;
    },
    closeTooltip() {
      return this.isDirty ? 'Unsaved changes. Save before closing or discard changes.' : 'Close';
    },
    dialogActions() {
      const actions = [
        {
          id: 'save',
          icon: 'save',
          label: 'Save',
          color: 'positive',
          disable: this.loading || this.saving,
          class: this.isDirty ? 'save-action-pulse' : '',
          tooltip: this.isDirty ? 'Save changes' : 'No unsaved changes',
        },
      ];
      if (this.editingCategoryId) {
        actions.push({
          id: 'delete',
          icon: 'delete',
          label: 'Delete',
          color: 'negative',
          disable: this.loading || this.saving,
          tooltip: 'Delete category',
        });
      }
      return actions;
    },
    profileOptions() {
      const definitions = Array.isArray(this.profileDefinitions) ? this.profileDefinitions : [];
      const settings = this.profileSettings || {};
      const options = [{label: 'Pass', value: null}];
      definitions.forEach((profile) => {
        if (settings?.[profile.key]?.enabled === false) {
          return;
        }
        options.push({
          label: `${profile.label} (${profile.container})`,
          value: profile.key,
        });
      });
      return options;
    },
    showNoEligibleStrmUsersWarning() {
      return this.generateStrmFiles && Number(this.eligibleStrmUserCount || 0) <= 0;
    },
  },
  methods: {
    show({
           contentType,
           category = null,
           availableCategories = [],
           existingCategoryNames = [],
           eligibleStrmUserCount = 0,
           profileDefinitions = [],
           profileSettings = {},
           nextSortOrder = 10,
         }) {
      this.isOpen = true;
      this.loading = false;
      this.saving = false;
      this.closeResult = null;
      this.contentType = contentType || 'movie';
      this.availableCategories = Array.isArray(availableCategories) ? structuredClone(availableCategories) : [];
      this.existingCategoryNames = Array.isArray(existingCategoryNames) ? structuredClone(existingCategoryNames) : [];
      this.eligibleStrmUserCount = Number(eligibleStrmUserCount || 0);
      this.profileDefinitions = Array.isArray(profileDefinitions) ? structuredClone(profileDefinitions) : [];
      this.profileSettings = structuredClone(profileSettings || {});
      this.nextCategoryKey = 1;

      if (category) {
        this.editingCategoryId = category.id;
        this.enabled = !!category.enabled;
        this.name = category.name || '';
        this.sortOrder = Number(category.sort_order || 0);
        this.profileId = category.profile_id || null;
        this.generateStrmFiles = !!category.generate_strm_files;
        this.strmBaseUrl = category.strm_base_url || '';
        this.selectedCategories = (category.categories || []).map((categoryItem) => this.withLocalKey({
          id: categoryItem.id,
          priority: Number(categoryItem.priority || 0),
          playlist_id: categoryItem.playlist_id,
          playlist_name: categoryItem.playlist_name,
          name: categoryItem.name,
          item_count: categoryItem.item_count || 0,
          strip_title_prefixes: (categoryItem.strip_title_prefixes || []).join('\n'),
          strip_title_suffixes: (categoryItem.strip_title_suffixes || []).join('\n'),
          config_open: false,
        }));
      } else {
        this.editingCategoryId = null;
        this.enabled = true;
        this.name = '';
        this.sortOrder = Number(nextSortOrder || 10);
        this.profileId = null;
        this.generateStrmFiles = false;
        this.strmBaseUrl = '';
        this.selectedCategories = [];
      }

      this.captureInitialState();
    },
    hide() {
      this.isOpen = false;
    },
    onDialogHide() {
      if (this.closeResult) {
        this.$emit('ok', this.closeResult);
      }
      this.$emit('hide');
      this.closeResult = null;
    },
    onDialogAction(action) {
      if (action.id === 'save') {
        this.save();
        return;
      }
      if (action.id === 'delete') {
        this.deleteCategory();
      }
    },
    onCloseRequest() {
      if (!this.isDirty) {
        this.hide();
        return;
      }
      this.$q.dialog({
        component: TicConfirmDialog,
        componentProps: {
          title: 'Discard Changes?',
          message: 'You have unsaved changes. Close this dialog and discard them?',
          icon: 'warning',
          iconColor: 'warning',
          confirmLabel: 'Discard',
          confirmIcon: 'delete',
          confirmColor: 'negative',
          cancelLabel: 'Keep Editing',
          persistent: true,
        },
      }).onOk(() => {
        this.hide();
      });
    },
    withLocalKey(category) {
      const currentKey = category.local_key || category.id;
      if (currentKey) {
        return {...category, local_key: String(currentKey), config_open: !!category.config_open};
      }
      const key = `local-${this.nextCategoryKey}`;
      this.nextCategoryKey += 1;
      return {...category, local_key: key, config_open: !!category.config_open};
    },
    captureInitialState() {
      this.initialStateSignature = this.currentStateSignature();
    },
    currentStateSignature() {
      return JSON.stringify({
        enabled: this.enabled,
        name: this.name,
        sortOrder: this.sortOrder,
        profileId: this.profileId,
        generateStrmFiles: this.generateStrmFiles,
        strmBaseUrl: this.strmBaseUrl,
        selectedCategories: (this.selectedCategories || []).map((category) => ({
          id: category.id,
          priority: Number(category.priority || 0),
          strip_title_prefixes: category.strip_title_prefixes || '',
          strip_title_suffixes: category.strip_title_suffixes || '',
        })),
      });
    },
    isCategoryConfigOpen(category) {
      return !!category?.config_open;
    },
    toggleCategoryConfig(category) {
      if (!category) {
        return;
      }
      category.config_open = !category.config_open;
    },
    onCategoryAction(action, category) {
      if (action.id !== 'remove') {
        return;
      }
      this.selectedCategories = (this.selectedCategories || []).filter((item) => item.local_key !== category.local_key);
    },
    splitStripRules(value) {
      return String(value || '').split(/[\n,]/).map((item) => item.trim()).filter(Boolean);
    },
    hasDuplicateCategoryName() {
      const targetName = String(this.name || '').trim().toLowerCase();
      if (!targetName) {
        return false;
      }
      return (this.existingCategoryNames || []).some((item) => {
        const itemId = Number(item?.id || 0);
        if (this.editingCategoryId && itemId === Number(this.editingCategoryId)) {
          return false;
        }
        return String(item?.name || '').trim().toLowerCase() === targetName;
      });
    },
    buildPayload() {
      const categoryIds = (this.selectedCategories || []).map((category) => category.id);
      return {
        content_type: this.contentType,
        enabled: !!this.enabled,
        name: this.name,
        sort_order: this.sortOrder,
        profile_id: this.profileId,
        generate_strm_files: !!this.generateStrmFiles,
        strm_base_url: String(this.strmBaseUrl || '').trim(),
        category_ids: categoryIds,
        category_configs: (this.selectedCategories || []).map((category) => ({
          category_id: category.id,
          priority: (this.selectedCategories || []).length
            - (this.selectedCategories || []).findIndex((item) => item.local_key === category.local_key),
          strip_title_prefixes: this.splitStripRules(category.strip_title_prefixes),
          strip_title_suffixes: this.splitStripRules(category.strip_title_suffixes),
        })),
      };
    },
    async save() {
      if (!String(this.name || '').trim()) {
        this.$q.notify({
          color: 'warning',
          message: 'Category name is required',
        });
        return;
      }
      if (this.hasDuplicateCategoryName()) {
        this.$q.notify({
          color: 'warning',
          message: 'A category with this name already exists in this section',
        });
        return;
      }
      if (this.generateStrmFiles && !String(this.strmBaseUrl || '').trim()) {
        this.$q.notify({
          color: 'warning',
          message: 'Library base URL is required when .strm generation is enabled',
        });
        return;
      }
      if (this.generateStrmFiles && !/^https?:\/\//i.test(String(this.strmBaseUrl || '').trim())) {
        this.$q.notify({
          color: 'warning',
          message: 'Library base URL must start with http:// or https://',
        });
        return;
      }
      if (!(this.selectedCategories || []).length) {
        this.$q.notify({
          color: 'warning',
          message: 'Add at least one upstream source category',
        });
        return;
      }
      this.saving = true;
      try {
        const payload = this.buildPayload();
        if (this.editingCategoryId) {
          await axios.put(`/tic-api/vod/groups/${this.editingCategoryId}`, payload);
        } else {
          await axios.post('/tic-api/vod/groups', payload);
        }
        this.closeResult = {
          id: this.editingCategoryId,
        };
        this.captureInitialState();
        this.hide();
      } catch {
        this.$q.notify({
          color: 'negative',
          message: 'Failed to save VOD category',
        });
      } finally {
        this.saving = false;
      }
    },
    deleteCategory() {
      if (!this.editingCategoryId) {
        return;
      }
      this.$q.dialog({
        component: TicConfirmDialog,
        componentProps: {
          title: `Delete VOD Category (${this.name || this.editingCategoryId})`,
          message: 'Delete this curated VOD category?',
          details: 'This action is final and cannot be undone.',
          icon: 'warning',
          iconColor: 'negative',
          confirmLabel: 'Delete',
          confirmIcon: 'delete',
          confirmColor: 'negative',
          cancelLabel: 'Keep Editing',
          persistent: true,
        },
      }).onOk(async () => {
        this.saving = true;
        try {
          await axios.delete(`/tic-api/vod/groups/${this.editingCategoryId}`);
          this.closeResult = {
            action: 'deleted',
            categoryId: this.editingCategoryId,
          };
          this.hide();
        } catch {
          this.$q.notify({
            color: 'negative',
            message: 'Failed to delete VOD category',
          });
        } finally {
          this.saving = false;
        }
      });
    },
    selectCategoriesFromList() {
      this.$q.dialog({
        component: VodSourceCategorySelectorDialog,
        componentProps: {
          categories: this.availableCategories,
          hideCategoryIds: (this.selectedCategories || []).map((category) => category.id),
        },
      }).onOk((payload) => {
        const selectedItems = payload?.selectedCategories || [];
        if (!selectedItems.length) {
          return;
        }
        const nextCategories = structuredClone(this.selectedCategories || []);
        selectedItems.forEach((category) => {
          const exists = nextCategories.some((item) => Number(item.id) === Number(category.id));
          if (exists) {
            return;
          }
          nextCategories.push(this.withLocalKey({
            id: category.id,
            priority: 0,
            playlist_id: category.playlist_id,
            playlist_name: category.playlist_name,
            name: category.name,
            item_count: category.item_count || 0,
            strip_title_prefixes: '',
            strip_title_suffixes: '',
            config_open: true,
          }));
        });
        this.selectedCategories = nextCategories;
      });
    },
  },
};
</script>

<style scoped>
.tic-form-layout > *:not(:last-child) {
  margin-bottom: 24px;
}

.source-config-panel > *:not(:last-child) {
  margin-bottom: 24px;
}

.channel-source-row {
  border-left: 4px solid transparent;
  transition: background-color 0.2s ease, border-color 0.2s ease, opacity 0.2s ease;
}

.source-settings-toggle {
  min-height: 24px;
  padding: 0 6px;
  font-size: 11px;
  white-space: nowrap;
}

.channel-source-actions {
  width: 190px;
  min-width: 190px;
  max-width: 190px;
  flex: 0 0 190px;
  display: flex;
  justify-content: flex-end;
}

.vod-category-row {
  align-items: flex-start;
}

@media (max-width: 599px) {
  .channel-source-actions {
    min-width: auto;
  }
}
</style>
