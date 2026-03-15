<template>
  <TicDialogWindow
    v-model="isOpen"
    title="Select from Source"
    width="1100px"
    close-tooltip="Close"
    :actions="dialogActions"
    @action="onDialogAction"
    @hide="onDialogHide"
  >
    <div class="vod-category-selector">
      <div class="selector-toolbar bg-card-head">
        <div class="row items-center q-col-gutter-sm">
          <div v-if="showActionsToggle" class="col-12 row justify-end">
            <TicActionButton
              :icon="actionsExpanded ? 'expand_less' : 'expand_more'"
              color="grey-8"
              :tooltip="actionsExpanded ? 'Hide filters' : 'Show filters'"
              @click="toggleActionsExpanded"
            />
          </div>

          <q-slide-transition>
            <div v-show="actionsExpanded" class="col-12">
              <TicListToolbar
                :search="{label: 'Search categories', placeholder: 'Search by category name', debounce: 300, clearable: true}"
                :search-value="searchValue"
                :filters="toolbarFilters"
                @update:search-value="searchValue = $event"
                @filter-change="onToolbarFilterChange"
              />

              <div class="selector-select-page-row">
                <q-checkbox
                  color="primary"
                  :model-value="allPageSelected"
                  label="Select page"
                  @update:model-value="toggleSelectPage"
                />
              </div>
            </div>
          </q-slide-transition>
        </div>
      </div>

      <q-slide-transition>
        <div v-show="(allPageSelected || selectedCount) && actionsExpanded" class="selector-selection-banner">
          <div class="selector-selection-banner__content">
            {{ selectedCount }} categories selected.
          </div>
          <div class="selector-selection-banner__actions">
            <TicButton label="Clear selection" color="secondary" dense @click="clearSelection" />
          </div>
        </div>
      </q-slide-transition>

      <div class="selector-scroll" @scroll.passive="handleScroll">
        <q-list v-if="filteredRows.length" separator bordered class="rounded-borders selector-category-list">
          <q-item v-for="row in filteredRows" :key="row.row_key" class="selector-category-item">
            <q-item-section avatar>
              <q-checkbox
                color="primary"
                :model-value="isRowSelected(row)"
                @update:model-value="(value) => toggleRowSelection(row, value)"
              />
            </q-item-section>

            <q-item-section>
              <q-item-label class="text-weight-medium">{{ row.name }}</q-item-label>
              <q-item-label caption>
                <span class="text-weight-medium">Source:</span> {{ row.playlist_name }}
                <span class="q-mx-xs">•</span>
                <span class="text-weight-medium">Items:</span> {{ row.item_count }}
              </q-item-label>
            </q-item-section>
          </q-item>
        </q-list>

        <div v-else class="selector-empty">
          <q-icon name="filter_b_and_w" size="2em" color="grey-6" />
          <div>No categories found for the current filters.</div>
        </div>
      </div>
    </div>
  </TicDialogWindow>
</template>

<script>
import {
  TicActionButton,
  TicButton,
  TicDialogWindow,
  TicListToolbar,
} from 'components/ui';

export default {
  name: 'VodSourceCategorySelectorDialog',
  components: {
    TicActionButton,
    TicButton,
    TicDialogWindow,
    TicListToolbar,
  },
  props: {
    categories: {
      type: Array,
      default: () => [],
    },
    hideCategoryIds: {
      type: Array,
      default: () => [],
    },
  },
  emits: ['ok', 'hide'],
  data() {
    return {
      isOpen: false,
      actionsExpanded: true,
      lastScrollTop: 0,
      searchValue: '',
      sourceFilter: null,
      selectedRowKeys: new Set(),
    };
  },
  computed: {
    dialogActions() {
      return [
        {
          id: 'select-categories',
          icon: 'check',
          label: 'Select',
          color: 'positive',
          disable: this.selectedCount < 1,
        },
      ];
    },
    showActionsToggle() {
      return this.$q.screen.lt.sm;
    },
    hiddenCategoryIdSet() {
      return new Set((this.hideCategoryIds || []).map((item) => Number(item)).filter((item) => !Number.isNaN(item)));
    },
    sourceOptions() {
      const options = [{label: 'All sources', value: null}];
      const seen = new Set();
      (this.categories || []).forEach((category) => {
        const playlistId = Number(category.playlist_id);
        if (Number.isNaN(playlistId) || seen.has(playlistId)) {
          return;
        }
        seen.add(playlistId);
        options.push({
          label: category.playlist_name || `Source ${playlistId}`,
          value: playlistId,
        });
      });
      return options;
    },
    toolbarFilters() {
      return [
        {
          key: 'source',
          modelValue: this.sourceFilter,
          label: 'Source',
          options: this.sourceOptions,
          optionLabel: 'label',
          optionValue: 'value',
          emitValue: true,
          mapOptions: true,
          clearable: false,
          dense: true,
          behavior: this.$q.screen.lt.md ? 'dialog' : 'menu',
        },
      ];
    },
    filteredRows() {
      const search = String(this.searchValue || '').trim().toLowerCase();
      return (this.categories || []).filter((category) => {
        const categoryId = Number(category.id);
        if (this.hiddenCategoryIdSet.has(categoryId)) {
          return false;
        }
        if (this.sourceFilter && Number(category.playlist_id) !== Number(this.sourceFilter)) {
          return false;
        }
        if (!search) {
          return true;
        }
        return [
          category.name,
          category.playlist_name,
        ].some((value) => String(value || '').toLowerCase().includes(search));
      }).map((category) => ({
        ...category,
        row_key: String(category.id),
      }));
    },
    selectableRowsByKey() {
      return (this.categories || []).reduce((acc, category) => {
        if (this.hiddenCategoryIdSet.has(Number(category.id))) {
          return acc;
        }
        acc[String(category.id)] = {
          ...category,
          row_key: String(category.id),
        };
        return acc;
      }, {});
    },
    allPageSelected() {
      if (!this.filteredRows.length) {
        return false;
      }
      return this.filteredRows.every((row) => this.selectedRowKeys.has(row.row_key));
    },
    selectedCount() {
      return this.selectedRowKeys.size;
    },
  },
  methods: {
    show() {
      this.isOpen = true;
      this.actionsExpanded = true;
      this.lastScrollTop = 0;
      this.searchValue = '';
      this.sourceFilter = null;
      this.clearSelection();
    },
    hide() {
      this.isOpen = false;
    },
    onDialogHide() {
      this.$emit('hide');
    },
    onDialogAction(action) {
      if (action?.id !== 'select-categories') {
        return;
      }
      const selectedCategories = Array.from(this.selectedRowKeys).
        map((rowKey) => this.selectableRowsByKey[rowKey]).
        filter(Boolean).
        map((row) => ({
          id: row.id,
          playlist_id: row.playlist_id,
          playlist_name: row.playlist_name,
          name: row.name,
          item_count: row.item_count,
        }));
      this.$emit('ok', {selectedCategories});
      this.hide();
    },
    onToolbarFilterChange({key, value}) {
      if (key !== 'source') {
        return;
      }
      this.sourceFilter = value || null;
      this.clearSelection();
    },
    toggleActionsExpanded() {
      this.actionsExpanded = !this.actionsExpanded;
    },
    handleScroll(event) {
      if (!this.showActionsToggle) {
        return;
      }
      const nextScrollTop = event?.target?.scrollTop || 0;
      if (nextScrollTop <= 8) {
        this.actionsExpanded = true;
        this.lastScrollTop = nextScrollTop;
        return;
      }
      if (nextScrollTop > this.lastScrollTop + 10) {
        this.actionsExpanded = false;
      } else if (nextScrollTop < this.lastScrollTop - 20) {
        this.actionsExpanded = true;
      }
      this.lastScrollTop = nextScrollTop;
    },
    isRowSelected(row) {
      return this.selectedRowKeys.has(row.row_key);
    },
    toggleRowSelection(row, value) {
      const nextSelected = new Set(this.selectedRowKeys);
      if (value) {
        nextSelected.add(row.row_key);
      } else {
        nextSelected.delete(row.row_key);
      }
      this.selectedRowKeys = nextSelected;
    },
    toggleSelectPage(value) {
      const nextSelected = new Set(this.selectedRowKeys);
      for (const row of this.filteredRows) {
        if (value) {
          nextSelected.add(row.row_key);
        } else {
          nextSelected.delete(row.row_key);
        }
      }
      this.selectedRowKeys = nextSelected;
    },
    clearSelection() {
      this.selectedRowKeys = new Set();
    },
  },
};
</script>

<style scoped>
.vod-category-selector {
  display: flex;
  flex-direction: column;
  height: 100%;
}

.selector-toolbar {
  position: sticky;
  top: 0;
  z-index: 3;
  padding: 8px 12px;
  border-bottom: 1px solid var(--q-separator-color);
}

.selector-selection-banner {
  display: flex;
  flex-wrap: wrap;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
  padding: 8px 12px;
  border-bottom: 1px solid var(--q-separator-color);
  background: color-mix(in srgb, var(--q-primary), transparent 92%);
}

.selector-selection-banner__content {
  font-size: 0.85rem;
}

.selector-selection-banner__actions {
  display: flex;
  align-items: center;
  gap: 8px;
}

.selector-scroll {
  flex: 1;
  overflow-y: auto;
  padding: 8px 12px 12px;
}

.selector-select-page-row {
  margin-left: 17px;
}

.selector-empty {
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  gap: 8px;
  min-height: 220px;
  color: var(--q-grey-7);
}

.selector-category-list {
  margin-bottom: 12px;
}

.selector-category-item {
  padding-top: 4px;
  padding-bottom: 4px;
}
</style>
