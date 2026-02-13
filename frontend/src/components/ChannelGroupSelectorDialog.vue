<template>
  <TicDialogWindow
    ref="channelGroupSelectorDialogRef"
    v-model="isOpen"
    title="Select Groups From Source"
    width="1100px"
    close-tooltip="Close"
    :actions="dialogActions"
    @action="onDialogAction"
    @hide="onDialogHide"
  >
    <div class="channel-group-selector">
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
              <div class="row q-col-gutter-sm items-center">
                <div :class="$q.screen.lt.sm ? 'col-12' : 'col-6 col-md-4'">
                  <TicSearchInput
                    v-model="searchValue"
                    label="Search groups"
                    placeholder="Search by group name"
                    :debounce="300"
                    :clearable="true"
                  />
                </div>

                <div class="col-6 col-md-3">
                  <TicSelectInput
                    class="selector-filter-field"
                    v-model="appliedFilters.playlistId"
                    label="Source"
                    :options="playlistOptions"
                    option-label="label"
                    option-value="value"
                    :emit-value="true"
                    :map-options="true"
                    :clearable="false"
                    :dense="true"
                    :behavior="$q.screen.lt.md ? 'dialog' : 'menu'"
                    @update:model-value="onInlineSourceFilterChange"
                  />
                </div>

                <div :class="$q.screen.lt.sm ? 'col-6' : 'col-auto'">
                  <TicButton
                    :label="sortButtonLabel"
                    icon="sort"
                    color="secondary"
                    :class="$q.screen.lt.sm ? 'full-width' : ''"
                    @click="openSortDialog"
                  />
                </div>

                <div class="col-12">
                  <div class="selector-select-page-row">
                    <q-checkbox
                      color="primary"
                      :model-value="allPageSelected"
                      label="Select page"
                      @update:model-value="toggleSelectPage"
                    />
                  </div>
                </div>
              </div>
            </div>
          </q-slide-transition>
        </div>
      </div>

      <q-slide-transition>
        <div
          v-show="(allPageSelected || selectAllMatching) && actionsExpanded"
          class="selector-selection-banner"
        >
          <div class="selector-selection-banner__content">
            <template v-if="showSelectAllMatchingPrompt">
              {{ selectedCount }} groups selected on this page.
            </template>
            <template v-else-if="selectAllMatching">
              All {{ selectedCount }} matching groups selected.
            </template>
          </div>
          <div class="selector-selection-banner__actions">
            <TicButton
              v-if="showSelectAllMatchingPrompt"
              :label="`Select all ${totalMatchingCount} matching groups`"
              color="secondary"
              dense
              @click="selectAllMatchingResults"
            />
            <TicButton
              v-else
              label="Clear selection"
              color="secondary"
              dense
              @click="clearSelection"
            />
          </div>
        </div>
      </q-slide-transition>

      <div
        id="channel-group-selector-scroll"
        class="selector-scroll"
        @scroll.passive="handleScroll"
      >
        <q-infinite-scroll
          :disable="allLoaded || loadingMore"
          :offset="160"
          scroll-target="#channel-group-selector-scroll"
          @load="loadMore"
        >
          <q-list v-if="rows.length" separator bordered class="rounded-borders selector-group-list">
            <q-item
              v-for="row in rows"
              :key="row.row_key"
              class="selector-group-item"
            >
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
                  <span class="text-weight-medium">Channels:</span> {{ row.channel_count }}
                </q-item-label>
              </q-item-section>
            </q-item>
          </q-list>

          <template #loading>
            <div class="row flex-center q-my-md">
              <q-spinner-dots size="30px" color="primary" />
            </div>
          </template>
        </q-infinite-scroll>

        <div v-if="!loadingInitial && !rows.length" class="selector-empty">
          <q-icon name="filter_b_and_w" size="2em" color="grey-6" />
          <div>No groups found for the current filters.</div>
        </div>

        <q-inner-loading :showing="loadingInitial">
          <q-spinner-dots size="42px" color="primary" />
        </q-inner-loading>
      </div>
    </div>

    <TicDialogPopup
      v-model="sortDialogOpen"
      title="Sort Groups"
      width="560px"
      max-width="95vw"
    >
      <div class="tic-form-layout">
        <TicSelectInput
          v-model="sortDraft.sortBy"
          label="Sort By"
          description="Select which field to sort by."
          :options="sortOptions"
          option-label="label"
          option-value="value"
          :emit-value="true"
          :map-options="true"
          :clearable="false"
          :behavior="$q.screen.lt.md ? 'dialog' : 'menu'"
        />
        <TicSelectInput
          v-model="sortDraft.sortDirection"
          label="Direction"
          description="Ascending or descending order."
          :options="sortDirectionOptions"
          option-label="label"
          option-value="value"
          :emit-value="true"
          :map-options="true"
          :clearable="false"
          :behavior="$q.screen.lt.md ? 'dialog' : 'menu'"
        />
      </div>
      <template #actions>
        <TicButton label="Clear" variant="flat" color="grey-7" @click="clearSortDraft" />
        <TicButton label="Apply" icon="check" color="positive" @click="applySortDraft" />
      </template>
    </TicDialogPopup>
  </TicDialogWindow>
</template>

<script>
import axios from 'axios';
import TicActionButton from 'components/ui/buttons/TicActionButton.vue';
import TicButton from 'components/ui/buttons/TicButton.vue';
import TicDialogPopup from 'components/ui/dialogs/TicDialogPopup.vue';
import TicDialogWindow from 'components/ui/dialogs/TicDialogWindow.vue';
import TicSearchInput from 'components/ui/inputs/TicSearchInput.vue';
import TicSelectInput from 'components/ui/inputs/TicSelectInput.vue';

const GROUP_PAGE_SIZE = 100;

export default {
  name: 'ChannelGroupSelectorDialog',
  components: {
    TicActionButton,
    TicButton,
    TicDialogPopup,
    TicDialogWindow,
    TicSearchInput,
    TicSelectInput,
  },
  emits: ['ok', 'hide'],
  data() {
    return {
      isOpen: false,
      rows: [],
      rowsByKey: {},
      loadingInitial: false,
      loadingMore: false,
      loadOffset: 0,
      totalMatchingCount: 0,
      actionsExpanded: true,
      lastScrollTop: 0,

      searchValue: '',

      playlistOptions: [{label: 'All', value: null}],
      appliedFilters: {
        playlistId: null,
      },

      sortDialogOpen: false,
      sortDraft: {
        sortBy: 'name',
        sortDirection: 'asc',
      },
      appliedSort: {
        sortBy: 'name',
        sortDirection: 'asc',
      },

      selectedRowKeys: new Set(),
      excludedRowKeys: new Set(),
      selectAllMatching: false,
    };
  },
  computed: {
    dialogActions() {
      return [
        {
          id: 'select-groups',
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
    allLoaded() {
      if (this.totalMatchingCount < 1) {
        return false;
      }
      return this.loadOffset >= this.totalMatchingCount;
    },
    allPageSelected() {
      if (!this.rows.length) {
        return false;
      }
      return this.rows.every((row) => this.isRowSelected(row));
    },
    selectedCount() {
      if (this.selectAllMatching) {
        const selected = this.totalMatchingCount - this.excludedRowKeys.size;
        return selected > 0 ? selected : 0;
      }
      return this.selectedRowKeys.size;
    },
    showSelectAllMatchingPrompt() {
      return this.allPageSelected && !this.selectAllMatching && this.totalMatchingCount > this.rows.length;
    },
    sortButtonLabel() {
      const field = this.sortOptions.find((option) => option.value === this.appliedSort.sortBy);
      const directionLabel = this.appliedSort.sortDirection === 'desc' ? 'Desc' : 'Asc';
      return `Sort: ${field?.label || 'Name'} (${directionLabel})`;
    },
    sortOptions() {
      return [
        {label: 'Group Name', value: 'name'},
        {label: 'Channel Count', value: 'channel_count'},
      ];
    },
    sortDirectionOptions() {
      return [
        {label: 'Ascending', value: 'asc'},
        {label: 'Descending', value: 'desc'},
      ];
    },
  },
  watch: {
    searchValue() {
      this.resetAndReload();
    },
  },
  methods: {
    show() {
      this.isOpen = true;
      this.initDialogState();
      this.fetchPlaylists();
    },
    hide() {
      this.isOpen = false;
    },
    onDialogHide() {
      this.$emit('hide');
    },
    onDialogAction(action) {
      if (action?.id === 'select-groups') {
        this.confirmSelection();
      }
    },
    initDialogState() {
      this.actionsExpanded = true;
      this.lastScrollTop = 0;
      this.searchValue = '';
      this.sortDialogOpen = false;
      this.appliedFilters = {
        playlistId: null,
      };
      this.sortDraft = {
        sortBy: 'name',
        sortDirection: 'asc',
      };
      this.appliedSort = {
        sortBy: 'name',
        sortDirection: 'asc',
      };
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
    async loadMore(index, done) {
      await this.loadNextChunk();
      done();
    },
    async onInlineSourceFilterChange() {
      this.clearSelection();
      await this.resetAndReload();
    },
    async resetAndReload() {
      this.rows = [];
      this.rowsByKey = {};
      this.loadOffset = 0;
      this.totalMatchingCount = 0;
      this.loadingInitial = true;
      await this.loadNextChunk();
      this.loadingInitial = false;
    },
    async loadNextChunk() {
      if (this.loadingMore || this.allLoaded || !this.appliedFilters.playlistId) {
        return;
      }

      this.loadingMore = true;
      try {
        const response = await this.fetchGroupsPage(this.loadOffset, GROUP_PAGE_SIZE);
        const groups = response.groups || [];
        this.totalMatchingCount = response.total || 0;
        this.loadOffset += groups.length;

        const playlist = this.playlistOptions.find((item) => item.value === this.appliedFilters.playlistId);
        const playlistName = playlist?.label || 'Source';
        const mapped = groups.map((group) => ({
          name: group.name,
          channel_count: group.channel_count || 0,
          playlist_id: this.appliedFilters.playlistId,
          playlist_name: playlistName,
          row_key: `${this.appliedFilters.playlistId}-${group.name}`,
        })).filter((group) => !this.rowsByKey[group.row_key]);

        if (mapped.length) {
          const nextRows = [...this.rows, ...mapped];
          this.rows = nextRows;
          const byKey = {...this.rowsByKey};
          for (const row of mapped) {
            byKey[row.row_key] = row;
          }
          this.rowsByKey = byKey;
        }
      } catch {
        this.$q.notify({
          color: 'negative',
          message: 'Failed to load groups for source',
        });
      } finally {
        this.loadingMore = false;
      }
    },
    async fetchGroupsPage(start, length) {
      const response = await axios({
        method: 'POST',
        url: '/tic-api/playlists/groups',
        data: {
          start,
          length,
          search_value: this.searchValue,
          order_by: this.appliedSort.sortBy,
          order_direction: this.appliedSort.sortDirection,
          playlist_id: this.appliedFilters.playlistId,
        },
      });
      return {
        groups: response.data?.data?.groups || [],
        total: response.data?.data?.total || 0,
      };
    },
    isRowSelected(row) {
      if (this.selectAllMatching) {
        return !this.excludedRowKeys.has(row.row_key);
      }
      return this.selectedRowKeys.has(row.row_key);
    },
    toggleRowSelection(row, value) {
      if (this.selectAllMatching) {
        const nextExcluded = new Set(this.excludedRowKeys);
        if (value) {
          nextExcluded.delete(row.row_key);
        } else {
          nextExcluded.add(row.row_key);
        }
        this.excludedRowKeys = nextExcluded;
        return;
      }

      const nextSelected = new Set(this.selectedRowKeys);
      if (value) {
        nextSelected.add(row.row_key);
      } else {
        nextSelected.delete(row.row_key);
      }
      this.selectedRowKeys = nextSelected;
    },
    toggleSelectPage(value) {
      if (this.selectAllMatching) {
        const nextExcluded = new Set(this.excludedRowKeys);
        for (const row of this.rows) {
          if (value) {
            nextExcluded.delete(row.row_key);
          } else {
            nextExcluded.add(row.row_key);
          }
        }
        this.excludedRowKeys = nextExcluded;
        return;
      }

      const nextSelected = new Set(this.selectedRowKeys);
      for (const row of this.rows) {
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
      this.excludedRowKeys = new Set();
      this.selectAllMatching = false;
    },
    selectAllMatchingResults() {
      this.selectAllMatching = true;
      this.selectedRowKeys = new Set();
      this.excludedRowKeys = new Set();
    },
    async confirmSelection() {
      if (this.selectedCount < 1) {
        return;
      }

      this.$q.loading.show({
        message: 'Preparing selected groups...',
      });

      try {
        let selectedGroups = [];
        if (this.selectAllMatching) {
          selectedGroups = await this.fetchAllMatchingSelectedGroups();
        } else {
          selectedGroups = Array.from(this.selectedRowKeys).map((rowKey) => this.rowsByKey[rowKey]).filter(Boolean);
        }

        this.$emit('ok', {
          selectedGroups: selectedGroups.map((group) => ({
            group_name: group.name,
            playlist_id: group.playlist_id,
            playlist_name: group.playlist_name,
            channel_count: group.channel_count,
          })),
        });
        this.hide();
      } catch {
        this.$q.notify({
          color: 'negative',
          message: 'Failed to process selected groups',
        });
      } finally {
        this.$q.loading.hide();
      }
    },
    async fetchAllMatchingSelectedGroups() {
      const selected = [];
      const excluded = this.excludedRowKeys;
      let start = 0;
      let total = 0;
      const playlist = this.playlistOptions.find((item) => item.value === this.appliedFilters.playlistId);
      const playlistName = playlist?.label || 'Source';

      do {
        const response = await this.fetchGroupsPage(start, GROUP_PAGE_SIZE);
        total = response.total || 0;
        const mapped = (response.groups || []).map((group) => ({
          name: group.name,
          channel_count: group.channel_count || 0,
          playlist_id: this.appliedFilters.playlistId,
          playlist_name: playlistName,
          row_key: `${this.appliedFilters.playlistId}-${group.name}`,
        })).filter((group) => !excluded.has(group.row_key));
        selected.push(...mapped);
        start += response.groups?.length || 0;
      } while (start < total);

      return selected;
    },
    openSortDialog() {
      this.sortDraft = {
        sortBy: this.appliedSort.sortBy,
        sortDirection: this.appliedSort.sortDirection,
      };
      this.sortDialogOpen = true;
    },
    clearSortDraft() {
      this.sortDraft = {
        sortBy: 'name',
        sortDirection: 'asc',
      };
    },
    applySortDraft() {
      this.appliedSort = {
        sortBy: this.sortDraft.sortBy,
        sortDirection: this.sortDraft.sortDirection,
      };
      this.sortDialogOpen = false;
      this.resetAndReload();
    },
    async fetchPlaylists() {
      try {
        const response = await axios({
          method: 'GET',
          url: '/tic-api/playlists/get',
        });
        const options = (response.data?.data || []).map((playlist) => ({
          label: playlist.name,
          value: playlist.id,
        }));
        this.playlistOptions = options;
        this.appliedFilters.playlistId = options[0]?.value || null;
        await this.resetAndReload();
      } catch {
        this.playlistOptions = [{label: 'All', value: null}];
        this.$q.notify({
          color: 'negative',
          message: 'Failed to fetch sources',
        });
      }
    },
  },
};
</script>

<style scoped>
.channel-group-selector {
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

.selector-toolbar :deep(.selector-filter-field .tic-select-input-field) {
  padding-bottom: 0 !important;
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

.selector-group-list {
  margin-bottom: 12px;
}

.selector-group-item {
  padding-top: 4px;
  padding-bottom: 4px;
}

@media (max-width: 599px) {
  .selector-toolbar {
    padding: 8px;
  }

  .selector-scroll {
    padding: 8px;
  }

  .selector-select-page-row {
    margin-left: 8px;
  }
}
</style>
