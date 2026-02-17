<template>
  <TicDialogWindow
    ref="channelStreamSelectorDialogRef"
    v-model="isOpen"
    title="Select from Source"
    width="1100px"
    close-tooltip="Close"
    :actions="dialogActions"
    @action="onDialogAction"
    @hide="onDialogHide"
  >
    <div class="channel-stream-selector">
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
              <div class="row q-col-gutter-sm items-end">
                <div :class="$q.screen.lt.sm ? 'col-12' : 'col-6 col-md-4'">
                  <TicSearchInput
                    class="section-toolbar-field"
                    v-model="searchValue"
                    label="Search streams"
                    placeholder="Search by stream name"
                    :debounce="300"
                    :clearable="true"
                  />
                </div>

                <template v-if="$q.screen.gt.sm">
                  <div class="col-6 col-md-3">
                    <TicSelectInput
                      class="section-toolbar-field"
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
                      @update:model-value="onInlinePlaylistFilterChange"
                    />
                  </div>

                  <div class="col-6 col-md-3">
                    <TicSelectInput
                      class="section-toolbar-field"
                      v-model="appliedFilters.groupTitle"
                      label="Group"
                      :options="inlineGroupOptions"
                      option-label="label"
                      option-value="value"
                      :emit-value="true"
                      :map-options="true"
                      :clearable="false"
                      :dense="true"
                      :disable="!appliedFilters.playlistId"
                      :behavior="$q.screen.lt.md ? 'dialog' : 'menu'"
                      @update:model-value="onInlineGroupFilterChange"
                    />
                  </div>
                </template>

                <div v-else :class="$q.screen.lt.sm ? 'col-6 section-toolbar-split-left' : 'col-auto'">
                  <TicButton
                    label="Filters"
                    icon="filter_list"
                    color="secondary"
                    :dense="$q.screen.lt.sm"
                    class="section-toolbar-btn section-toolbar-btn--compact"
                    @click="openFilterDialog"
                  />
                </div>

                <div :class="$q.screen.lt.sm ? 'col-6 section-toolbar-split-right' : 'col-auto'">
                  <TicButton
                    :label="$q.screen.lt.sm ? 'Sort' : sortButtonLabel"
                    icon="sort"
                    color="secondary"
                    :dense="$q.screen.lt.sm"
                    class="section-toolbar-btn section-toolbar-btn--compact"
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
        <div v-show="(allPageSelected || selectAllMatching) && actionsExpanded" class="selector-selection-banner">
          <div class="selector-selection-banner__content">
            <template v-if="showSelectAllMatchingPrompt"> {{ selectedCount }} streams selected on this page.</template>
            <template v-else-if="selectAllMatching"> All {{ selectedCount }} matching streams selected.</template>
          </div>
          <div class="selector-selection-banner__actions">
            <TicButton
              v-if="showSelectAllMatchingPrompt"
              :label="`Select all ${totalMatchingCount} matching streams`"
              color="secondary"
              dense
              @click="selectAllMatchingResults"
            />
            <TicButton v-else label="Clear selection" color="secondary" dense @click="clearSelection" />
          </div>
        </div>
      </q-slide-transition>

      <div
        id="channel-stream-selector-scroll"
        ref="scrollTargetRef"
        class="selector-scroll"
        @scroll.passive="handleScroll"
      >
        <q-infinite-scroll
          ref="infiniteScrollRef"
          :disable="allLoaded || loadingMore"
          :offset="160"
          scroll-target="#channel-stream-selector-scroll"
          @load="loadMore"
        >
          <q-list v-if="rows.length" separator bordered class="rounded-borders selector-stream-list">
            <q-item v-for="row in rows" :key="row.row_key" class="selector-stream-item">
              <q-item-section avatar>
                <q-checkbox
                  color="primary"
                  :model-value="isRowSelected(row)"
                  @update:model-value="(value) => toggleRowSelection(row, value)"
                />
              </q-item-section>

              <q-item-section avatar>
                <div class="stream-logo-wrap">
                  <img v-if="row.tvg_logo" :src="row.tvg_logo" alt="logo" class="stream-logo-img" />
                  <q-icon v-else name="play_arrow" size="18px" color="grey-6" />
                </div>
              </q-item-section>

              <q-item-section>
                <q-item-label class="text-weight-medium">{{ row.name }}</q-item-label>
                <q-item-label caption>
                  <span class="text-weight-medium">Source:</span> {{ row.playlist_name }}
                  <span class="q-mx-xs">•</span>
                  <span class="text-weight-medium">Group:</span> {{ row.group_title || 'Uncategorized' }}
                </q-item-label>
              </q-item-section>

              <q-item-section side top>
                <div class="row items-center q-gutter-xs">
                  <TicActionButton
                    icon="play_arrow"
                    color="primary"
                    tooltip="Preview Stream"
                    @click="previewStream(row)"
                  />
                  <TicActionButton
                    icon="content_copy"
                    color="grey-8"
                    tooltip="Copy Stream URL"
                    @click="copyStreamUrl(row)"
                  />
                </div>
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
          <div>No streams found for the current filters.</div>
        </div>

        <q-inner-loading :showing="loadingInitial">
          <q-spinner-dots size="42px" color="primary" />
        </q-inner-loading>
      </div>
    </div>

    <TicDialogPopup v-model="filterDialogOpen" title="Filter Streams" width="560px" max-width="95vw">
      <div class="tic-form-layout">
        <TicSelectInput
          v-model="filterDraft.playlistId"
          label="Source"
          description="Filter streams by source playlist."
          :options="playlistOptions"
          option-label="label"
          option-value="value"
          :emit-value="true"
          :map-options="true"
          :clearable="false"
          :behavior="$q.screen.lt.md ? 'dialog' : 'menu'"
        />
        <TicSelectInput
          v-model="filterDraft.groupTitle"
          label="Group"
          description="Filter streams by group title."
          :options="draftGroupOptions"
          option-label="label"
          option-value="value"
          :emit-value="true"
          :map-options="true"
          :clearable="false"
          :disable="!filterDraft.playlistId"
          :behavior="$q.screen.lt.md ? 'dialog' : 'menu'"
        />
      </div>
      <template #actions>
        <TicButton label="Clear" variant="flat" color="grey-7" @click="clearFilterDraft" />
        <TicButton label="Apply" icon="check" color="positive" @click="applyFilterDraft" />
      </template>
    </TicDialogPopup>

    <TicDialogPopup v-model="sortDialogOpen" title="Sort Streams" width="560px" max-width="95vw">
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
import {copyToClipboard} from 'quasar';
import {useVideoStore} from 'stores/video';
import TicActionButton from 'components/ui/buttons/TicActionButton.vue';
import TicButton from 'components/ui/buttons/TicButton.vue';
import TicDialogPopup from 'components/ui/dialogs/TicDialogPopup.vue';
import TicDialogWindow from 'components/ui/dialogs/TicDialogWindow.vue';
import TicSearchInput from 'components/ui/inputs/TicSearchInput.vue';
import TicSelectInput from 'components/ui/inputs/TicSelectInput.vue';

const STREAM_PAGE_SIZE = 100;

export default {
  name: 'ChannelStreamSelectorDialog',
  components: {
    TicActionButton,
    TicButton,
    TicDialogPopup,
    TicDialogWindow,
    TicSearchInput,
    TicSelectInput,
  },
  props: {
    hideStreams: {
      type: Array,
      default: () => [],
    },
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

      filterDialogOpen: false,
      playlistOptions: [{label: 'All', value: null}],
      groupOptionsAll: [{label: 'All', value: null}],
      draftGroupOptions: [{label: 'All', value: null}],
      inlineGroupOptions: [{label: 'All', value: null}],
      filterDraft: {
        playlistId: null,
        groupTitle: null,
      },
      appliedFilters: {
        playlistId: null,
        groupTitle: null,
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
    authUser() {
      return this.$pinia?.state?.value?.auth?.user || null;
    },
    hideStreamUrlSet() {
      return new Set((this.hideStreams || []).map((item) => String(item || '').trim()).filter(Boolean));
    },
    dialogActions() {
      return [
        {
          id: 'select-streams',
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
        {label: 'Name', value: 'name'},
        {label: 'Source', value: 'playlist_name'},
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
    'filterDraft.playlistId'(playlistId) {
      this.filterDraft.groupTitle = null;
      this.fetchGroupsForDraft(playlistId);
    },
    'appliedFilters.playlistId'(playlistId) {
      this.appliedFilters.groupTitle = null;
      this.fetchInlineGroups(playlistId);
    },
  },
  methods: {
    show() {
      this.isOpen = true;
      this.initDialogState();
      this.fetchPlaylists();
      this.fetchGroupsForDraft(this.filterDraft.playlistId);
      this.resetAndReload();
    },
    hide() {
      this.isOpen = false;
    },
    onDialogHide() {
      this.$emit('hide');
    },
    onDialogAction(action) {
      if (action?.id === 'select-streams') {
        this.confirmSelection();
      }
    },
    initDialogState() {
      this.actionsExpanded = true;
      this.lastScrollTop = 0;

      this.filterDialogOpen = false;
      this.sortDialogOpen = false;

      this.filterDraft = {
        playlistId: null,
        groupTitle: null,
      };
      this.appliedFilters = {
        playlistId: null,
        groupTitle: null,
      };

      this.sortDraft = {
        sortBy: 'name',
        sortDirection: 'asc',
      };
      this.appliedSort = {
        sortBy: 'name',
        sortDirection: 'asc',
      };

      this.searchValue = '';
      this.clearSelection();
    },
    onInlinePlaylistFilterChange() {
      this.clearSelection();
      this.resetAndReload();
    },
    onInlineGroupFilterChange() {
      this.clearSelection();
      this.resetAndReload();
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
      if (this.loadingMore || this.allLoaded) {
        return;
      }

      this.loadingMore = true;
      try {
        let appendedRows = 0;
        while (appendedRows === 0 && (this.loadOffset < this.totalMatchingCount || this.totalMatchingCount === 0)) {
          const response = await this.fetchStreamsPage(this.loadOffset, STREAM_PAGE_SIZE);
          const streams = response.streams || [];
          this.totalMatchingCount = response.recordsFiltered || 0;
          this.loadOffset += streams.length;

          const mapped = streams.filter((stream) => {
            return !this.hideStreamUrlSet.has(String(stream?.url || '').trim());
          }).map((stream) => this.mapStream(stream)).filter((stream) => !this.rowsByKey[stream.row_key]);

          if (mapped.length) {
            const nextRows = [...this.rows, ...mapped];
            this.rows = nextRows;
            const byKey = {...this.rowsByKey};
            for (const row of mapped) {
              byKey[row.row_key] = row;
            }
            this.rowsByKey = byKey;
            appendedRows = mapped.length;
          }

          if (!streams.length || this.loadOffset >= this.totalMatchingCount) {
            break;
          }
        }
      } catch {
        this.$q.notify({
          color: 'negative',
          message: 'Failed to load stream list',
        });
      } finally {
        this.loadingMore = false;
      }
    },
    async fetchStreamsPage(start, length) {
      const response = await axios({
        method: 'POST',
        url: '/tic-api/playlists/streams',
        data: {
          start,
          length,
          search_value: this.searchValue,
          order_by: this.appliedSort.sortBy,
          order_direction: this.appliedSort.sortDirection,
          playlist_id: this.appliedFilters.playlistId || null,
          group_title: this.appliedFilters.groupTitle || null,
        },
      });
      return {
        streams: response.data?.data?.streams || [],
        recordsFiltered: response.data?.data?.records_filtered || 0,
      };
    },
    mapStream(stream) {
      const rowKey = this.rowKeyForStream(stream);
      return {
        id: stream.id,
        channel_id: stream.channel_id,
        name: stream.name,
        url: stream.url,
        playlist_id: stream.playlist_id,
        playlist_name: stream.playlist_name,
        tvg_logo: stream.tvg_logo,
        group_title: stream.group_title,
        row_key: rowKey,
      };
    },
    rowKeyForStream(stream) {
      return `${stream.playlist_id}-${stream.id}`;
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
        message: 'Preparing selected streams...',
      });

      try {
        let selectedStreams = [];
        if (this.selectAllMatching) {
          selectedStreams = await this.fetchAllMatchingSelectedStreams();
        } else {
          selectedStreams = Array.from(this.selectedRowKeys).map((rowKey) => this.rowsByKey[rowKey]).filter(Boolean);
        }

        if (!selectedStreams.length) {
          this.$q.notify({
            color: 'warning',
            message: 'No streams selected',
          });
          return;
        }

        const returnItems = selectedStreams.map((row) => ({
          id: row.id,
          playlist_id: row.playlist_id,
          playlist_name: row.playlist_name,
          channel_id: row.channel_id,
          stream_name: row.name,
          stream_url: row.url,
        }));

        this.$emit('ok', {selectedStreams: returnItems});
        this.hide();
      } catch {
        this.$q.notify({
          color: 'negative',
          message: 'Failed to process selected streams',
        });
      } finally {
        this.$q.loading.hide();
      }
    },
    async fetchAllMatchingSelectedStreams() {
      const selectedRows = [];
      const excluded = this.excludedRowKeys;
      let start = 0;
      let total = 0;

      do {
        const response = await this.fetchStreamsPage(start, STREAM_PAGE_SIZE);
        total = response.recordsFiltered || 0;
        const mapped = (response.streams || []).filter((stream) => {
          return !this.hideStreamUrlSet.has(String(stream?.url || '').trim());
        }).map((stream) => this.mapStream(stream)).filter((row) => !excluded.has(row.row_key));
        selectedRows.push(...mapped);
        start += response.streams?.length || 0;
      } while (start < total);

      return selectedRows;
    },
    openFilterDialog() {
      this.filterDraft = {
        playlistId: this.appliedFilters.playlistId,
        groupTitle: this.appliedFilters.groupTitle,
      };
      this.fetchGroupsForDraft(this.filterDraft.playlistId);
      this.filterDialogOpen = true;
    },
    clearFilterDraft() {
      this.filterDraft = {
        playlistId: null,
        groupTitle: null,
      };
      this.fetchGroupsForDraft(null);
    },
    applyFilterDraft() {
      this.appliedFilters = {
        playlistId: this.filterDraft.playlistId,
        groupTitle: this.filterDraft.groupTitle,
      };
      this.filterDialogOpen = false;
      this.clearSelection();
      this.resetAndReload();
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
        const options = [{label: 'All', value: null}];
        for (const playlist of response.data.data || []) {
          options.push({
            label: playlist.name,
            value: playlist.id,
          });
        }
        this.playlistOptions = options;
      } catch {
        this.playlistOptions = [{label: 'All', value: null}];
      }
    },
    async fetchGroupsForDraft(playlistId) {
      if (!playlistId) {
        this.groupOptionsAll = [{label: 'All', value: null}];
        this.draftGroupOptions = this.groupOptionsAll;
        return;
      }

      try {
        const response = await axios({
          method: 'POST',
          url: '/tic-api/playlists/groups',
          data: {
            playlist_id: playlistId,
            start: 0,
            length: 0,
            search_value: '',
            order_by: 'name',
            order_direction: 'asc',
          },
        });
        const groups = response.data.data?.groups || response.data.data || [];
        const options = [{label: 'All', value: null}];
        for (const group of groups) {
          options.push({
            label: group.name,
            value: group.name,
          });
        }
        this.groupOptionsAll = options;
        this.draftGroupOptions = options;
      } catch {
        this.groupOptionsAll = [{label: 'All', value: null}];
        this.draftGroupOptions = this.groupOptionsAll;
      }
    },
    async fetchInlineGroups(playlistId) {
      if (!playlistId) {
        this.inlineGroupOptions = [{label: 'All', value: null}];
        return;
      }

      try {
        const response = await axios({
          method: 'POST',
          url: '/tic-api/playlists/groups',
          data: {
            playlist_id: playlistId,
            start: 0,
            length: 0,
            search_value: '',
            order_by: 'name',
            order_direction: 'asc',
          },
        });
        const groups = response.data.data?.groups || response.data.data || [];
        const options = [{label: 'All', value: null}];
        for (const group of groups) {
          options.push({
            label: group.name,
            value: group.name,
          });
        }
        this.inlineGroupOptions = options;
      } catch {
        this.inlineGroupOptions = [{label: 'All', value: null}];
      }
    },
    previewStream(stream) {
      if (!stream?.url) {
        this.$q.notify({color: 'negative', message: 'Stream URL missing'});
        return;
      }
      this.videoStore.showPlayer({
        url: stream.url,
        title: stream.name,
        type: stream.url.toLowerCase().includes('.m3u8') ? 'hls' : 'mpegts',
      });
    },
    async copyStreamUrl(stream) {
      if (!stream?.url) {
        this.$q.notify({color: 'negative', message: 'Stream URL missing'});
        return;
      }
      let copyUrl = stream.url;
      if (stream?.id) {
        try {
          const response = await axios.get(`/tic-api/playlists/streams/${stream.id}/preview`);
          if (response.data.success && response.data.preview_url) {
            copyUrl = response.data.preview_url;
          }
        } catch (error) {
          console.error('Copy stream URL resolve error:', error);
        }
      }
      await copyToClipboard(copyUrl);
      this.$q.notify({color: 'positive', message: 'Stream URL copied'});
    },
  },
  setup() {
    const videoStore = useVideoStore();
    return {videoStore};
  },
};
</script>

<style scoped>
.channel-stream-selector {
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

.selector-stream-list {
  margin-bottom: 12px;
}

.selector-stream-item {
  padding-top: 4px;
  padding-bottom: 4px;
}

.stream-logo-wrap {
  width: 44px;
  height: 32px;
  display: inline-flex;
  align-items: center;
  justify-content: center;
  border-radius: var(--tic-radius-sm);
  background: color-mix(in srgb, var(--q-primary), transparent 94%);
  border: 1px solid var(--q-separator-color);
  overflow: hidden;
}

.stream-logo-img {
  max-width: 100%;
  max-height: 100%;
  object-fit: contain;
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
