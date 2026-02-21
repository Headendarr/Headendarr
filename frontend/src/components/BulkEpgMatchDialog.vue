<template>
  <TicDialogWindow
    v-model="isOpen"
    title="Update EPG"
    width="1280px"
    close-tooltip="Close"
    :actions="dialogActions"
    @action="onDialogAction"
    @hide="onDialogHide"
  >
    <div class="bulk-epg-match q-pa-md">
      <div class="text-subtitle1 text-weight-medium q-mb-sm">{{ selectedCount }} channels selected for update</div>
      <div v-if="rows.length" class="bulk-epg-match__select-all-row">
        <q-checkbox
          color="primary"
          :model-value="selectAllModel"
          label="Select all"
          @update:model-value="toggleSelectAll"
        />
      </div>

      <div v-if="loadingPreview" class="row items-center q-gutter-sm q-py-lg">
        <q-spinner size="24px" color="primary" />
        <div>Generating EPG candidates...</div>
      </div>

      <div v-else-if="!rows.length" class="text-grey-7">No channels available for bulk EPG update.</div>

      <div v-else class="bulk-epg-match__list">
        <q-list separator>
          <q-item v-for="row in rows" :key="row.channel.id" class="q-px-none q-py-sm">
            <q-item-section>
              <TicListItemCard
                accent-color="var(--tic-list-card-default-border, transparent)"
                surface-color="var(--tic-list-card-default-bg)"
                header-color="var(--tic-list-card-default-header-bg)"
              >
                <template #header-left>
                  <div class="row items-center no-wrap">
                    <q-checkbox v-model="row.selected" color="primary" class="q-mr-sm" />
                    <div>
                      <div class="text-weight-medium">{{ row.channel.number }} • {{ row.channel.name }}</div>
                      <!--<div class="text-caption text-grey-7">Current guide: {{ formatGuide(row.channel.current_guide) }}</div>-->
                    </div>
                  </div>
                </template>

                <template #header-actions>
                  <TicActionButton
                    icon="play_arrow"
                    color="primary"
                    tooltip="Preview stream"
                    @click="previewChannel(row.channel)"
                  />
                  <TicActionButton
                    icon="content_copy"
                    color="grey-8"
                    tooltip="Copy stream URL"
                    @click="copyChannelStream(row.channel)"
                  />
                </template>

                <div class="bulk-epg-match__row-body row items-start">
                  <div class="col-12 col-md-5 bulk-epg-match__left-col">
                    <div v-if="!row.candidates.length" class="text-negative text-caption">
                      No likely EPG match found.
                    </div>
                    <div v-else>
                      <q-select
                        outlined
                        dense
                        :model-value="row.selectedCandidateKey"
                        label="EPG candidate"
                        :options="candidateOptions(row)"
                        option-label="label"
                        option-value="value"
                        emit-value
                        map-options
                        :clearable="false"
                        :behavior="$q.screen.lt.md ? 'dialog' : 'menu'"
                        @update:model-value="(value) => onCandidateChange(row, value)"
                      >
                        <template #selected-item="scope">
                          <div class="bulk-epg-match__select-selected">
                            <div class="bulk-epg-match__option-main">{{ scope.opt?.title || scope.opt?.label }}</div>
                            <div class="bulk-epg-match__option-sub">{{ scope.opt?.reasonLabel || '' }}</div>
                          </div>
                        </template>
                        <template #option="scope">
                          <q-item v-bind="scope.itemProps">
                            <q-item-section>
                              <q-item-label class="bulk-epg-match__option-main">
                                {{ scope.opt?.title || scope.opt?.label }}
                              </q-item-label>
                              <q-item-label caption class="bulk-epg-match__option-sub">
                                {{ scope.opt?.reasonLabel || '' }}
                              </q-item-label>
                            </q-item-section>
                          </q-item>
                        </template>
                        <template #no-option>
                          <q-item>
                            <q-item-section class="text-grey">No matching options</q-item-section>
                          </q-item>
                        </template>
                      </q-select>
                      <div v-if="row.selectedCandidate" class="text-caption text-grey-7 q-mt-xs bulk-epg-match__match-summary">
                        {{ candidateMatchSummary(row) }}
                      </div>
                      <q-checkbox
                        v-if="row.selectedCandidate"
                        v-model="row.useEpgLogo"
                        color="primary"
                        dense
                        class="q-mt-xs"
                        label="Use EPG Logo"
                      />
                      <div v-if="row.selectedCandidate" class="text-caption text-grey-7 q-ml-sm">
                        Replaces channel logo with the selected EPG channel logo.
                      </div>
                    </div>
                  </div>

                  <div class="col-auto gt-sm bulk-epg-match__divider-col">
                    <q-separator vertical class="bulk-epg-match__vertical-separator" />
                  </div>

                  <div class="col-12 col-md bulk-epg-match__right-col">
                    <div v-if="row.previewLoading" class="row items-center q-gutter-sm q-mt-sm">
                      <q-spinner size="18px" color="primary" />
                      <div class="text-caption text-grey-7">Loading EPG details...</div>
                    </div>

                    <div v-else-if="row.preview" class="text-caption">
                      <div class="row items-start no-wrap">
                        <TicChannelIcon
                          class="q-mr-sm bulk-epg-match__preview-icon"
                          :src="row.preview.icon_url || ''"
                          :size="46"
                          fallback-icon="tv"
                        />
                        <div class="col">
                          <div class="text-weight-medium bulk-epg-match__preview-title">
                            {{ row.preview.epg_name }} ★ {{ row.preview.name }}
                          </div>
                          <div class="bulk-epg-match__detail-row">
                            <span class="text-weight-medium">Channel ID:</span> {{ row.preview.channel_id || '-' }}
                            <span class="q-mx-xs">•</span>
                            <span class="text-weight-medium">Now->future:</span>
                            {{ row.preview.programmes_now_to_future || 0 }}
                            <span class="q-mx-xs">•</span>
                            <span class="text-weight-medium">Total:</span> {{ row.preview.total_programmes || 0 }}
                          </div>
                          <div class="bulk-epg-match__detail-row">
                            <span class="text-weight-medium">Coverage horizon:</span>
                            {{ formatHorizon(row.preview.future_horizon_hours) }}
                          </div>
                          <div class="bulk-epg-match__detail-row">
                            <span class="text-weight-medium">Now:</span>
                            <span class="bulk-epg-match__detail-value-clamp">
                              {{ formatNowProgramme(row.preview.now_programme) }}
                            </span>
                          </div>
                          <div class="bulk-epg-match__detail-row">
                            <span class="text-weight-medium">Next:</span>
                            <span class="bulk-epg-match__detail-value-clamp">
                              {{ formatUpcoming(row.preview.next_programmes) }}
                            </span>
                          </div>
                        </div>
                      </div>
                    </div>
                    <div v-else class="text-caption text-grey-7">Select an EPG candidate to view details.</div>
                  </div>
                </div>
              </TicListItemCard>
            </q-item-section>
          </q-item>
        </q-list>
      </div>
    </div>
  </TicDialogWindow>
</template>

<script>
import axios from 'axios';
import {copyToClipboard} from 'quasar';
import {useVideoStore} from 'stores/video';
import {
  TicActionButton,
  TicChannelIcon,
  TicDialogWindow,
  TicListItemCard,
} from 'components/ui';

export default {
  name: 'BulkEpgMatchDialog',
  components: {
    TicActionButton,
    TicChannelIcon,
    TicDialogWindow,
    TicListItemCard,
  },
  props: {
    channelIds: {
      type: Array,
      default: () => [],
    },
  },
  emits: ['ok', 'hide'],
  data() {
    return {
      isOpen: false,
      loadingPreview: false,
      saving: false,
      rows: [],
      summary: {
        channels_considered: 0,
        with_candidates: 0,
        without_candidates: 0,
      },
    };
  },
  computed: {
    selectableRows() {
      return this.rows.filter((row) => row.selectedCandidate);
    },
    allRowsSelected() {
      return this.selectableRows.length > 0 && this.selectableRows.every((row) => row.selected);
    },
    someRowsSelected() {
      return this.selectableRows.some((row) => row.selected);
    },
    selectAllModel() {
      if (this.allRowsSelected) {
        return true;
      }
      if (!this.someRowsSelected) {
        return false;
      }
      return null;
    },
    selectedCount() {
      return this.rows.filter((row) => row.selected && row.selectedCandidate).length;
    },
    dialogActions() {
      return [
        {
          id: 'save',
          icon: 'save',
          label: 'Save',
          color: 'positive',
          loading: this.saving,
          disable: this.loadingPreview || this.saving || this.selectedCount < 1,
        },
      ];
    },
  },
  methods: {
    show() {
      this.isOpen = true;
      this.fetchPreview();
    },
    hide() {
      this.isOpen = false;
    },
    onDialogHide() {
      this.$emit('hide');
    },
    async fetchPreview() {
      this.loadingPreview = true;
      this.rows = [];
      this.summary = {
        channels_considered: 0,
        with_candidates: 0,
        without_candidates: 0,
      };
      try {
        const response = await axios.post('/tic-api/channels/bulk/epg-match/preview', {
          scope: 'selected',
          channel_ids: this.channelIds || [],
          overwrite_existing: false,
          max_candidates_per_channel: 5,
        });
        const payload = response?.data?.data || {};
        this.summary = payload.summary || this.summary;
        const rows = (payload.rows || []).map((row) => {
          const candidates = Array.isArray(row.candidates) ? row.candidates : [];
          const first = candidates.length ? candidates[0] : null;
          return {
            ...row,
            candidates,
            selectedCandidateKey: first ? this.candidateKey(first) : '',
            selectedCandidate: first,
            selected: Boolean(first),
            useEpgLogo: false,
            preview: null,
            previewLoading: false,
          };
        });
        this.rows = rows;
        await this.preloadSelectedCandidateDetails();
      } catch (error) {
        console.error('Failed to load bulk EPG preview:', error);
        this.$q.notify({
          color: 'negative',
          icon: 'report_problem',
          message: 'Failed to load EPG match candidates',
        });
      } finally {
        this.loadingPreview = false;
      }
    },
    toggleSelectAll(value) {
      const checked = value === null ? true : Boolean(value);
      this.rows.forEach((row) => {
        if (row.selectedCandidate) {
          row.selected = checked;
        }
      });
    },
    async preloadSelectedCandidateDetails() {
      for (const row of this.rows) {
        if (row.selected && row.selectedCandidate) {
          // Keep requests controlled to avoid flooding the API when many channels are selected.
          // eslint-disable-next-line no-await-in-loop
          await this.fetchCandidatePreview(row, row.selectedCandidate);
        }
      }
    },
    candidateKey(candidate) {
      return `${candidate.epg_id}::${candidate.epg_channel_row_id || candidate.epg_channel_id}`;
    },
    candidateOptions(row) {
      return (row.candidates || []).map((candidate) => ({
        title: `${candidate.epg_name || `EPG ${candidate.epg_id}`} • ${candidate.epg_display_name ||
        candidate.epg_channel_id}`,
        reasonLabel: `${this.candidateReasonLabel(candidate.reason)} (${Number(
          candidate.total_programmes || 0)} programmes)`,
        label: `${candidate.epg_name || `EPG ${candidate.epg_id}`} • ${candidate.epg_display_name ||
        candidate.epg_channel_id}`,
        value: this.candidateKey(candidate),
      }));
    },
    candidateReasonLabel(reason) {
      const map = {
        source_channel_id_exact: 'Matched from stream source channel ID',
        source_tvg_id_exact: 'Matched from stream source TVG ID',
        existing_mapping_valid: 'Existing guide mapping is still valid',
        name_exact: 'Exact channel name match',
        name_variant_plus_one_penalty: 'Timeshift (+1/plus1) fallback match',
        name_fuzzy: 'Fuzzy name similarity match',
      };
      return map[reason] || 'Matched by fallback logic';
    },
    candidateMatchSummary(row) {
      const candidate = row?.selectedCandidate;
      if (!candidate) {
        return '';
      }
      const rank = Number(candidate.rank || 0);
      const total = Array.isArray(row?.candidates) ? row.candidates.length : 0;
      const reason = this.candidateReasonLabel(candidate.reason);
      if (rank > 0 && total > 0) {
        return `${reason}. Priority rank ${rank} of ${total}.`;
      }
      return `${reason}.`;
    },
    async onCandidateChange(row, value) {
      row.selectedCandidateKey = value;
      row.selectedCandidate = (
        row.candidates || []
      ).find((candidate) => this.candidateKey(candidate) === value) || null;
      row.preview = null;
      if (row.selectedCandidate) {
        await this.fetchCandidatePreview(row, row.selectedCandidate);
      }
    },
    async fetchCandidatePreview(row, candidate) {
      if (!candidate?.epg_channel_row_id) {
        row.preview = null;
        return;
      }
      row.previewLoading = true;
      try {
        const response = await axios.post('/tic-api/channels/bulk/epg-match/candidate-preview', {
          epg_channel_row_id: candidate.epg_channel_row_id,
        });
        row.preview = response?.data?.data || null;
      } catch (error) {
        row.preview = null;
      } finally {
        row.previewLoading = false;
      }
    },
    async onDialogAction(action) {
      if (action?.id !== 'save' || this.saving) {
        return;
      }
      this.saving = true;
      try {
        const updates = this.rows.
          filter((row) => row.selected && row.selectedCandidate).
          map((row) => ({
            channel_id: row.channel.id,
            epg_id: row.selectedCandidate.epg_id,
            epg_channel_id: row.selectedCandidate.epg_channel_id,
            use_epg_logo: !!row.useEpgLogo,
          }));

        const response = await axios.post('/tic-api/channels/bulk/epg-match/apply', {updates});
        const summary = response?.data?.data?.summary || {updated: 0, skipped: 0, failed: 0};
        this.$q.notify({
          color: summary.failed > 0 ? 'warning' : 'positive',
          icon: summary.failed > 0 ? 'warning' : 'check_circle',
          message: `EPG updates complete. Updated ${summary.updated}, skipped ${summary.skipped}, failed ${summary.failed}.`,
          timeout: 4500,
        });
        this.$emit('ok', {refresh: true, summary});
        this.hide();
      } catch (error) {
        console.error('Failed to apply bulk EPG updates:', error);
        this.$q.notify({
          color: 'negative',
          icon: 'report_problem',
          message: 'Failed to apply EPG updates',
        });
      } finally {
        this.saving = false;
      }
    },
    async previewChannel(channel) {
      try {
        const response = await axios.get(`/tic-api/channels/${channel.id}/preview`);
        if (response?.data?.success) {
          this.videoStore.showPlayer({
            url: response.data.preview_url,
            title: channel.name,
            type: response.data.stream_type || 'auto',
          });
          return;
        }
        this.$q.notify({color: 'negative', message: response?.data?.message || 'Failed to load preview'});
      } catch (error) {
        this.$q.notify({color: 'negative', message: 'Failed to load preview'});
      }
    },
    async copyChannelStream(channel) {
      try {
        const response = await axios.get(`/tic-api/channels/${channel.id}/preview`);
        if (response?.data?.success) {
          await copyToClipboard(response.data.preview_url);
          this.$q.notify({color: 'positive', message: 'Stream URL copied'});
          return;
        }
        this.$q.notify({color: 'negative', message: response?.data?.message || 'Failed to copy stream URL'});
      } catch (error) {
        this.$q.notify({color: 'negative', message: 'Failed to copy stream URL'});
      }
    },
    formatGuide(guide) {
      if (!guide) {
        return 'Not assigned';
      }
      return `${guide.epg_name || 'EPG'} - ${guide.channel_id || '-'}`;
    },
    formatTime(ts) {
      if (!ts) {
        return '';
      }
      try {
        return new Date(Number(ts) * 1000).toLocaleString();
      } catch {
        return '';
      }
    },
    formatNowProgramme(nowProgramme) {
      if (!nowProgramme) {
        return 'No programme currently scheduled';
      }
      return `${nowProgramme.title || '(Untitled)'} (${this.formatTime(nowProgramme.start_ts)} - ${this.formatTime(
        nowProgramme.stop_ts)})`;
    },
    formatUpcoming(nextProgrammes) {
      const rows = Array.isArray(nextProgrammes) ? nextProgrammes.filter(Boolean) : [];
      if (!rows.length) {
        return 'No upcoming programme data';
      }
      return rows.
        slice(0, 3).
        map((programme) => `${programme.title || '(Untitled)'} @ ${this.formatTime(programme.start_ts)}`).
        join(' | ');
    },
    formatHorizon(hours) {
      if (hours === null || typeof hours === 'undefined') {
        return 'No future data';
      }
      return `${hours}h`;
    },
  },
  setup() {
    const videoStore = useVideoStore();
    return {videoStore};
  },
};
</script>

<style scoped>
.bulk-epg-match__list {
  max-height: calc(100vh - 240px);
  overflow: auto;
}

.bulk-epg-match__select-all-row {
  margin-bottom: 6px;
}

.bulk-epg-match__row-body {
  border-top: 1px solid var(--q-separator-color);
  padding-top: 8px;
  min-height: 148px;
}

.bulk-epg-match__left-col {
  padding-right: 12px;
}

.bulk-epg-match__divider-col {
  display: flex;
  justify-content: center;
  padding: 0 4px;
  align-self: stretch;
}

.bulk-epg-match__vertical-separator {
  height: 100%;
}

.bulk-epg-match__right-col {
  min-width: 0;
  padding-left: 12px;
}

.bulk-epg-match__preview-icon {
  align-self: flex-start;
}

.bulk-epg-match__preview-title {
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.bulk-epg-match__detail-row {
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.bulk-epg-match__detail-value-clamp {
  display: inline-block;
  max-width: calc(100% - 56px);
  overflow: hidden;
  text-overflow: ellipsis;
  vertical-align: bottom;
}

.bulk-epg-match__select-selected {
  min-width: 0;
  max-width: 100%;
  line-height: 1.2;
  padding: 2px 0;
  min-height: 2.4em;
}

.bulk-epg-match__option-main {
  font-size: 0.86rem;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  line-height: 1.2;
}

.bulk-epg-match__option-sub {
  font-size: 0.75rem;
  color: var(--q-grey-7);
  line-height: 1.2;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.bulk-epg-match__match-summary {
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

@media (max-width: 1023px) {
  .bulk-epg-match__list {
    max-height: calc(100vh - 200px);
  }

  .bulk-epg-match__left-col {
    padding-right: 0;
  }

  .bulk-epg-match__right-col {
    padding-left: 0;
    margin-top: 10px;
  }
}
</style>
