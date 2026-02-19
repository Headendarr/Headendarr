<template>
  <q-page>
    <div :class="$q.screen.lt.sm ? 'q-pa-none' : 'q-pa-md'">
      <div class="row">
        <div
          :class="
            uiStore.showHelp && !$q.screen.lt.md ? 'col-sm-7 col-md-8 help-main' : 'col-12 help-main help-main--full'
          "
        >
          <q-card flat>
            <div class="dvr-tabs-bar">
              <q-tabs
                v-model="tab"
                align="left"
                class="dvr-tabs text-primary"
                content-class="dvr-tabs-content"
              >
                <q-tab name="recordings" label="Recordings" />
                <q-tab name="rules" label="Recording Rules" />
              </q-tabs>
            </div>

            <q-separator />

            <q-tab-panels v-model="tab" animated class="dvr-tab-panels">
              <q-tab-panel name="recordings">
                <div class="row q-col-gutter-sm items-end q-mb-sm dvr-toolbar">
                  <div :class="$q.screen.lt.sm ? 'col-12' : 'col-auto'">
                    <TicButton
                      label="Schedule Recording"
                      icon="add"
                      color="primary"
                      class="section-toolbar-btn"
                      :class="$q.screen.lt.sm ? 'full-width' : ''"
                      @click="showScheduleDialog = true"
                    />
                  </div>
                  <div :class="$q.screen.lt.sm ? 'col-12' : 'col-12 col-sm-6 col-md-4'">
                    <TicSearchInput
                      v-model="recordingsSearch"
                      class="section-toolbar-field"
                      label="Search recordings"
                      placeholder="Title, channel, status..."
                    />
                  </div>

                  <template v-if="!$q.screen.lt.md">
                    <div class="col-12 col-sm-6 col-md-3">
                      <TicSelectInput
                        v-model="statusFilter"
                        class="section-toolbar-field"
                        label="Status"
                        :options="statusOptions"
                        option-label="label"
                        option-value="value"
                        :emit-value="true"
                        :map-options="true"
                        :clearable="false"
                        :dense="true"
                        :behavior="$q.screen.lt.md ? 'dialog' : 'menu'"
                      />
                    </div>
                  </template>
                  <template v-else>
                    <div :class="$q.screen.lt.sm ? 'col-6 section-toolbar-split-left' : 'col-auto'">
                      <TicButton
                        label="Filters"
                        icon="filter_list"
                        color="secondary"
                        :dense="$q.screen.lt.sm"
                        class="section-toolbar-btn section-toolbar-btn--compact"
                        @click="recordingsFilterDialogOpen = true"
                      />
                    </div>
                  </template>

                  <div :class="$q.screen.lt.sm ? 'col-6 section-toolbar-split-right' : 'col-auto'">
                    <TicButton
                      :label="$q.screen.lt.md ? 'Sort' : recordingsSortLabel"
                      icon="sort"
                      color="secondary"
                      :dense="$q.screen.lt.sm"
                      class="section-toolbar-btn section-toolbar-btn--compact"
                      @click="recordingsSortDialogOpen = true"
                    />
                  </div>
                </div>

                <q-list class="dvr-list">
                  <q-item
                    v-for="recording in visibleRecordings"
                    :key="recording.id"
                    class="dvr-list-item"
                  >
                    <q-item-section>
                      <TicListItemCard v-bind="recordingCardProps(recording)">
                        <template #header-left>
                          <div class="dvr-card-title text-weight-medium">
                            {{ recording.title || 'Untitled Recording' }}
                          </div>
                          <div class="text-caption text-grey-7">
                            {{ recording.channel_name || 'Unknown channel' }}
                          </div>
                        </template>
                        <template #header-actions>
                          <TicActionButton
                            v-for="action in recordingActions(recording)"
                            :key="`recording-${recording.id}-${action.id}`"
                            :icon="action.icon"
                            :color="action.color || 'grey-8'"
                            :tooltip="action.label || ''"
                            @click="handleRecordingAction(action, recording)"
                          />
                        </template>
                        <div class="dvr-meta-grid q-mt-sm">
                          <div class="dvr-meta-field">
                            <span class="text-caption text-grey-7">Start</span>
                            <span>{{ formatTs(recording.start_ts) }}</span>
                          </div>
                          <div class="dvr-meta-field">
                            <span class="text-caption text-grey-7">Stop</span>
                            <span>{{ formatTs(recording.stop_ts) }}</span>
                          </div>
                          <div class="dvr-meta-field">
                            <span class="text-caption text-grey-7">Status</span>
                            <q-chip
                              dense
                              class="dvr-status-chip"
                              :color="recordingStatusColor(recording.status)"
                              text-color="white"
                            >
                              {{ recording.status || '-' }}
                            </q-chip>
                          </div>
                          <div class="dvr-meta-field">
                            <span class="text-caption text-grey-7">TVHeadend Sync</span>
                            <q-chip
                              dense
                              class="dvr-status-chip"
                              :color="syncStatusColor(recording.sync_status)"
                              text-color="white"
                            >
                              {{ recording.sync_status || '-' }}
                            </q-chip>
                          </div>
                        </div>
                      </TicListItemCard>
                    </q-item-section>
                  </q-item>

                  <q-item v-if="!loadingRecordings && !visibleRecordings.length">
                    <q-item-section>
                      <q-item-label class="text-grey-7"> No recordings found.</q-item-label>
                    </q-item-section>
                  </q-item>
                </q-list>

                <q-infinite-scroll
                  v-if="!loadingRecordings && hasMoreRecordings"
                  ref="recordingsInfiniteRef"
                  :offset="80"
                  scroll-target="body"
                  @load="onRecordingsLoad"
                >
                  <template #loading>
                    <div class="row flex-center q-my-md">
                      <q-spinner-dots size="30px" color="primary" />
                    </div>
                  </template>
                </q-infinite-scroll>

                <q-inner-loading :showing="loadingRecordings">
                  <q-spinner-dots size="42px" color="primary" />
                </q-inner-loading>
              </q-tab-panel>

              <q-tab-panel name="rules">
                <div class="row q-col-gutter-sm items-end q-mb-sm dvr-toolbar">
                  <div :class="$q.screen.lt.sm ? 'col-12' : 'col-auto'">
                    <TicButton
                      label="Add Rule"
                      icon="add"
                      color="primary"
                      class="section-toolbar-btn"
                      :class="$q.screen.lt.sm ? 'full-width' : ''"
                      @click="openRuleDialog()"
                    />
                  </div>
                  <div :class="$q.screen.lt.sm ? 'col-12' : 'col-12 col-sm-6 col-md-4'">
                    <TicSearchInput
                      v-model="rulesSearch"
                      class="section-toolbar-field"
                      label="Search rules"
                      placeholder="Title match or channel..."
                    />
                  </div>

                  <div :class="$q.screen.lt.sm ? 'col-12 section-toolbar-split-right' : 'col-auto'">
                    <TicButton
                      :label="$q.screen.lt.md ? 'Sort' : rulesSortLabel"
                      icon="sort"
                      color="secondary"
                      :dense="$q.screen.lt.sm"
                      class="section-toolbar-btn section-toolbar-btn--compact"
                      @click="rulesSortDialogOpen = true"
                    />
                  </div>
                </div>

                <q-list class="dvr-list">
                  <q-item v-for="rule in visibleRules" :key="rule.id" class="dvr-list-item">
                    <q-item-section>
                      <TicListItemCard>
                        <template #header-left>
                          <div class="dvr-card-title text-weight-medium">
                            {{ rule.title_match || 'Untitled Rule' }}
                          </div>
                          <div class="text-caption text-grey-7">
                            {{ rule.channel_name || 'All channels' }}
                          </div>
                        </template>
                        <template #header-actions>
                          <TicActionButton
                            v-for="action in ruleActions(rule)"
                            :key="`rule-${rule.id}-${action.id}`"
                            :icon="action.icon"
                            :color="action.color || 'grey-8'"
                            :tooltip="action.label || ''"
                            @click="handleRuleAction(action, rule)"
                          />
                        </template>
                        <div class="dvr-meta-grid q-mt-sm">
                          <div class="dvr-meta-field">
                            <span class="text-caption text-grey-7">Lookahead</span>
                            <span>{{ rule.lookahead_days || 7 }} days</span>
                          </div>
                        </div>
                      </TicListItemCard>
                    </q-item-section>
                  </q-item>

                  <q-item v-if="!loadingRules && !visibleRules.length">
                    <q-item-section>
                      <q-item-label class="text-grey-7"> No recording rules found.</q-item-label>
                    </q-item-section>
                  </q-item>
                </q-list>

                <q-infinite-scroll
                  v-if="!loadingRules && hasMoreRules"
                  ref="rulesInfiniteRef"
                  :offset="80"
                  scroll-target="body"
                  @load="onRulesLoad"
                >
                  <template #loading>
                    <div class="row flex-center q-my-md">
                      <q-spinner-dots size="30px" color="primary" />
                    </div>
                  </template>
                </q-infinite-scroll>

                <q-inner-loading :showing="loadingRules">
                  <q-spinner-dots size="42px" color="primary" />
                </q-inner-loading>
              </q-tab-panel>
            </q-tab-panels>
          </q-card>
        </div>

        <TicResponsiveHelp v-model="uiStore.showHelp">
          <q-card-section>
            <div class="text-h5 q-mb-none">Setup Steps:</div>
            <q-list>
              <q-separator inset spaced />
              <q-item>
                <q-item-section>
                  <q-item-label>
                    1. Use <b>Schedule Recording</b> to create one-time recordings with channel and time range.
                  </q-item-label>
                </q-item-section>
              </q-item>
              <q-item>
                <q-item-section>
                  <q-item-label> 2. Use <b>Recording Rules</b> for recurring scheduling by title match.</q-item-label>
                </q-item-section>
              </q-item>
              <q-item>
                <q-item-section>
                  <q-item-label> 3. Use status and sync details to verify Headendarr and TVHeadend are aligned.</q-item-label>
                </q-item-section>
              </q-item>
            </q-list>
          </q-card-section>
        </TicResponsiveHelp>
      </div>
    </div>

    <TicDialogWindow
      v-model="showScheduleDialog"
      title="Schedule Recording"
      width="620px"
      :actions="scheduleDialogActions"
      @action="onScheduleDialogAction"
    >
      <div class="q-pa-md">
        <q-form class="tic-form-layout">
          <TicSelectInput
            v-model="scheduleForm.channel_id"
            label="Channel"
            description="Select the channel to record."
            :options="channelOptions"
            option-label="label"
            option-value="value"
            :emit-value="true"
            :map-options="true"
            :clearable="false"
            :behavior="$q.screen.lt.md ? 'dialog' : 'menu'"
          />
          <TicTextInput
            v-model="scheduleForm.title"
            label="Title"
            description="Recording title shown in Headendarr and TVHeadend."
          />
          <TicTextInput
            v-model="scheduleForm.start"
            type="datetime-local"
            label="Start"
            description="Local start date and time."
          />
          <TicTextInput
            v-model="scheduleForm.stop"
            type="datetime-local"
            label="Stop"
            description="Local stop date and time."
          />
          <TicSelectInput
            v-model="scheduleForm.recording_profile_key"
            label="Recording Profile"
            description="Choose the recording pathname/profile to use for this schedule."
            :options="recordingProfileOptions"
            option-label="label"
            option-value="value"
            :emit-value="true"
            :map-options="true"
            :clearable="false"
            :behavior="$q.screen.lt.md ? 'dialog' : 'menu'"
          />
        </q-form>
      </div>
    </TicDialogWindow>

    <TicDialogWindow
      v-model="showRuleDialog"
      :title="isEditingRule ? 'Edit Recording Rule' : 'Create Recording Rule'"
      width="620px"
      :actions="ruleDialogActions"
      @action="onRuleDialogAction"
    >
      <div class="q-pa-md">
        <q-form class="tic-form-layout">
          <TicSelectInput
            v-model="ruleForm.channel_id"
            label="Channel"
            description="Select a channel for this rule."
            :options="channelOptions"
            option-label="label"
            option-value="value"
            :emit-value="true"
            :map-options="true"
            :clearable="false"
            :behavior="$q.screen.lt.md ? 'dialog' : 'menu'"
          />
          <TicTextInput
            v-model="ruleForm.title_match"
            label="Title Match"
            description="Only programmes with matching titles are scheduled."
          />
          <TicNumberInput
            v-model="ruleForm.lookahead_days"
            label="Lookahead Days"
            description="How many days ahead Headendarr should schedule recordings."
            :min="1"
            :max="30"
          />
          <TicSelectInput
            v-model="ruleForm.recording_profile_key"
            label="Recording Profile"
            description="Profile used for recordings generated by this rule."
            :options="recordingProfileOptions"
            option-label="label"
            option-value="value"
            :emit-value="true"
            :map-options="true"
            :clearable="false"
            :behavior="$q.screen.lt.md ? 'dialog' : 'menu'"
          />
        </q-form>
      </div>
    </TicDialogWindow>

    <TicDialogPopup v-model="recordingsFilterDialogOpen" title="Filter Recordings" width="560px" max-width="95vw">
      <div class="tic-form-layout">
        <TicSelectInput
          v-model="filterDraft.status"
          label="Status"
          description="Filter recordings by status."
          :options="statusOptions"
          option-label="label"
          option-value="value"
          :emit-value="true"
          :map-options="true"
          :clearable="false"
          :behavior="$q.screen.lt.md ? 'dialog' : 'menu'"
        />
      </div>
      <template #actions>
        <TicButton label="Clear" variant="flat" color="grey-7" @click="clearRecordingFilterDraft" />
        <TicButton label="Apply" icon="check" color="positive" @click="applyRecordingFilterDraft" />
      </template>
    </TicDialogPopup>

    <TicDialogPopup v-model="recordingsSortDialogOpen" title="Sort Recordings" width="560px" max-width="95vw">
      <div class="tic-form-layout">
        <TicSelectInput
          v-model="recordingsSortDraft.sortBy"
          label="Sort By"
          :options="recordingSortOptions"
          option-label="label"
          option-value="value"
          :emit-value="true"
          :map-options="true"
          :clearable="false"
          :behavior="$q.screen.lt.md ? 'dialog' : 'menu'"
        />
        <TicSelectInput
          v-model="recordingsSortDraft.sortDirection"
          label="Direction"
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
        <TicButton label="Clear" variant="flat" color="grey-7" @click="clearRecordingSortDraft" />
        <TicButton label="Apply" icon="check" color="positive" @click="applyRecordingSortDraft" />
      </template>
    </TicDialogPopup>

    <TicDialogPopup v-model="rulesSortDialogOpen" title="Sort Rules" width="560px" max-width="95vw">
      <div class="tic-form-layout">
        <TicSelectInput
          v-model="rulesSortDraft.sortBy"
          label="Sort By"
          :options="ruleSortOptions"
          option-label="label"
          option-value="value"
          :emit-value="true"
          :map-options="true"
          :clearable="false"
          :behavior="$q.screen.lt.md ? 'dialog' : 'menu'"
        />
        <TicSelectInput
          v-model="rulesSortDraft.sortDirection"
          label="Direction"
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
        <TicButton label="Clear" variant="flat" color="grey-7" @click="clearRuleSortDraft" />
        <TicButton label="Apply" icon="check" color="positive" @click="applyRuleSortDraft" />
      </template>
    </TicDialogPopup>
  </q-page>
</template>

<script>
import {defineComponent} from 'vue';
import axios from 'axios';
import {useUiStore} from 'stores/ui';
import {useVideoStore} from 'stores/video';
import {
  TicButton,
  TicActionButton,
  TicConfirmDialog,
  TicDialogPopup,
  TicDialogWindow,
  TicListItemCard,
  TicNumberInput,
  TicResponsiveHelp,
  TicSearchInput,
  TicSelectInput,
  TicTextInput,
} from 'components/ui';

const DVR_PAGE_SIZE = 50;

export default defineComponent({
  name: 'DvrPage',
  components: {
    TicButton,
    TicActionButton,
    TicDialogPopup,
    TicDialogWindow,
    TicListItemCard,
    TicNumberInput,
    TicResponsiveHelp,
    TicSearchInput,
    TicSelectInput,
    TicTextInput,
  },
  setup() {
    return {
      uiStore: useUiStore(),
      videoStore: useVideoStore(),
    };
  },
  data() {
    return {
      tab: 'recordings',
      loadingRecordings: false,
      loadingRules: false,
      recordings: [],
      rules: [],
      channels: [],
      recordingProfiles: [],
      showScheduleDialog: false,
      showRuleDialog: false,
      isEditingRule: false,
      activeRuleId: null,

      statusFilter: null,
      recordingsSearch: '',
      rulesSearch: '',

      recordingsSort: {
        sortBy: 'start_ts',
        sortDirection: 'desc',
      },
      rulesSort: {
        sortBy: 'title_match',
        sortDirection: 'asc',
      },

      recordingsFilterDialogOpen: false,
      recordingsSortDialogOpen: false,
      rulesSortDialogOpen: false,

      filterDraft: {
        status: null,
      },
      recordingsSortDraft: {
        sortBy: 'start_ts',
        sortDirection: 'desc',
      },
      rulesSortDraft: {
        sortBy: 'title_match',
        sortDirection: 'asc',
      },

      visibleRecordingsCount: DVR_PAGE_SIZE,
      visibleRulesCount: DVR_PAGE_SIZE,

      scheduleSaving: false,
      ruleSaving: false,
      scheduleForm: {
        channel_id: null,
        title: '',
        start: '',
        stop: '',
        recording_profile_key: null,
      },
      ruleForm: {
        channel_id: null,
        title_match: '',
        lookahead_days: 7,
        recording_profile_key: null,
      },
      pollingActive: false,
      pollAbortController: null,
      pollLoopPromise: null,
      dvrRefreshTimerId: null,
    };
  },
  computed: {
    statusOptions() {
      return [
        {label: 'All', value: null},
        {label: 'Scheduled', value: 'scheduled'},
        {label: 'Recording', value: 'recording'},
        {label: 'Completed', value: 'completed'},
        {label: 'Canceled', value: 'canceled'},
        {label: 'Deleted', value: 'deleted'},
        {label: 'Failed', value: 'failed'},
      ];
    },
    sortDirectionOptions() {
      return [
        {label: 'Ascending', value: 'asc'},
        {label: 'Descending', value: 'desc'},
      ];
    },
    recordingSortOptions() {
      return [
        {label: 'Start Time', value: 'start_ts'},
        {label: 'Stop Time', value: 'stop_ts'},
        {label: 'Title', value: 'title'},
        {label: 'Channel', value: 'channel_name'},
        {label: 'Status', value: 'status'},
      ];
    },
    ruleSortOptions() {
      return [
        {label: 'Title Match', value: 'title_match'},
        {label: 'Channel', value: 'channel_name'},
        {label: 'Lookahead Days', value: 'lookahead_days'},
      ];
    },
    recordingsSortLabel() {
      const sort = this.recordingSortOptions.find((item) => item.value === this.recordingsSort.sortBy);
      return sort ? `Sort: ${sort.label}` : 'Sort';
    },
    rulesSortLabel() {
      const sort = this.ruleSortOptions.find((item) => item.value === this.rulesSort.sortBy);
      return sort ? `Sort: ${sort.label}` : 'Sort';
    },
    channelOptions() {
      return (this.channels || []).map((channel) => ({
        label: channel.name,
        value: channel.id,
      }));
    },
    recordingProfileOptions() {
      return (this.recordingProfiles || []).map((profile) => ({
        label: profile.name || profile.key || 'Profile',
        value: profile.key || null,
      }));
    },
    defaultRecordingProfileKey() {
      return this.recordingProfileOptions[0]?.value || null;
    },
    filteredSortedRecordings() {
      const statusFilter = this.statusFilter ? String(this.statusFilter).toLowerCase() : null;
      const search = String(this.recordingsSearch || '').trim().toLowerCase();
      const filtered = (this.recordings || []).filter((recording) => {
        const status = String(recording.status || '').toLowerCase();
        if (statusFilter) {
          if (statusFilter === 'recording') {
            if (!(status === 'recording' || status.includes('running') || status.includes('in_progress'))) {
              return false;
            }
          } else if (statusFilter === 'completed') {
            if (!'completed finished done success recorded ok'.split(' ').some((token) => status.includes(token))) {
              return false;
            }
          } else if (!status.includes(statusFilter)) {
            return false;
          }
        }

        if (!search) {
          return true;
        }

        const values = [recording.title, recording.channel_name, recording.status, recording.sync_status];
        return values.some((value) =>
          String(value || '').toLowerCase().includes(search),
        );
      });

      return this.sortRows(filtered, this.recordingsSort.sortBy, this.recordingsSort.sortDirection);
    },
    visibleRecordings() {
      return this.filteredSortedRecordings.slice(0, this.visibleRecordingsCount);
    },
    hasMoreRecordings() {
      return this.visibleRecordingsCount < this.filteredSortedRecordings.length;
    },
    filteredSortedRules() {
      const search = String(this.rulesSearch || '').trim().toLowerCase();
      const filtered = (this.rules || []).filter((rule) => {
        if (!search) {
          return true;
        }
        const values = [rule.title_match, rule.channel_name, String(rule.lookahead_days || '')];
        return values.some((value) =>
          String(value || '').toLowerCase().includes(search),
        );
      });
      return this.sortRows(filtered, this.rulesSort.sortBy, this.rulesSort.sortDirection);
    },
    visibleRules() {
      return this.filteredSortedRules.slice(0, this.visibleRulesCount);
    },
    hasMoreRules() {
      return this.visibleRulesCount < this.filteredSortedRules.length;
    },
    scheduleDialogActions() {
      return [
        {
          id: 'save',
          label: 'Save',
          icon: 'save',
          color: 'positive',
          loading: this.scheduleSaving,
        },
      ];
    },
    ruleDialogActions() {
      return [
        {
          id: 'save',
          label: 'Save',
          icon: 'save',
          color: 'positive',
          loading: this.ruleSaving,
        },
      ];
    },
  },
  watch: {
    recordingsSearch() {
      this.resetVisibleRecordings();
    },
    statusFilter() {
      this.resetVisibleRecordings();
    },
    recordingsSort: {
      deep: true,
      handler() {
        this.resetVisibleRecordings();
      },
    },
    rulesSearch() {
      this.resetVisibleRules();
    },
    rulesSort: {
      deep: true,
      handler() {
        this.resetVisibleRules();
      },
    },
  },
  methods: {
    sortRows(rows, sortBy, sortDirection) {
      const direction = sortDirection === 'desc' ? -1 : 1;
      return [...rows].sort((a, b) => {
        const left = a?.[sortBy];
        const right = b?.[sortBy];

        if (left == null && right == null) return 0;
        if (left == null) return 1;
        if (right == null) return -1;

        if (typeof left === 'number' && typeof right === 'number') {
          return (left - right) * direction;
        }

        return String(left).localeCompare(String(right), undefined, {numeric: true, sensitivity: 'base'}) * direction;
      });
    },
    formatTs(ts) {
      if (!ts) return '-';
      return new Date(ts * 1000).toLocaleString();
    },
    recordingStatusColor(status) {
      const normalized = String(status || '').toLowerCase();
      if (normalized.includes('recording') || normalized.includes('running') || normalized.includes('in_progress')) {
        return 'warning';
      }
      if (
        normalized.includes('completed') ||
        normalized.includes('finished') ||
        normalized.includes('done') ||
        normalized.includes('success') ||
        normalized.includes('recorded') ||
        normalized.includes('ok')
      ) {
        return 'positive';
      }
      if (normalized.includes('failed') || normalized.includes('error') || normalized.includes('missing')) {
        return 'negative';
      }
      if (normalized.includes('cancel')) {
        return 'grey-7';
      }
      if (normalized.includes('scheduled') || normalized.includes('queued')) {
        return 'primary';
      }
      return 'grey-7';
    },
    syncStatusColor(syncStatus) {
      const normalized = String(syncStatus || '').toLowerCase();
      if (
        normalized.includes('ok') ||
        normalized.includes('synced') ||
        normalized.includes('success') ||
        normalized.includes('mapped')
      ) {
        return 'positive';
      }
      if (normalized.includes('error') || normalized.includes('failed') || normalized.includes('missing')) {
        return 'negative';
      }
      if (normalized.includes('pending') || normalized.includes('queued') || normalized.includes('running')) {
        return 'warning';
      }
      return 'grey-7';
    },
    resetVisibleRecordings() {
      this.visibleRecordingsCount = DVR_PAGE_SIZE;
      this.$nextTick(() => {
        if (this.$refs.recordingsInfiniteRef) {
          this.$refs.recordingsInfiniteRef.reset();
        }
      });
    },
    resetVisibleRules() {
      this.visibleRulesCount = DVR_PAGE_SIZE;
      this.$nextTick(() => {
        if (this.$refs.rulesInfiniteRef) {
          this.$refs.rulesInfiniteRef.reset();
        }
      });
    },
    onRecordingsLoad(index, done) {
      if (!this.hasMoreRecordings) {
        done(true);
        return;
      }
      this.visibleRecordingsCount += DVR_PAGE_SIZE;
      done(this.visibleRecordingsCount >= this.filteredSortedRecordings.length);
    },
    onRulesLoad(index, done) {
      if (!this.hasMoreRules) {
        done(true);
        return;
      }
      this.visibleRulesCount += DVR_PAGE_SIZE;
      done(this.visibleRulesCount >= this.filteredSortedRules.length);
    },
    async loadRecordings() {
      this.loadingRecordings = true;
      try {
        const response = await axios.get('/tic-api/recordings');
        this.recordings = response.data.data || [];
        this.resetVisibleRecordings();
      } finally {
        this.loadingRecordings = false;
      }
    },
    async loadRules() {
      this.loadingRules = true;
      try {
        const response = await axios.get('/tic-api/recording-rules');
        this.rules = response.data.data || [];
        this.resetVisibleRules();
      } finally {
        this.loadingRules = false;
      }
    },
    async loadChannels() {
      const response = await axios.get('/tic-api/channels/basic');
      this.channels = response.data.data || [];
    },
    async loadRecordingProfiles() {
      try {
        const response = await axios.get('/tic-api/recording-profiles');
        this.recordingProfiles = response.data.data || [];
      } catch {
        this.recordingProfiles = [{key: 'default', name: 'Default', pathname: '%F_%R $u$n.$x'}];
      }
      if (!this.scheduleForm.recording_profile_key || !this.recordingProfileOptions.some((item) => item.value === this.scheduleForm.recording_profile_key)) {
        this.scheduleForm.recording_profile_key = this.defaultRecordingProfileKey;
      }
      if (!this.ruleForm.recording_profile_key || !this.recordingProfileOptions.some((item) => item.value === this.ruleForm.recording_profile_key)) {
        this.ruleForm.recording_profile_key = this.defaultRecordingProfileKey;
      }
    },
    async refreshAll() {
      await Promise.all([this.loadRecordings(), this.loadRules(), this.loadChannels(), this.loadRecordingProfiles()]);
    },
    startDvrAutoRefreshTimer() {
      if (this.dvrRefreshTimerId) {
        clearInterval(this.dvrRefreshTimerId);
      }
      this.dvrRefreshTimerId = setInterval(() => {
        this.refreshAll().catch(() => {
          // Keep timer alive if one refresh cycle fails.
        });
      }, 60 * 1000);
    },
    recordingActions(recording) {
      const actions = [];
      if (this.canStopRecording(recording)) {
        actions.push({id: 'stop', icon: 'stop', label: 'Stop recording', color: 'warning'});
      } else if (this.canCancelRecording(recording)) {
        actions.push({id: 'cancel', icon: 'cancel', label: 'Cancel recording', color: 'warning'});
      }
      actions.push({id: 'delete', icon: 'delete', label: 'Delete recording', color: 'negative'});
      if (this.isRecordingPlayable(recording)) {
        actions.push({id: 'play', icon: 'play_arrow', label: 'Play recording', color: 'primary'});
      }
      return actions;
    },
    recordingCardColors(recording) {
      const group = this.recordingStatusGroup(recording?.status);
      if (group === 'successful') {
        return {
          accent: 'var(--tic-list-card-healthy-border)',
          surface: 'var(--tic-list-card-healthy-bg)',
          header: 'var(--tic-list-card-healthy-header)',
        };
      }
      if (group === 'failed') {
        return {
          accent: 'var(--tic-list-card-error-border)',
          surface: 'var(--tic-list-card-error-bg)',
          header: 'var(--tic-list-card-error-header)',
        };
      }
      return {};
    },
    recordingCardProps(recording) {
      const colors = this.recordingCardColors(recording);
      const props = {};
      if (colors.accent) props.accentColor = colors.accent;
      if (colors.surface) props.surfaceColor = colors.surface;
      if (colors.header) props.headerColor = colors.header;
      return props;
    },
    recordingStatusGroup(status) {
      const normalized = String(status || '').toLowerCase();
      if (normalized.includes('recording') || normalized.includes('running') || normalized.includes('in_progress')) {
        return 'upcoming';
      }
      if (normalized.includes('scheduled') || normalized.includes('queued') || normalized.includes('upcoming')) {
        return 'upcoming';
      }
      if (
        normalized.includes('failed') ||
        normalized.includes('error') ||
        normalized.includes('missing') ||
        normalized.includes('abort') ||
        normalized.includes('cancel')
      ) {
        return 'failed';
      }
      if (
        normalized.includes('completed') ||
        normalized.includes('finished') ||
        normalized.includes('done') ||
        normalized.includes('success') ||
        normalized.includes('recorded') ||
        normalized.includes('ok')
      ) {
        return 'successful';
      }
      return 'upcoming';
    },
    handleRecordingAction(action, recording) {
      if (action.id === 'play') {
        this.playRecording(recording);
      }
      if (action.id === 'stop') {
        this.stopRecording(recording.id);
      }
      if (action.id === 'cancel') {
        this.cancelScheduledRecording(recording.id);
      }
      if (action.id === 'delete') {
        this.confirmDeleteRecording(recording);
      }
    },
    ruleActions() {
      return [
        {id: 'edit', icon: 'edit', label: 'Edit rule', color: 'primary'},
        {id: 'delete', icon: 'delete', label: 'Delete rule', color: 'negative'},
      ];
    },
    handleRuleAction(action, rule) {
      if (action.id === 'edit') {
        this.openRuleDialog(rule);
      }
      if (action.id === 'delete') {
        this.confirmDeleteRule(rule);
      }
    },
    isRecordingPlayable(recording) {
      const status = String(recording?.status || '').toLowerCase();
      return 'completed finished done success recorded'.split(' ').some((token) => status.includes(token));
    },
    canCancelRecording(recording) {
      const status = String(recording?.status || '').toLowerCase();
      return status === 'scheduled' || status.includes('scheduled');
    },
    canStopRecording(recording) {
      const status = String(recording?.status || '').toLowerCase();
      return status === 'recording' || status.includes('running') || status.includes('in_progress');
    },
    playRecording(recording) {
      if (!recording?.id) return;
      this.videoStore.showPlayer({
        url: `/tic-api/recordings/${recording.id}/hls.m3u8`,
        title: recording.title || 'Recording',
        type: 'application/x-mpegURL',
      });
    },
    async cancelScheduledRecording(id) {
      try {
        await axios.delete(`/tic-api/recordings/${id}`);
        this.$q.notify({color: 'positive', message: 'Recording canceled'});
        await this.loadRecordings();
      } catch {
        this.$q.notify({color: 'negative', message: 'Failed to cancel recording'});
      }
    },
    async stopRecording(id) {
      try {
        await axios.post(`/tic-api/recordings/${id}/stop`);
        this.$q.notify({color: 'positive', message: 'Recording stopped'});
        await this.loadRecordings();
      } catch {
        this.$q.notify({color: 'negative', message: 'Failed to stop recording'});
      }
    },
    async deleteRecording(id) {
      try {
        await axios.delete(`/tic-api/recordings/${id}`);
        this.$q.notify({color: 'positive', message: 'Recording deleted'});
        await this.loadRecordings();
      } catch {
        this.$q.notify({color: 'negative', message: 'Failed to delete recording'});
      }
    },
    confirmDeleteRecording(recording) {
      this.$q.dialog({
        component: TicConfirmDialog,
        componentProps: {
          title: 'Delete recording?',
          message: 'This removes the recording from Headendarr and TVHeadend where available.',
          icon: 'delete_forever',
          iconColor: 'negative',
          confirmLabel: 'Delete',
          confirmIcon: 'delete',
          confirmColor: 'negative',
        },
      }).onOk(async () => {
        await this.deleteRecording(recording.id);
      });
    },
    openRuleDialog(rule = null) {
      if (rule) {
        this.isEditingRule = true;
        this.activeRuleId = rule.id;
        this.ruleForm = {
          channel_id: rule.channel_id ?? null,
          title_match: rule.title_match ?? '',
          lookahead_days: rule.lookahead_days ?? 7,
          recording_profile_key: rule.recording_profile_key || this.defaultRecordingProfileKey,
        };
      } else {
        this.isEditingRule = false;
        this.activeRuleId = null;
        this.ruleForm = {
          channel_id: null,
          title_match: '',
          lookahead_days: 7,
          recording_profile_key: this.defaultRecordingProfileKey,
        };
      }
      this.showRuleDialog = true;
    },
    async deleteRule(id) {
      try {
        await axios.delete(`/tic-api/recording-rules/${id}`);
        this.$q.notify({color: 'positive', message: 'Recording rule deleted'});
        await this.loadRules();
      } catch {
        this.$q.notify({color: 'negative', message: 'Failed to delete recording rule'});
      }
    },
    confirmDeleteRule(rule) {
      this.$q.dialog({
        component: TicConfirmDialog,
        componentProps: {
          title: 'Delete recording rule?',
          message: 'This removes the recurring rule. Existing scheduled recordings remain unchanged.',
          icon: 'delete_forever',
          iconColor: 'negative',
          confirmLabel: 'Delete',
          confirmIcon: 'delete',
          confirmColor: 'negative',
        },
      }).onOk(async () => {
        await this.deleteRule(rule.id);
      });
    },
    onScheduleDialogAction(action) {
      if (action.id === 'save') {
        this.submitSchedule();
      }
    },
    async submitSchedule() {
      const startTs = Math.floor(new Date(this.scheduleForm.start).getTime() / 1000);
      const stopTs = Math.floor(new Date(this.scheduleForm.stop).getTime() / 1000);
      if (
        !this.scheduleForm.channel_id ||
        !this.scheduleForm.start ||
        !this.scheduleForm.stop ||
        Number.isNaN(startTs) ||
        Number.isNaN(stopTs)
      ) {
        this.$q.notify({color: 'negative', message: 'Channel, start, and stop are required'});
        return;
      }

      this.scheduleSaving = true;
      try {
        await axios.post('/tic-api/recordings', {
          channel_id: this.scheduleForm.channel_id,
          title: this.scheduleForm.title,
          start_ts: startTs,
          stop_ts: stopTs,
          recording_profile_key: this.scheduleForm.recording_profile_key || this.defaultRecordingProfileKey,
        });
        this.$q.notify({color: 'positive', message: 'Recording scheduled'});
        this.showScheduleDialog = false;
        this.scheduleForm = {
          channel_id: null,
          title: '',
          start: '',
          stop: '',
          recording_profile_key: this.defaultRecordingProfileKey,
        };
        await this.loadRecordings();
      } catch {
        this.$q.notify({color: 'negative', message: 'Failed to schedule recording'});
      } finally {
        this.scheduleSaving = false;
      }
    },
    onRuleDialogAction(action) {
      if (action.id === 'save') {
        this.submitRule();
      }
    },
    async submitRule() {
      this.ruleSaving = true;
      try {
        const payload = {
          channel_id: this.ruleForm.channel_id,
          title_match: this.ruleForm.title_match,
          lookahead_days: Number(this.ruleForm.lookahead_days || 7),
          recording_profile_key: this.ruleForm.recording_profile_key || this.defaultRecordingProfileKey,
        };
        if (this.isEditingRule && this.activeRuleId) {
          await axios.put(`/tic-api/recording-rules/${this.activeRuleId}`, payload);
          this.$q.notify({color: 'positive', message: 'Recording rule updated'});
        } else {
          await axios.post('/tic-api/recording-rules', payload);
          this.$q.notify({color: 'positive', message: 'Recording rule created'});
        }
        this.showRuleDialog = false;
        this.isEditingRule = false;
        this.activeRuleId = null;
        await this.loadRules();
      } catch {
        this.$q.notify({color: 'negative', message: 'Failed to save recording rule'});
      } finally {
        this.ruleSaving = false;
      }
    },
    clearRecordingFilterDraft() {
      this.filterDraft.status = null;
    },
    applyRecordingFilterDraft() {
      this.statusFilter = this.filterDraft.status;
      this.recordingsFilterDialogOpen = false;
    },
    clearRecordingSortDraft() {
      this.recordingsSortDraft = {sortBy: 'start_ts', sortDirection: 'desc'};
    },
    applyRecordingSortDraft() {
      this.recordingsSort = {...this.recordingsSortDraft};
      this.recordingsSortDialogOpen = false;
    },
    clearRuleSortDraft() {
      this.rulesSortDraft = {sortBy: 'title_match', sortDirection: 'asc'};
    },
    applyRuleSortDraft() {
      this.rulesSort = {...this.rulesSortDraft};
      this.rulesSortDialogOpen = false;
    },
    async pollRecordings() {
      if (this.pollLoopPromise) {
        return this.pollLoopPromise;
      }
      this.pollingActive = true;
      this.pollLoopPromise = (async () => {
      while (this.pollingActive) {
        const controller = new AbortController();
        this.pollAbortController = controller;
        try {
          const response = await axios.get('/tic-api/recordings/poll', {
            params: {
              wait: 1,
              timeout: 25,
            },
            signal: controller.signal,
          });
          if (!this.pollingActive) {
            break;
          }
          this.recordings = response.data.data || [];
        } catch (error) {
          const aborted = error?.code === 'ERR_CANCELED'
            || error?.name === 'CanceledError'
            || error?.name === 'AbortError';
          if (!this.pollingActive || aborted) {
            continue;
          }
          await new Promise((resolve) => setTimeout(resolve, 1000));
        } finally {
          if (this.pollAbortController === controller) {
            this.pollAbortController = null;
          }
        }
      }
      })().finally(() => {
        this.pollLoopPromise = null;
        this.pollAbortController = null;
      });
      return this.pollLoopPromise;
    },
  },
  async mounted() {
    this.filterDraft.status = this.statusFilter;
    this.recordingsSortDraft = {...this.recordingsSort};
    this.rulesSortDraft = {...this.rulesSort};
    await this.refreshAll();
    this.startDvrAutoRefreshTimer();
    this.pollRecordings();
  },
  beforeUnmount() {
    this.pollingActive = false;
    if (this.pollAbortController) {
      this.pollAbortController.abort();
      this.pollAbortController = null;
    }
    if (this.dvrRefreshTimerId) {
      clearInterval(this.dvrRefreshTimerId);
      this.dvrRefreshTimerId = null;
    }
  },
});
</script>

<style scoped>
.dvr-tabs-bar {
  background: transparent;
}

.dvr-tabs {
  background: transparent !important;
}

.dvr-tabs :deep(.dvr-tabs-content) {
  background: var(--guide-channel-bg);
}

.dvr-tab-panels {
  background: transparent !important;
}

.dvr-tab-panels :deep(.q-tab-panel) {
  background: transparent !important;
}

.dvr-list-item {
  align-items: flex-start;
}

.dvr-card-title {
  line-height: 1.3;
}

.dvr-meta-grid {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 8px 16px;
  margin-top: 8px;
}

.dvr-meta-field {
  display: flex;
  flex-direction: column;
  gap: 2px;
  min-width: 0;
}

.dvr-meta-field :deep(.q-chip) {
  align-self: flex-start;
}

.dvr-status-chip :deep(.q-chip__content) {
  padding-left: 8px;
  padding-right: 8px;
}

.dvr-list {
  border: none;
}

.dvr-list-item {
  margin-top: 4px;
  margin-bottom: 4px;
  border-bottom: 1px solid var(--q-separator-color);
  padding-left: 0;
  padding-right: 0;
  padding-top: 0;
  padding-bottom: 0;
}

.dvr-list .dvr-list-item:last-child {
  border-bottom: none;
}

@media (max-width: 599px) {
  .dvr-list-item {
    border-bottom: 2px solid var(--q-separator-color);
  }
}
</style>
