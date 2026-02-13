<template>
  <q-page>

    <div class="q-pa-md">

      <div class="row">
        <div :class="uiStore.showHelp && !$q.screen.lt.md ? 'col-sm-7 col-md-8 help-main' : 'col-12 help-main help-main--full'">

          <q-card flat>
            <q-card-section :class="$q.platform.is.mobile ? 'q-px-none' : ''">
              <div class="row items-center q-gutter-md">
                <div class="text-h5">DVR</div>
                <q-space />
                <q-btn color="primary" label="Refresh" @click="refreshAll" />
              </div>
            </q-card-section>

            <q-tabs v-model="tab" class="text-primary">
              <q-tab name="recordings" label="Recordings" />
              <q-tab name="rules" label="Recording Rules" />
            </q-tabs>

            <q-separator />

            <q-tab-panels v-model="tab" animated>
              <q-tab-panel name="recordings">
                <div class="row items-center q-gutter-md q-mb-md">
                  <q-btn
                    color="primary"
                    label="Schedule Recording"
                    @click="showScheduleDialog = true"
                  />
                  <q-select
                    v-model="statusFilter"
                    :options="statusOptions"
                    label="Status Filter"
                    dense
                    outlined
                    clearable
                    emit-value
                    map-options
                    style="min-width: 220px;"
                  />
                  <q-input
                    v-model="recordingsSearch"
                    dense
                    outlined
                    clearable
                    debounce="200"
                    placeholder="Search recordings"
                    style="min-width: 240px;"
                  />
                </div>
                <q-table
                  :rows="filteredRecordings"
                  :columns="recordingColumns"
                  row-key="id"
                  flat
                  dense
                  :pagination="recordingsPagination"
                  @update:pagination="onRecordingsPagination"
                >
                  <template v-slot:body-cell-actions="props">
                    <q-td :props="props">
                      <q-btn
                        v-if="isRecordingPlayable(props.row)"
                        dense
                        flat
                        icon="play_arrow"
                        color="primary"
                        @click="playRecording(props.row)"
                      >
                        <q-tooltip class="bg-white text-primary">Play recording</q-tooltip>
                      </q-btn>
                      <q-btn
                        v-if="canCancelRecording(props.row)"
                        dense
                        flat
                        icon="cancel"
                        color="negative"
                        @click="cancelRecording(props.row.id)"
                      >
                        <q-tooltip class="bg-white text-primary">Cancel recording</q-tooltip>
                      </q-btn>
                      <q-btn
                        dense
                        flat
                        icon="delete"
                        color="negative"
                        @click="confirmDeleteRecording(props.row)"
                      >
                        <q-tooltip class="bg-white text-primary">Delete recording</q-tooltip>
                      </q-btn>
                    </q-td>
                  </template>
                </q-table>
              </q-tab-panel>

              <q-tab-panel name="rules">
                <div class="row items-center q-gutter-md q-mb-md">
                  <q-btn
                    color="primary"
                    label="Add Rule"
                    @click="openRuleDialog()"
                  />
                  <q-input
                    v-model="rulesSearch"
                    dense
                    outlined
                    clearable
                    debounce="200"
                    placeholder="Search rules"
                    style="min-width: 240px;"
                  />
                </div>
                <q-table
                  :rows="filteredRules"
                  :columns="ruleColumns"
                  row-key="id"
                  flat
                  dense
                  :pagination="rulesPagination"
                  @update:pagination="onRulesPagination"
                >
                  <template v-slot:body-cell-actions="props">
                    <q-td :props="props">
                      <q-btn
                        dense
                        flat
                        icon="edit"
                        color="primary"
                        @click="openRuleDialog(props.row)"
                      >
                        <q-tooltip class="bg-white text-primary">Edit rule</q-tooltip>
                      </q-btn>
                      <q-btn
                        dense
                        flat
                        icon="delete"
                        color="negative"
                        @click="confirmDeleteRule(props.row)"
                      >
                        <q-tooltip class="bg-white text-primary">Delete rule</q-tooltip>
                      </q-btn>
                    </q-td>
                  </template>
                </q-table>
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
                        1. Use <b>Schedule Recording</b> to create a one-time recording for a channel and time
                        window.
                      </q-item-label>
                    </q-item-section>
                  </q-item>
                  <q-item>
                    <q-item-section>
                      <q-item-label>
                        2. Use <b>Recording Rules</b> to create recurring rules that automatically schedule future
                        recordings by title match.
                      </q-item-label>
                    </q-item-section>
                  </q-item>
                  <q-item>
                    <q-item-section>
                      <q-item-label>
                        3. After creating a recording or rule, check the <b>Sync</b> status to confirm it has been
                        pushed to TVHeadend.
                      </q-item-label>
                    </q-item-section>
                  </q-item>

            </q-list>
          </q-card-section>
          <q-card-section>
                <div class="text-h5 q-mb-none">Notes:</div>
                <q-list>

                  <q-separator inset spaced />

                  <q-item>
                    <q-item-section>
                      <q-item-label>
                        Recordings are queued and synced to TVHeadend in the background. TVHeadend performs the
                        actual
                        recording. If a sync fails, click <b>Refresh</b> to reload the current status.
                      </q-item-label>
                    </q-item-section>
                  </q-item>

            </q-list>
          </q-card-section>
        </TicResponsiveHelp>
      </div>

      <q-dialog v-model="showScheduleDialog">
        <q-card style="width: 520px; max-width: 95vw;">
          <q-card-section class="bg-primary text-white">
            <div class="text-h6">Schedule Recording</div>
          </q-card-section>
          <q-card-section>
            <q-select
              v-model="scheduleForm.channel_id"
              :options="channelOptions"
              label="Channel"
              emit-value
              map-options
              outlined
            />
            <q-input
              v-model="scheduleForm.title"
              label="Title"
              outlined
              class="q-mt-md"
            />
            <q-input
              v-model="scheduleForm.start"
              type="datetime-local"
              label="Start"
              outlined
              class="q-mt-md"
            />
            <q-input
              v-model="scheduleForm.stop"
              type="datetime-local"
              label="Stop"
              outlined
              class="q-mt-md"
            />
          </q-card-section>
          <q-card-actions align="right">
            <q-btn flat label="Cancel" v-close-popup />
            <q-btn color="primary" label="Save" @click="submitSchedule" />
          </q-card-actions>
        </q-card>
      </q-dialog>

      <q-dialog v-model="showRuleDialog">
        <q-card style="width: 520px; max-width: 95vw;">
          <q-card-section class="bg-primary text-white">
            <div class="text-h6">{{ isEditingRule ? 'Edit Recording Rule' : 'Create Recording Rule' }}</div>
          </q-card-section>
          <q-card-section>
            <q-select
              v-model="ruleForm.channel_id"
              :options="channelOptions"
              label="Channel"
              emit-value
              map-options
              outlined
            />
            <q-input
              v-model="ruleForm.title_match"
              label="Title Match"
              outlined
              class="q-mt-md"
            />
            <q-input
              v-model.number="ruleForm.lookahead_days"
              type="number"
              label="Lookahead Days"
              outlined
              class="q-mt-md"
            />
          </q-card-section>
          <q-card-actions align="right">
            <q-btn flat label="Cancel" v-close-popup />
            <q-btn color="primary" label="Save" @click="submitRule" />
          </q-card-actions>
        </q-card>
      </q-dialog>
    </div>
  </q-page>
</template>

<script>
import {defineComponent, ref, onMounted, onBeforeUnmount, computed} from 'vue';
import axios from 'axios';
import {useUiStore} from 'stores/ui';
import {useVideoStore} from 'stores/video';
import {useQuasar} from 'quasar';
import {TicResponsiveHelp} from 'components/ui';

export default defineComponent({
  name: 'DvrPage',
  components: {
    TicResponsiveHelp,
  },
  setup() {
    const videoStore = useVideoStore();
    const $q = useQuasar();
    const tab = ref('recordings');
    const recordings = ref([]);
    const rules = ref([]);
    const channels = ref([]);
    const showScheduleDialog = ref(false);
    const showRuleDialog = ref(false);
    const isEditingRule = ref(false);
    const activeRuleId = ref(null);
    const recordingsPagination = ref({rowsPerPage: 25});
    const rulesPagination = ref({rowsPerPage: 25});
    const statusFilter = ref(null);
    const recordingsSearch = ref('');
    const rulesSearch = ref('');
    const scheduleForm = ref({
      channel_id: null,
      title: '',
      start: '',
      stop: '',
    });
    const ruleForm = ref({
      channel_id: null,
      title_match: '',
      lookahead_days: 7,
    });

    const recordingColumns = [
      {name: 'channel_name', label: 'Channel', field: 'channel_name', align: 'left'},
      {name: 'title', label: 'Title', field: 'title', align: 'left'},
      {name: 'start_ts', label: 'Start', field: row => formatTs(row.start_ts), align: 'left'},
      {name: 'stop_ts', label: 'Stop', field: row => formatTs(row.stop_ts), align: 'left'},
      {name: 'status', label: 'Status', field: 'status', align: 'left'},
      {name: 'sync_status', label: 'TVH Sync Status', field: 'sync_status', align: 'left'},
      {name: 'actions', label: '', field: 'actions', align: 'right'},
    ];

    const ruleColumns = [
      {name: 'channel_name', label: 'Channel', field: 'channel_name', align: 'left'},
      {name: 'title_match', label: 'Title Match', field: 'title_match', align: 'left'},
      {name: 'lookahead_days', label: 'Lookahead', field: 'lookahead_days', align: 'left'},
      {name: 'actions', label: '', field: 'actions', align: 'right'},
    ];

    const channelOptions = computed(() =>
      channels.value.map((channel) => ({
        label: channel.name,
        value: channel.id,
      })),
    );

    const formatTs = (ts) => {
      if (!ts) return '-';
      return new Date(ts * 1000).toLocaleString();
    };

    const loadRecordings = async () => {
      const response = await axios.get('/tic-api/recordings');
      recordings.value = response.data.data || [];
    };

    const loadRules = async () => {
      const response = await axios.get('/tic-api/recording-rules');
      rules.value = response.data.data || [];
    };

    const loadChannels = async () => {
      const response = await axios.get('/tic-api/channels/basic');
      channels.value = response.data.data || [];
    };

    const refreshAll = async () => {
      await Promise.all([loadRecordings(), loadRules(), loadChannels()]);
    };

    const cancelRecording = async (id) => {
      try {
        await axios.post(`/tic-api/recordings/${id}/cancel`);
        $q.notify({color: 'positive', message: 'Recording canceled'});
        await loadRecordings();
      } catch (error) {
        $q.notify({color: 'negative', message: 'Failed to cancel recording'});
      }
    };

    const deleteRecording = async (id) => {
      try {
        await axios.delete(`/tic-api/recordings/${id}`);
        $q.notify({color: 'positive', message: 'Recording deleted'});
        await loadRecordings();
      } catch (error) {
        $q.notify({color: 'negative', message: 'Failed to delete recording'});
      }
    };

    const submitSchedule = async () => {
      const startTs = Math.floor(new Date(scheduleForm.value.start).getTime() / 1000);
      const stopTs = Math.floor(new Date(scheduleForm.value.stop).getTime() / 1000);
      try {
        await axios.post('/tic-api/recordings', {
          channel_id: scheduleForm.value.channel_id,
          title: scheduleForm.value.title,
          start_ts: startTs,
          stop_ts: stopTs,
        });
        $q.notify({color: 'positive', message: 'Recording scheduled'});
        showScheduleDialog.value = false;
        await loadRecordings();
      } catch (error) {
        $q.notify({color: 'negative', message: 'Failed to schedule recording'});
      }
    };

    const submitRule = async () => {
      try {
        if (isEditingRule.value && activeRuleId.value) {
          await axios.put(`/tic-api/recording-rules/${activeRuleId.value}`, {
            channel_id: ruleForm.value.channel_id,
            title_match: ruleForm.value.title_match,
            lookahead_days: ruleForm.value.lookahead_days,
          });
          $q.notify({color: 'positive', message: 'Recording rule updated'});
        } else {
          await axios.post('/tic-api/recording-rules', {
            channel_id: ruleForm.value.channel_id,
            title_match: ruleForm.value.title_match,
            lookahead_days: ruleForm.value.lookahead_days,
          });
          $q.notify({color: 'positive', message: 'Recording rule created'});
        }
        showRuleDialog.value = false;
        isEditingRule.value = false;
        activeRuleId.value = null;
        await loadRules();
      } catch (error) {
        $q.notify({color: 'negative', message: 'Failed to save recording rule'});
      }
    };

    const deleteRule = async (id) => {
      try {
        await axios.delete(`/tic-api/recording-rules/${id}`);
        $q.notify({color: 'positive', message: 'Recording rule deleted'});
        await loadRules();
      } catch (error) {
        $q.notify({color: 'negative', message: 'Failed to delete recording rule'});
      }
    };

    const completedStates = new Set(['completed', 'finished', 'done', 'success', 'recorded']);
    const activeStates = new Set(['scheduled', 'recording', 'running', 'in_progress']);
    const statusOptions = [
      {label: 'All', value: null},
      {label: 'Scheduled', value: 'scheduled'},
      {label: 'Recording', value: 'recording'},
      {label: 'Completed', value: 'completed'},
      {label: 'Canceled', value: 'canceled'},
      {label: 'Deleted', value: 'deleted'},
      {label: 'Failed', value: 'failed'},
    ];

    const filteredRecordings = computed(() => {
      const filter = statusFilter.value;
      const normalized = filter ? String(filter).toLowerCase() : null;
      const search = recordingsSearch.value.trim().toLowerCase();
      return (recordings.value || []).filter((rec) => {
        const status = String(rec.status || '').toLowerCase();
        const channelName = String(rec.channel_name || '').toLowerCase();
        const title = String(rec.title || '').toLowerCase();
        const matchesSearch = !search || channelName.includes(search) || title.includes(search);
        if (!matchesSearch) return false;
        if (!normalized) return true;
        if (normalized === 'recording') {
          return status === 'recording' || status.includes('running') || status.includes('in_progress');
        }
        if (normalized === 'completed') {
          return (
            status.includes('completed') ||
            status.includes('finished') ||
            status.includes('done') ||
            status.includes('success') ||
            status.includes('recorded') ||
            status.includes('ok')
          );
        }
        return status.includes(normalized);
      });
    });

    const filteredRules = computed(() => {
      const search = rulesSearch.value.trim().toLowerCase();
      if (!search) return rules.value;
      return (rules.value || []).filter((rule) => {
        const channelName = String(rule.channel_name || '').toLowerCase();
        const titleMatch = String(rule.title_match || '').toLowerCase();
        return channelName.includes(search) || titleMatch.includes(search);
      });
    });

    const isRecordingPlayable = (rec) => {
      if (!rec || !rec.status) return false;
      const normalized = String(rec.status).toLowerCase().trim();
      if (completedStates.has(normalized)) return true;
      return normalized.includes('completed') || normalized.includes('finished') || normalized.includes('success');
    };

    const canCancelRecording = (rec) => {
      if (!rec || !rec.status) return false;
      return activeStates.has(String(rec.status).toLowerCase());
    };

    const playRecording = (rec) => {
      if (!rec?.id) return;
      videoStore.showPlayer({
        url: `/tic-api/recordings/${rec.id}/hls.m3u8`,
        title: rec.title || 'Recording',
        type: 'application/x-mpegURL',
      });
    };

    const confirmDeleteRecording = (rec) => {
      $q.dialog({
        title: 'Delete recording?',
        message: 'This will remove the recording from TIC and delete it from TVHeadend if available.',
        cancel: true,
        persistent: true,
      }).onOk(async () => {
        await deleteRecording(rec.id);
      });
    };

    const confirmDeleteRule = (rule) => {
      $q.dialog({
        title: 'Delete recording rule?',
        message: 'This will remove the rule and will not delete already scheduled recordings.',
        cancel: true,
        persistent: true,
      }).onOk(async () => {
        await deleteRule(rule.id);
      });
    };

    const openRuleDialog = (rule = null) => {
      if (rule) {
        isEditingRule.value = true;
        activeRuleId.value = rule.id;
        ruleForm.value = {
          channel_id: rule.channel_id ?? null,
          title_match: rule.title_match ?? '',
          lookahead_days: rule.lookahead_days ?? 7,
        };
      } else {
        isEditingRule.value = false;
        activeRuleId.value = null;
        ruleForm.value = {
          channel_id: null,
          title_match: '',
          lookahead_days: 7,
        };
      }
      showRuleDialog.value = true;
    };

    let pollingActive = true;

    const pollRecordings = async () => {
      while (pollingActive) {
        try {
          const response = await axios.get('/tic-api/recordings/poll', {
            params: {
              wait: 1,
              timeout: 25,
            },
          });
          recordings.value = response.data.data || [];
        } catch (error) {
          await new Promise((resolve) => setTimeout(resolve, 1000));
        }
      }
    };

    onMounted(async () => {
      const storedRecordings = localStorage.getItem('tic_dvr_recordings_rows');
      const storedRules = localStorage.getItem('tic_dvr_rules_rows');
      if (storedRecordings) {
        recordingsPagination.value = {rowsPerPage: Number(storedRecordings) || 25};
      }
      if (storedRules) {
        rulesPagination.value = {rowsPerPage: Number(storedRules) || 25};
      }
      await refreshAll();
      pollRecordings();
    });

    onBeforeUnmount(() => {
      pollingActive = false;
    });

    return {
      uiStore: useUiStore(),
      tab,
      recordings,
      rules,
      recordingColumns,
      ruleColumns,
      showScheduleDialog,
      showRuleDialog,
      isEditingRule,
      openRuleDialog,
      scheduleForm,
      ruleForm,
      channelOptions,
      recordingsPagination,
      rulesPagination,
      statusFilter,
      statusOptions,
      recordingsSearch,
      rulesSearch,
      filteredRecordings,
      filteredRules,
      refreshAll,
      cancelRecording,
      deleteRecording,
      submitSchedule,
      submitRule,
      deleteRule,
      isRecordingPlayable,
      canCancelRecording,
      playRecording,
      confirmDeleteRecording,
      confirmDeleteRule,
      onRecordingsPagination: (p) => {
        recordingsPagination.value = p;
        if (p?.rowsPerPage) {
          localStorage.setItem('tic_dvr_recordings_rows', String(p.rowsPerPage));
        }
      },
      onRulesPagination: (p) => {
        rulesPagination.value = p;
        if (p?.rowsPerPage) {
          localStorage.setItem('tic_dvr_rules_rows', String(p.rowsPerPage));
        }
      },
    };
  },
});
</script>
