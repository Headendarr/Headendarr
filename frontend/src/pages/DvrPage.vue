<template>
  <q-page padding>
    <q-card flat>
      <q-card-section class="row items-center q-gutter-md">
        <div class="text-h5">DVR</div>
        <q-space />
        <q-btn color="primary" label="Refresh" @click="refreshAll" />
      </q-card-section>

      <q-tabs v-model="tab" class="text-primary">
        <q-tab name="recordings" label="Recordings" />
        <q-tab name="rules" label="Recording Rules" />
      </q-tabs>

      <q-separator />

      <q-tab-panels v-model="tab" animated>
        <q-tab-panel name="recordings">
          <q-btn
            color="primary"
            label="Schedule Recording"
            class="q-mb-md"
            @click="showScheduleDialog = true"
          />
          <q-table
            :rows="recordings"
            :columns="recordingColumns"
            row-key="id"
            flat
            dense
          >
            <template v-slot:body-cell-actions="props">
              <q-td :props="props">
                <q-btn
                  dense
                  flat
                  icon="cancel"
                  color="negative"
                  @click="cancelRecording(props.row.id)"
                />
              </q-td>
            </template>
          </q-table>
        </q-tab-panel>

        <q-tab-panel name="rules">
          <q-btn
            color="primary"
            label="Add Rule"
            class="q-mb-md"
            @click="showRuleDialog = true"
          />
          <q-table
            :rows="rules"
            :columns="ruleColumns"
            row-key="id"
            flat
            dense
          >
            <template v-slot:body-cell-actions="props">
              <q-td :props="props">
                <q-btn
                  dense
                  flat
                  icon="delete"
                  color="negative"
                  @click="deleteRule(props.row.id)"
                />
              </q-td>
            </template>
          </q-table>
        </q-tab-panel>
      </q-tab-panels>
    </q-card>

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
          <div class="text-h6">Create Recording Rule</div>
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
  </q-page>
</template>

<script>
import {defineComponent, ref, onMounted, computed} from 'vue';
import axios from 'axios';

export default defineComponent({
  name: 'DvrPage',
  setup() {
    const tab = ref('recordings');
    const recordings = ref([]);
    const rules = ref([]);
    const channels = ref([]);
    const showScheduleDialog = ref(false);
    const showRuleDialog = ref(false);
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
      {name: 'sync_status', label: 'Sync', field: 'sync_status', align: 'left'},
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
      }))
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
      await axios.post(`/tic-api/recordings/${id}/cancel`);
      await loadRecordings();
    };

    const submitSchedule = async () => {
      const startTs = Math.floor(new Date(scheduleForm.value.start).getTime() / 1000);
      const stopTs = Math.floor(new Date(scheduleForm.value.stop).getTime() / 1000);
      await axios.post('/tic-api/recordings', {
        channel_id: scheduleForm.value.channel_id,
        title: scheduleForm.value.title,
        start_ts: startTs,
        stop_ts: stopTs,
      });
      showScheduleDialog.value = false;
      await loadRecordings();
    };

    const submitRule = async () => {
      await axios.post('/tic-api/recording-rules', {
        channel_id: ruleForm.value.channel_id,
        title_match: ruleForm.value.title_match,
        lookahead_days: ruleForm.value.lookahead_days,
      });
      showRuleDialog.value = false;
      await loadRules();
    };

    const deleteRule = async (id) => {
      await axios.delete(`/tic-api/recording-rules/${id}`);
      await loadRules();
    };

    onMounted(refreshAll);

    return {
      tab,
      recordings,
      rules,
      recordingColumns,
      ruleColumns,
      showScheduleDialog,
      showRuleDialog,
      scheduleForm,
      ruleForm,
      channelOptions,
      refreshAll,
      cancelRecording,
      submitSchedule,
      submitRule,
      deleteRule,
    };
  },
});
</script>
