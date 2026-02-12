<template>
  <q-dialog ref="channelIssuesDialogRef" @hide="onDialogHide">
    <q-card style="min-width: 520px; max-width: 760px;">
      <q-card-section class="bg-card-head">
        <div class="row items-center no-wrap">
          <div class="col">
            <div class="text-h6 text-blue-10">Needs Attention</div>
            <div class="text-caption text-grey-7">
              Review the issues below and follow the suggested steps to resolve them.
            </div>
          </div>
          <div class="col-auto">
            <q-btn dense round flat icon="close" v-close-popup />
          </div>
        </div>
      </q-card-section>

      <q-separator />

      <q-card-section>
        <div class="q-gutter-md">
          <div
            v-for="issue in issueList"
            :key="issue.key"
            class="issue-block"
          >
            <div class="text-subtitle2 text-weight-medium issue-title">
              <q-icon name="warning" class="issue-title__icon" />
              {{ issue.title }}
            </div>
            <div class="text-body2 text-grey-8 q-mt-xs">
              {{ issue.description }}
            </div>
            <div v-if="issue.streams && issue.streams.length" class="q-mt-sm">
              <div class="text-caption text-grey-7">Affected stream(s)</div>
              <div class="text-body2">
                <div v-for="stream in issue.streams" :key="stream.label">
                  {{ stream.label }}
                </div>
              </div>
            </div>
            <div v-if="issue.actions && issue.actions.length" class="q-gutter-sm q-mt-sm">
              <q-btn
                v-for="action in issue.actions"
                :key="action.label"
                :label="action.label"
                color="primary"
                outline
                @click="action.handler"
              />
            </div>
          </div>
        </div>
      </q-card-section>
    </q-card>
  </q-dialog>
</template>

<script>
import {ref} from 'vue';
import axios from 'axios';

export default {
  name: 'ChannelIssuesDialog',
  props: {
    channel: {
      type: Object,
      required: true,
    },
    issues: {
      type: Array,
      default: () => [],
    },
  },
  emits: ['ok', 'hide', 'open-settings'],
  data() {
    return {
      didEmitOk: ref(false),
      syncing: ref(false),
    };
  },
  computed: {
    issueList() {
      const actions = {
        openSettings: {
          label: 'Open Channel Settings',
          handler: this.openChannelSettings,
        },
        syncTVHeadend: {
          label: 'Sync TVHeadend Now',
          handler: this.syncTVHeadend,
        },
      };
      const definitions = {
        no_sources: {
          title: 'No streams',
          description:
            'This channel has no streams linked. Add a stream to enable playback.',
          actions: [actions.openSettings],
        },
        all_sources_disabled: {
          title: 'All streams disabled',
          description:
            'All linked streams are disabled or missing. Enable a stream, replace it, or remove it if it is no longer needed.',
          actions: [actions.openSettings],
        },
        missing_tvh_mux: {
          title: 'Missing stream in TVHeadend',
          description:
            'TVHeadend does not have a stream (mux) for one or more linked streams. This can happen if the TVHeadend sync has not run or failed.',
          actions: [actions.openSettings, actions.syncTVHeadend],
        },
        tvh_mux_failed: {
          title: 'TVHeadend stream failed',
          description:
            'TVHeadend tried to scan or tune a stream (mux) and failed. The stream might be offline, geo-blocked, or the source credentials could be invalid.',
          actions: [actions.openSettings, actions.syncTVHeadend],
          streams: this.failedStreams,
        },
      };
      return (this.issues || []).map(
        (issue) => ({key: issue, ...(definitions[issue] || {title: issue, description: ''})})).
        filter((issue) => issue.title);
    },
    failedStreams() {
      const raw = this.channel?.status?.failed_streams || [];
      const labels = raw
        .map((entry) => {
          const name = entry?.stream_name || 'Unknown stream';
          const playlist = entry?.playlist_name;
          return playlist ? `${name} (${playlist})` : name;
        });
      return [...new Set(labels)].map(label => ({label}));
    },
  },
  methods: {
    show() {
      this.didEmitOk = false;
      this.$refs.channelIssuesDialogRef.show();
    },
    hide() {
      this.$refs.channelIssuesDialogRef.hide();
    },
    onDialogHide() {
      if (!this.didEmitOk) {
        this.$emit('ok', {refresh: false});
      }
      this.didEmitOk = false;
      this.$emit('hide');
    },
    openChannelSettings() {
      this.didEmitOk = true;
      this.$emit('ok', {openSettings: true, channelId: this.channel?.id});
      this.hide();
    },
    syncTVHeadend() {
      if (this.syncing) return;
      this.syncing = true;
      axios({
        method: 'POST',
        url: '/tic-api/channels/sync',
      }).then(() => {
        this.$q.notify({color: 'positive', message: 'TVHeadend sync queued'});
      }).catch(() => {
        this.$q.notify({color: 'negative', message: 'Failed to queue TVHeadend sync'});
      }).finally(() => {
        this.syncing = false;
      });
    },
  },
};
</script>

<style scoped>
.issue-block + .issue-block {
  padding-top: 12px;
  border-top: 1px solid rgba(0, 0, 0, 0.08);
}

.issue-title {
  display: flex;
  align-items: center;
  gap: 6px;
  color: #c96a00;
}

.issue-title__icon {
  font-size: 16px;
}
</style>
