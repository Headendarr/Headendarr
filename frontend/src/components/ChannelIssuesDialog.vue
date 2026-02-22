<template>
  <TicDialogPopup v-model="isOpen" title="Needs Attention" width="760px" max-width="96vw" @hide="onDialogHide">
    <template #default>
      <div class="text-caption text-grey-7 q-mb-md">
        Review the issues below and follow the suggested steps to resolve them.
      </div>
      <div class="q-gutter-md">
        <template v-for="(issue, index) in issueList" :key="issue.key">
          <div class="issue-block">
            <div class="text-subtitle2 text-weight-medium issue-title">
              <q-icon name="warning" class="issue-title__icon" />
              {{ issue.title }}
            </div>
            <div class="text-body2 text-grey-8 q-mt-xs">
              {{ issue.description }}
            </div>
            <div v-if="issue.key === 'channel_logo_unavailable' && logoIssueError" class="q-mt-sm">
              <div class="text-caption text-grey-7">Last error</div>
              <div class="issue-log-line">{{ logoIssueError }}</div>
            </div>

            <div v-if="hasIssueDetails(issue)" class="issue-actions q-mt-md">
              <div v-if="issue.streams && issue.streams.length">
                <div class="text-caption text-grey-7">Affected stream(s)</div>
                <div class="text-body2">
                  <div v-for="stream in issue.streams" :key="stream.label">
                    {{ stream.label }}
                  </div>
                </div>
              </div>
              <div v-if="issue.csoDetails && issue.csoDetails.length">
                <div class="text-caption text-grey-7">Latest CSO event details</div>
                <div class="text-body2">
                  <div v-for="detail in issue.csoDetails" :key="detail">
                    {{ detail }}
                  </div>
                </div>
              </div>

              <div v-if="issue.key === 'channel_logo_unavailable'" class="q-mt-md">
                <div class="text-caption text-grey-7 q-mb-xs">Suggested logos</div>
                <div v-if="loadingLogoSuggestions" class="text-caption text-grey-7">Loading suggestions...</div>
                <div v-else-if="!logoSuggestions.length" class="text-caption text-grey-7">
                  No suggested logos found from linked streams/EPG.
                </div>
                <q-list v-else bordered separator class="rounded-borders">
                  <q-item v-for="suggestion in logoSuggestions" :key="suggestion.url">
                    <q-item-section avatar>
                      <div class="logo-preview">
                        <q-img :src="suggestion.url" class="logo-preview__img" fit="contain" />
                      </div>
                    </q-item-section>
                    <q-item-section>
                      <q-item-label>{{ suggestion.label || suggestion.source }}</q-item-label>
                      <q-item-label caption lines="1">{{ suggestion.url }}</q-item-label>
                    </q-item-section>
                    <q-item-section side>
                      <q-btn
                        dense
                        color="primary"
                        label="Apply"
                        :loading="applyingLogoUrl === suggestion.url"
                        :disable="applyingLogoUrl !== null"
                        @click="applySpecificSuggestedLogo(suggestion)"
                      />
                    </q-item-section>
                  </q-item>
                </q-list>
              </div>

              <div v-if="issue.actions && issue.actions.length" class="q-gutter-sm q-mt-sm">
                <q-btn
                  v-for="action in issue.actions"
                  :key="action.label"
                  :label="action.label"
                  color="primary"
                  @click="action.handler"
                />
              </div>
            </div>
          </div>
          <q-separator v-if="index < issueList.length - 1" class="q-mt-md" />
        </template>
      </div>
    </template>
  </TicDialogPopup>
</template>

<script>
import axios from 'axios';
import {TicDialogPopup} from 'components/ui';

export default {
  name: 'ChannelIssuesDialog',
  components: {
    TicDialogPopup,
  },
  // Needs attention issue layout rule:
  // 1) title + description
  // 2) nested issue details area (left border + indent) containing:
  //    - optional detail list (e.g. affected streams, suggested logos)
  //    - action buttons at the bottom
  // This keeps all issue sections consistent and makes required actions explicit.
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
      isOpen: false,
      didEmitOk: false,
      syncing: false,
      loadingLogoSuggestions: false,
      logoSuggestions: [],
      applyingLogoUrl: null,
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
          description: 'This channel has no streams linked. Add a stream to enable playback.',
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
        channel_logo_unavailable: {
          title: 'Channel logo unavailable',
          description: this.logoIssueDescription,
          actions: [actions.openSettings],
        },
        cso_connection_issue: {
          title: 'Channel Stream Organiser connection issue',
          description:
            'CSO could not connect to an upstream source or maintain output playback. Check source availability, connection limits, and channel stream ordering.',
          csoDetails: this.csoIssueDetails,
          actions: [actions.openSettings],
        },
        cso_stream_unhealthy: {
          title: 'Channel Stream Organiser unhealthy stream',
          description:
            'CSO detected unstable stream health (for example buffering/underspeed) and triggered failover attempts. Review source quality and priority.',
          csoDetails: this.csoIssueDetails,
          actions: [actions.openSettings],
        },
      };
      const issues = this.normalizedIssues;
      return issues.map((issue) => ({key: issue, ...(definitions[issue] || {title: issue, description: ''})})).
        filter((issue) => issue.title);
    },
    normalizedIssues() {
      const fromProps = Array.isArray(this.issues) ? this.issues : [];
      const fromStatus = Array.isArray(this.channel?.status?.issues) ? this.channel.status.issues : [];
      const combined = [...fromProps, ...fromStatus];
      return [...new Set(combined.filter((issue) => typeof issue === 'string' && issue.length))];
    },
    failedStreams() {
      const raw = this.channel?.status?.failed_streams || [];
      const labels = raw.map((entry) => {
        const name = entry?.stream_name || 'Unknown stream';
        const playlist = entry?.playlist_name;
        return playlist ? `${name} (${playlist})` : name;
      });
      return [...new Set(labels)].map((label) => ({label}));
    },
    logoIssueDescription() {
      return 'Headendarr could not fetch/cache this channel logo. Update the logo URL to a working image, or clear the logo field to remove this warning.';
    },
    logoIssueError() {
      return this.channel?.status?.logo_health?.error || '';
    },
    csoLatestEvent() {
      return this.channel?.status?.cso_health?.latest_event || null;
    },
    csoIssueDetails() {
      const event = this.csoLatestEvent || {};
      const details = event.details || {};
      const lines = [];

      const reason = details.reason || event.reason;
      if (reason) {
        lines.push(`Reason: ${reason}`);
      }
      if (details.stream_name) {
        lines.push(`Stream: ${details.stream_name}`);
      }
      if (details.playlist_name) {
        lines.push(`Playlist: ${details.playlist_name}`);
      }
      if (details.source_priority !== undefined && details.source_priority !== null) {
        lines.push(`Priority: ${details.source_priority}`);
      }
      if (details.source_id) {
        lines.push(`Source ID: ${details.source_id}`);
      }
      if (details.ffmpeg_error) {
        lines.push(`FFmpeg: ${details.ffmpeg_error}`);
      }
      if (event.created_at) {
        lines.push(`At: ${event.created_at}`);
      }
      return lines;
    },
  },
  methods: {
    hasIssueDetails(issue) {
      return Boolean(
        (issue.actions && issue.actions.length) ||
        (issue.streams && issue.streams.length) ||
        (issue.csoDetails && issue.csoDetails.length) ||
        issue.key === 'channel_logo_unavailable',
      );
    },
    show() {
      this.didEmitOk = false;
      this.loadLogoSuggestionsIfNeeded();
      this.isOpen = true;
    },
    hide() {
      this.isOpen = false;
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
    loadLogoSuggestionsIfNeeded() {
      if (!this.normalizedIssues.includes('channel_logo_unavailable') || !this.channel?.id) {
        this.logoSuggestions = [];
        return;
      }
      this.loadingLogoSuggestions = true;
      axios({
        method: 'GET',
        url: `/tic-api/channels/${this.channel.id}/logo-suggestions`,
      }).then((response) => {
        this.logoSuggestions = response?.data?.data || [];
      }).catch(() => {
        this.logoSuggestions = [];
      }).finally(() => {
        this.loadingLogoSuggestions = false;
      });
    },
    applySpecificSuggestedLogo(suggestion) {
      if (!suggestion?.url || !this.channel?.id) {
        return;
      }
      this.applyingLogoUrl = suggestion.url;
      axios({
        method: 'POST',
        url: `/tic-api/channels/${this.channel.id}/logo-suggestions/apply`,
        data: {url: suggestion.url},
      }).then((response) => {
        if (response?.data?.success) {
          this.$q.notify({color: 'positive', message: 'Suggested logo applied'});
          this.didEmitOk = true;
          this.$emit('ok', {refresh: true});
          this.hide();
          return;
        }
        this.$q.notify({color: 'warning', message: response?.data?.message || 'Failed to apply logo'});
      }).catch(() => {
        this.$q.notify({color: 'negative', message: 'Failed to apply suggested logo'});
      }).finally(() => {
        this.applyingLogoUrl = null;
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

.issue-actions {
  border-left: 2px solid rgba(0, 0, 0, 0.14);
  padding-left: 12px;
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

.issue-log-line {
  margin-top: 4px;
  padding: 6px 8px;
  border: 1px solid rgba(0, 0, 0, 0.12);
  border-radius: 6px;
  background: rgba(0, 0, 0, 0.03);
  font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, 'Liberation Mono', 'Courier New', monospace;
  font-size: 12px;
  line-height: 1.4;
  color: #444;
  white-space: pre-wrap;
  word-break: break-word;
}

.logo-preview {
  width: 34px;
  height: 34px;
  border-radius: 7px;
  border: 1px solid rgba(0, 0, 0, 0.12);
  background: #fff;
  display: flex;
  align-items: center;
  justify-content: center;
  overflow: hidden;
}

.logo-preview__img {
  width: 100%;
  height: 100%;
}
</style>
