<template>
  <TicDialogWindow
    v-model="isOpen"
    title="Stream Diagnostics"
    width="700px"
    @hide="onDialogHide"
  >
    <div class="q-pa-md stream-test-dialog">
      <q-form class="tic-form-layout q-mb-md">
        <TicTextInput
          v-model="localStreamUrl"
          label="Stream URL to Test"
          :disable="loading"
          placeholder="http://..."
        >
          <template #prepend>
            <q-icon name="link" />
          </template>
        </TicTextInput>

        <TicTextInput
          v-model="localUserAgent"
          label="User-Agent"
          :disable="loading"
          placeholder="Optional"
          description="If left empty, diagnostics will use the default browser user-agent."
        >
          <template #prepend>
            <q-icon name="smart_toy" />
          </template>
        </TicTextInput>

        <TicToggleInput
          v-model="bypassProxies"
          label="Bypass HLS Proxies"
          description="Automatically unwrap and test the original upstream source URL."
          :disable="loading"
        />
      </q-form>

      <div v-if="!loading" class="row justify-center q-mb-md">
        <TicButton
          :label="report ? 'Run Again' : 'Start Test'"
          :icon="report ? 'refresh' : 'play_arrow'"
          color="primary"
          @click="startTest"
        />
      </div>

      <div v-if="loading" class="column items-center justify-center q-pa-xl diagnostic-loading-container">
        <div class="relative-position q-mb-xl">
          <q-spinner-gear size="80px" color="primary" />
          <q-icon
            name="speed"
            size="30px"
            color="secondary"
            class="absolute-center"
          />
        </div>
        <div class="text-h5 text-weight-bold text-primary q-mb-xs pulse-text">
          Running Diagnostics...
        </div>
        <div class="text-subtitle1 text-grey-7 q-mb-lg text-center">
          Analysing stream performance and routing over {{ manualSampleSeconds }} seconds. Please wait...
        </div>
        <div class="full-width q-px-md">
          <q-linear-progress
            :value="testProgress"
            color="primary"
            class="q-mt-sm rounded-borders"
            size="12px"
            stripe
            animated
          />
          <div class="row justify-between text-caption text-grey-6 q-mt-xs">
            <span>{{ progressStatusLabel }}</span>
            <span>{{ Math.round(testProgress * 100) }}%</span>
          </div>
        </div>
      </div>

      <div v-else-if="report" class="diagnostic-report">
        <div class="row q-col-gutter-md">
          <!-- Geo Info -->
          <div class="col-12 col-md-6">
            <q-card flat bordered class="h-100 diagnostic-card">
              <q-card-section>
                <div class="text-subtitle2">Network Route</div>
              </q-card-section>
              <q-separator />
              <q-list dense>
                <q-item>
                  <q-item-section>
                    <q-item-label caption>Hostname</q-item-label>
                    <q-item-label>{{ report.dns?.hostname || 'Unknown' }}</q-item-label>
                  </q-item-section>
                </q-item>
                <q-item>
                  <q-item-section>
                    <q-item-label caption>DNS Answers</q-item-label>
                    <q-item-label>
                      {{ formatDnsAnswers(report.dns?.answers) }}
                    </q-item-label>
                  </q-item-section>
                </q-item>
                <q-item>
                  <q-item-section>
                    <q-item-label caption>Connected Endpoint</q-item-label>
                    <q-item-label>
                      {{ formatConnectedEndpoint(report.connection) }}
                    </q-item-label>
                  </q-item-section>
                </q-item>
                <q-item v-if="report.connection?.final_hostname">
                  <q-item-section>
                    <q-item-label caption>Final Host</q-item-label>
                    <q-item-label>{{ report.connection.final_hostname }}</q-item-label>
                  </q-item-section>
                </q-item>
                <q-item>
                  <q-item-section>
                    <q-item-label caption>Location</q-item-label>
                    <q-item-label>
                      <template v-if="report.geo?.country">
                        {{ report.geo.city }}, {{ report.geo.country }}
                      </template>
                      <template v-else>Unknown</template>
                    </q-item-label>
                  </q-item-section>
                </q-item>
                <q-item>
                  <q-item-section>
                    <q-item-label caption>ISP</q-item-label>
                    <q-item-label>{{ report.geo?.isp || 'Unknown' }}</q-item-label>
                  </q-item-section>
                </q-item>
                <q-item v-if="report.trace?.target">
                  <q-item-section>
                    <q-item-label caption>Route Trace</q-item-label>
                    <q-item-label>{{ formatTraceSummary(report.trace) }}</q-item-label>
                  </q-item-section>
                </q-item>
                <q-item v-if="report.proxy_hops_count > 0">
                  <q-item-section>
                    <q-item-label caption>Proxy Hops</q-item-label>
                    <q-item-label>{{ report.proxy_hops_count }}</q-item-label>
                  </q-item-section>
                </q-item>
              </q-list>
              <q-separator v-if="report.proxy_hops_count > 0" />
              <q-card-section v-if="report.proxy_hops_count > 0" class="q-pt-sm q-pb-sm">
                <div class="text-caption text-grey-8 q-mb-sm">
                  Route resolved after unwrapping {{ report.proxy_hops_count }} proxy hop(s).
                </div>
                <div
                  v-for="hop in report.proxy_chain || []"
                  :key="`hop-${hop.hop}`"
                  class="q-mb-xs text-caption"
                >
                  <span class="text-weight-medium">Hop {{ hop.hop }}:</span>
                  {{ hop.proxy_hostname || 'Unknown proxy host' }} -> {{ hop.target_hostname || 'Unknown target host' }}
                </div>
              </q-card-section>
            </q-card>
          </div>

          <!-- Performance Info -->
          <div class="col-12 col-md-6">
            <q-card flat bordered class="h-100 diagnostic-card">
              <q-card-section>
                <div class="text-subtitle2">Stream Performance</div>
              </q-card-section>
              <q-separator />
              <q-list dense>
                <q-item>
                  <q-item-section>
                    <q-item-label caption>Time To First Data</q-item-label>
                    <q-item-label :class="firstDataClass(report.probe?.time_to_first_media_seconds)">
                      {{ formatFirstDataTime(report.probe?.time_to_first_media_seconds) }}
                    </q-item-label>
                  </q-item-section>
                </q-item>
                <q-item>
                  <q-item-section>
                    <q-item-label caption>Average Speed</q-item-label>
                    <q-item-label :class="speedClass(report.probe?.avg_speed)">
                      {{ formatSpeed(report.probe?.avg_speed) }}
                    </q-item-label>
                  </q-item-section>
                </q-item>
                <q-item>
                  <q-item-section>
                    <q-item-label caption>Bitrate</q-item-label>
                    <q-item-label>{{ formatBitrate(report.probe?.avg_bitrate) }}</q-item-label>
                  </q-item-section>
                </q-item>
                <q-item>
                  <q-item-section>
                    <q-item-label caption>Status</q-item-label>
                    <q-item-label v-if="report.errors?.length" class="text-negative">
                      Errors Detected
                    </q-item-label>
                    <q-item-label v-else class="text-positive">
                      Stable
                    </q-item-label>
                  </q-item-section>
                </q-item>
              </q-list>
              <q-separator v-if="report.probe?.summary" />
              <q-card-section
                v-if="report.probe?.summary"
                :class="healthBgClass(report.probe?.health)"
                class="q-py-sm diagnostic-health"
              >
                <div class="row no-wrap items-center">
                  <q-icon :name="healthIcon(report.probe?.health)" :color="healthColor(report.probe?.health)"
                          size="20px" class="q-mr-sm" />
                  <div class="text-caption" :class="`text-${healthColor(report.probe?.health)}`"
                       style="line-height: 1.2">
                    {{ report.probe.summary }}
                  </div>
                </div>
              </q-card-section>
            </q-card>
          </div>
        </div>

        <!-- Logs -->
        <div class="q-mt-md diagnostic-logs-section">
          <div class="text-subtitle2 q-mb-sm">Detailed Logs</div>
          <div class="diagnostic-logs q-pa-sm rounded-borders scroll">
            <div v-for="(log, i) in report.logs" :key="i">{{ log }}</div>
            <div v-for="(err, i) in report.errors" :key="'err-'+i" class="text-negative">{{ err }}</div>
          </div>
        </div>
      </div>

      <div v-else class="text-center q-pa-lg diagnostic-empty-state">
        <q-icon name="speed" size="64px" color="grey-5" />
        <div class="q-mt-md">Click 'Start Test' to analyze this stream.</div>
      </div>
    </div>
  </TicDialogWindow>
</template>

<script>
import axios from 'axios';
import TicButton from 'components/ui/buttons/TicButton.vue';
import TicDialogWindow from 'components/ui/dialogs/TicDialogWindow.vue';
import TicTextInput from 'components/ui/inputs/TicTextInput.vue';
import TicToggleInput from 'components/ui/inputs/TicToggleInput.vue';

export default {
  name: 'StreamTestDialog',
  components: {
    TicButton,
    TicDialogWindow,
    TicTextInput,
    TicToggleInput,
  },
  props: {
    streamUrl: {
      type: String,
      required: true,
    },
    userAgent: {
      type: String,
      default: '',
    },
    channelSourceId: {
      type: [Number, String],
      default: null,
    },
  },
  emits: ['hide'],
  data() {
    return {
      isOpen: false,
      loading: false,
      taskId: null,
      pollInterval: null,
      report: null,
      localStreamUrl: '',
      localUserAgent: '',
      testProgress: 0,
      progressInterval: null,
      bypassProxies: false,
      manualSampleSeconds: 12,
    };
  },
  watch: {
    streamUrl: {
      handler(val) {
        this.localStreamUrl = val;
      },
      immediate: true,
    },
    userAgent: {
      handler(val) {
        this.localUserAgent = (val || '').trim();
      },
      immediate: true,
    },
  },
  computed: {
    progressStatusLabel() {
      if (this.testProgress < 0.1) return 'Connecting to stream...';
      if (this.testProgress < 0.3) return 'Resolving network route...';
      if (this.testProgress < 0.8) return 'Capturing performance sample...';
      if (this.testProgress < 0.95) return 'Finalising report...';
      return 'Complete!';
    },
  },
  methods: {
    show() {
      this.isOpen = true;
      this.reset();
    },
    reset() {
      this.loading = false;
      this.taskId = null;
      this.report = null;
      this.testProgress = 0;
      this.stopPolling();
      this.stopProgress();
    },
    onDialogHide() {
      this.stopPolling();
      this.stopProgress();
      if (this.taskId) {
        // Optional: cancel task on backend?
      }
      this.$emit('hide');
    },
    startTest() {
      if (!this.localStreamUrl) {
        this.$q.notify({color: 'negative', message: 'Please enter a stream URL'});
        return;
      }
      this.loading = true;
      this.report = null;
      this.testProgress = 0;
      this.startProgress();
      axios.post('/tic-api/diagnostics/stream/test', {
        stream_url: this.localStreamUrl,
        bypass_proxies: this.bypassProxies,
        user_agent: this.localUserAgent,
        channel_source_id: this.channelSourceId,
      }).then(response => {
        if (response.data.success) {
          this.taskId = response.data.task_id;
          this.pollStatus();
        } else {
          this.$q.notify({color: 'negative', message: response.data.message});
          this.loading = false;
          this.stopProgress();
        }
      }).catch(err => {
        this.$q.notify({color: 'negative', message: 'Failed to start test'});
        this.loading = false;
        this.stopProgress();
      });
    },
    startProgress() {
      this.stopProgress();
      // Manual diagnostics runs for ~12s by default.
      const stepIntervalMs = 200;
      const progressStep = stepIntervalMs / (this.manualSampleSeconds * 1000);
      this.progressInterval = setInterval(() => {
        if (this.testProgress < 0.95) {
          this.testProgress += progressStep;
        }
      }, stepIntervalMs);
    },
    stopProgress() {
      if (this.progressInterval) {
        clearInterval(this.progressInterval);
        this.progressInterval = null;
      }
    },
    pollStatus() {
      this.stopPolling();
      this.pollInterval = setInterval(() => {
        axios.get(`/tic-api/diagnostics/stream/test/${this.taskId}`).then(response => {
          const data = response.data.data;
          if (data.status === 'finished' || data.status === 'error' || data.status === 'cancelled') {
            this.stopPolling();
            this.stopProgress();
            this.testProgress = 1.0;
            setTimeout(() => {
              this.loading = false;
              this.report = data.report;
            }, 500);
          } else {
            // Update live logs if available
            // currently only full report on finish, but could extend
          }
        }).catch(() => {
          this.stopPolling();
          this.stopProgress();
          this.loading = false;
          this.$q.notify({color: 'negative', message: 'Failed to poll status'});
        });
      }, 1000);
    },
    stopPolling() {
      if (this.pollInterval) {
        clearInterval(this.pollInterval);
        this.pollInterval = null;
      }
    },
    formatSpeed(speed) {
      if (speed === undefined || speed === null) return 'N/A';
      return speed.toFixed(2) + 'x';
    },
    formatFirstDataTime(seconds) {
      if (seconds === undefined || seconds === null) return 'N/A';
      if (seconds < 1) return (seconds * 1000).toFixed(0) + ' ms';
      return seconds.toFixed(2) + ' s';
    },
    formatBitrate(bitrate) {
      if (!bitrate) return 'N/A';
      if (bitrate > 1000000) return (bitrate / 1000000).toFixed(2) + ' Mbps';
      return (bitrate / 1000).toFixed(0) + ' Kbps';
    },
    formatDnsAnswers(answers) {
      if (!Array.isArray(answers) || answers.length === 0) return 'Unknown';
      return answers.map((item) => item.address).join(', ');
    },
    formatConnectedEndpoint(connection) {
      if (!connection?.peer_ip) return 'Unknown';
      if (connection.peer_port) return connection.peer_ip + ':' + connection.peer_port;
      return connection.peer_ip;
    },
    formatTraceSummary(trace) {
      if (!trace?.target) return 'Not run';
      const hopCount = Array.isArray(trace.hops) ? trace.hops.length : 0;
      const status = trace.completed ? 'completed' : 'partial';
      return `${trace.target} via ${hopCount} hop${hopCount === 1 ? '' : 's'} (${status})`;
    },
    firstDataClass(seconds) {
      if (seconds === undefined || seconds === null) return '';
      if (seconds < 1) return 'text-positive text-weight-bold';
      if (seconds < 3) return 'text-warning text-weight-bold';
      return 'text-negative text-weight-bold';
    },
    speedClass(speed) {
      if (!speed) return '';
      if (speed >= 1.0) return 'text-positive text-weight-bold';
      if (speed >= 0.8) return 'text-warning text-weight-bold';
      return 'text-negative text-weight-bold';
    },
    healthColor(health) {
      const colors = {
        good: 'positive',
        fair: 'warning',
        poor: 'negative',
        critical: 'negative',
        uncertain: 'info',
      };
      return colors[health] || 'grey-7';
    },
    healthBgClass(health) {
      const classes = {
        good: 'diagnostic-health--good',
        fair: 'diagnostic-health--fair',
        poor: 'diagnostic-health--poor',
        critical: 'diagnostic-health--critical',
        uncertain: 'diagnostic-health--uncertain',
      };
      return classes[health] || 'diagnostic-health--default';
    },
    healthIcon(health) {
      const icons = {
        good: 'check_circle',
        fair: 'report_problem',
        poor: 'error',
        critical: 'dangerous',
        uncertain: 'help',
      };
      return icons[health] || 'info';
    },
  },
  beforeUnmount() {
    this.stopPolling();
    this.stopProgress();
  },
};
</script>

<style scoped>
.stream-test-dialog {
  min-height: 100%;
}

.diagnostic-report {
  min-height: 100%;
}

.diagnostic-loading-container {
  min-height: 350px;
}

.pulse-text {
  animation: pulse 2s infinite ease-in-out;
}

@keyframes pulse {
  0% {
    opacity: 0.6;
  }
  50% {
    opacity: 1;
  }
  100% {
    opacity: 0.6;
  }
}

.h-100 {
  height: 100%;
}

.diagnostic-card {
  background: var(--app-surface-bg);
  border: var(--tic-elevated-border);
}

.diagnostic-health {
  border-top: 1px solid var(--q-separator-color, rgba(0, 0, 0, 0.12));
}

.diagnostic-health--good {
  background: color-mix(in srgb, var(--q-positive), var(--app-surface-bg) 88%);
}

.diagnostic-health--fair {
  background: color-mix(in srgb, var(--q-warning), var(--app-surface-bg) 88%);
}

.diagnostic-health--poor,
.diagnostic-health--critical {
  background: color-mix(in srgb, var(--q-negative), var(--app-surface-bg) 90%);
}

.diagnostic-health--uncertain {
  background: color-mix(in srgb, var(--q-info), var(--app-surface-bg) 88%);
}

.diagnostic-health--default {
  background: color-mix(in srgb, var(--q-grey-6, #9e9e9e), var(--app-surface-bg) 90%);
}

:global(body.body--dark) .diagnostic-health--good {
  background: color-mix(in srgb, var(--q-positive), var(--app-surface-bg) 76%);
}

:global(body.body--dark) .diagnostic-health--fair {
  background: color-mix(in srgb, var(--q-warning), var(--app-surface-bg) 78%);
}

:global(body.body--dark) .diagnostic-health--poor,
:global(body.body--dark) .diagnostic-health--critical {
  background: color-mix(in srgb, var(--q-negative), var(--app-surface-bg) 80%);
}

:global(body.body--dark) .diagnostic-health--uncertain {
  background: color-mix(in srgb, var(--q-info), var(--app-surface-bg) 78%);
}

:global(body.body--dark) .diagnostic-health--default {
  background: color-mix(in srgb, var(--q-grey-6, #9e9e9e), var(--app-surface-bg) 82%);
}

.diagnostic-logs,
.diagnostic-empty-state {
  background: color-mix(in srgb, var(--app-surface-bg), var(--q-primary) 4%);
  border: 1px solid var(--q-separator-color, rgba(0, 0, 0, 0.12));
}

.diagnostic-logs-section {
  display: flex;
  flex-direction: column;
}

.diagnostic-logs {
  min-height: 200px;
  max-height: clamp(200px, 42vh, 420px);
  font-family: monospace;
  font-size: 12px;
  white-space: pre-wrap;
  overflow-wrap: anywhere;
}
</style>
