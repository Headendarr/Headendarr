<template>
  <TicDialogWindow
    v-model="isOpen"
    title="Stream Diagnostics"
    width="700px"
    :actions="dialogActions"
    @action="onDialogAction"
    @hide="onDialogHide"
  >
    <div class="q-pa-md">
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
          Analysing stream performance and routing over 20 seconds. Please wait...
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

      <div v-else-if="report">
        <div class="row q-col-gutter-md">
          <!-- Geo Info -->
          <div class="col-12 col-md-6">
            <q-card flat bordered class="h-100">
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
                    <q-item-label caption>IP Address</q-item-label>
                    <q-item-label>{{ report.dns?.ip || 'Unknown' }}</q-item-label>
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
              </q-list>
            </q-card>
          </div>

          <!-- Performance Info -->
          <div class="col-12 col-md-6">
            <q-card flat bordered class="h-100">
              <q-card-section>
                <div class="text-subtitle2">Stream Performance</div>
              </q-card-section>
              <q-separator />
              <q-list dense>
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
              <q-card-section v-if="report.probe?.summary" :class="healthBgClass(report.probe?.health)" class="q-py-sm">
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
        <div class="q-mt-md">
          <div class="text-subtitle2 q-mb-sm">Detailed Logs</div>
          <div class="bg-grey-2 q-pa-sm rounded-borders scroll"
               style="max-height: 200px; font-family: monospace; font-size: 12px;">
            <div v-for="(log, i) in report.logs" :key="i">{{ log }}</div>
            <div v-for="(err, i) in report.errors" :key="'err-'+i" class="text-negative">{{ err }}</div>
          </div>
        </div>
      </div>

      <div v-else class="text-center q-pa-lg">
        <q-icon name="speed" size="64px" color="grey-5" />
        <div class="q-mt-md">Click 'Start Test' to analyze this stream.</div>
      </div>
    </div>
  </TicDialogWindow>
</template>

<script>
import axios from 'axios';
import TicDialogWindow from 'components/ui/dialogs/TicDialogWindow.vue';
import TicTextInput from 'components/ui/inputs/TicTextInput.vue';
import TicToggleInput from 'components/ui/inputs/TicToggleInput.vue';

export default {
  name: 'StreamTestDialog',
  components: {
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
    dialogActions() {
      if (this.loading) {
        return [];
      }
      if (this.report) {
        return [
          {
            id: 'restart',
            label: 'Run Again',
            icon: 'refresh',
            color: 'primary',
          },
          {
            id: 'close',
            label: 'Close',
            color: 'grey-8',
          },
        ];
      }
      return [
        {
          id: 'start',
          label: 'Start Test',
          icon: 'play_arrow',
          color: 'primary',
        },
      ];
    },
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
    onDialogAction(action) {
      if (action.id === 'start' || action.id === 'restart') {
        this.startTest();
      } else if (action.id === 'close') {
        this.isOpen = false;
      }
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
      // Test is ~20s. We'll advance 1% every 200ms.
      this.progressInterval = setInterval(() => {
        if (this.testProgress < 0.95) {
          this.testProgress += 0.01;
        }
      }, 200);
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
          if (data.status === 'finished' || data.status === 'error') {
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
    formatBitrate(bitrate) {
      if (!bitrate) return 'N/A';
      if (bitrate > 1000000) return (bitrate / 1000000).toFixed(2) + ' Mbps';
      return (bitrate / 1000).toFixed(0) + ' Kbps';
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
        good: 'bg-green-1',
        fair: 'bg-orange-1',
        poor: 'bg-red-1',
        critical: 'bg-red-1',
        uncertain: 'bg-blue-1',
      };
      return classes[health] || 'bg-grey-1';
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
</style>
