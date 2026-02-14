<template>
  <q-page>
    <div :class="$q.screen.lt.sm ? 'q-pa-none' : 'q-pa-md'">
      <div class="row q-col-gutter-md">
        <div class="col-12 col-lg-8">
          <q-card flat class="dashboard-card dashboard-card--activity">
            <q-card-section>
              <div class="text-h6 text-primary">Current Activity</div>
              <div class="text-caption text-grey-7">Active streaming sessions and connection details</div>
            </q-card-section>
            <q-separator />
            <q-list separator>
              <q-item v-for="(item, idx) in activity" :key="`${item.username || 'anon'}-${item.last_seen || idx}`">
                <q-item-section>
                  <q-item-label class="text-weight-medium">
                    {{ item.username || 'Unknown user' }}
                  </q-item-label>
                  <q-item-label caption>
                    {{ item.device_label || fallbackDevice(item.user_agent) }}
                    <span class="q-mx-xs">|</span>
                    {{ item.region_label || 'Unknown region' }}
                    <span class="q-mx-xs">|</span>
                    {{ item.ip_address || 'No IP' }}
                  </q-item-label>
                  <q-item-label caption>
                    Endpoint: {{ item.endpoint || '-' }}
                  </q-item-label>
                  <q-item-label v-if="item.details" caption class="text-grey-7">
                    {{ item.details }}
                  </q-item-label>
                </q-item-section>
                <q-item-section side top>
                  <q-chip dense color="primary" text-color="white">
                    Active {{ activityAgeLabel(item.age_seconds) }}
                  </q-chip>
                </q-item-section>
              </q-item>
              <q-item v-if="!activityLoading && !activity.length">
                <q-item-section>
                  <q-item-label class="text-grey-7">No active streams right now.</q-item-label>
                </q-item-section>
              </q-item>
            </q-list>
            <q-inner-loading :showing="activityLoading">
              <q-spinner-dots size="36px" color="primary" />
            </q-inner-loading>
          </q-card>
        </div>

        <div class="col-12 col-lg-4">
          <q-card flat class="dashboard-card q-mb-md">
            <q-card-section>
              <div class="row items-center justify-between no-wrap">
                <div class="text-subtitle1 text-primary">Channels</div>
                <q-btn
                  flat
                  dense
                  no-caps
                  color="primary"
                  icon-right="arrow_forward"
                  label="View all"
                  @click="goTo('/channels')"
                />
              </div>

              <div class="channels-summary q-mt-sm">
                <div class="text-body2">Configured: {{ summary.channels?.channel_count || 0 }}</div>
                <div class="text-body2">Needs attention: {{ summary.channels?.warning_channel_count || 0 }}</div>

                <div class="channels-issues q-mt-sm">
                  <q-list dense>
                    <q-item
                      v-for="(issue, idx) in combinedIssues"
                      :key="`${issue.issue_key}-${idx}`"
                      clickable
                      @click="goTo(issue.route)"
                    >
                      <q-item-section avatar>
                        <q-icon name="warning" color="warning" />
                      </q-item-section>
                      <q-item-section>
                        <q-item-label>{{ issue.label }}</q-item-label>
                        <q-item-label caption>{{ issue.count }} item(s)</q-item-label>
                      </q-item-section>
                    </q-item>
                    <q-item v-if="!combinedIssues.length">
                      <q-item-section>
                        <q-item-label class="text-grey-7">No issues currently detected.</q-item-label>
                      </q-item-section>
                    </q-item>
                  </q-list>
                </div>
              </div>
            </q-card-section>
          </q-card>
        </div>

        <div class="col-12 col-lg-6">
          <q-card flat class="dashboard-card">
            <q-card-section>
              <div class="row items-center justify-between no-wrap">
                <div class="text-subtitle1 text-primary">Recent Audit</div>
                <q-btn
                  flat
                  dense
                  no-caps
                  color="primary"
                  icon-right="arrow_forward"
                  label="View all"
                  @click="goTo('/audit')"
                />
              </div>
            </q-card-section>
            <q-separator />
            <q-list separator>
              <q-item v-for="entry in summary.recent_audit || []" :key="entry.id">
                <q-item-section>
                  <q-item-label>{{ entry.activity_label || 'Other activity' }}</q-item-label>
                  <q-item-label caption>
                    {{ formatAuditTimestamp(entry.created_at) }}
                    <span class="q-mx-xs">|</span>
                    {{ entry.username || 'Unknown user' }}
                  </q-item-label>
                </q-item-section>
              </q-item>
              <q-item v-if="!(summary.recent_audit || []).length">
                <q-item-section>
                  <q-item-label class="text-grey-7">No recent audit entries.</q-item-label>
                </q-item-section>
              </q-item>
            </q-list>
          </q-card>
        </div>

        <div class="col-12 col-lg-6">
          <q-card flat class="dashboard-card">
            <q-card-section>
              <div class="text-subtitle1 text-primary">Storage Utilization</div>
            </q-card-section>
            <q-separator />
            <q-list separator>
              <q-item v-for="item in summary.storage || []" :key="item.label">
                <q-item-section>
                  <q-item-label>{{ item.label }}</q-item-label>
                  <q-item-label caption>{{ item.path || '-' }}</q-item-label>
                  <div v-if="item.exists" class="q-mt-sm">
                    <q-linear-progress
                      rounded
                      size="10px"
                      color="primary"
                      track-color="grey-4"
                      :value="utilizationRatio(item.used_bytes, item.total_bytes)"
                    />
                    <div class="text-caption text-grey-7 storage-utilization-text">
                      Used {{ formatBytes(item.used_bytes) }} of {{ formatBytes(item.total_bytes) }}
                      ({{ formatPercent(item.used_bytes, item.total_bytes) }})
                    </div>
                  </div>
                  <q-item-label v-else caption class="text-warning q-mt-sm">Path unavailable</q-item-label>
                </q-item-section>
              </q-item>
              <q-item v-if="!(summary.storage || []).length">
                <q-item-section>
                  <q-item-label class="text-grey-7">No storage data available.</q-item-label>
                </q-item-section>
              </q-item>
            </q-list>
          </q-card>
        </div>
      </div>

      <q-inner-loading :showing="loadingSummary">
        <q-spinner-dots size="42px" color="primary" />
      </q-inner-loading>
    </div>
  </q-page>
</template>

<script>
import axios from 'axios';
import {defineComponent} from 'vue';
import {useAuthStore} from 'stores/auth';
import {useUiStore} from 'stores/ui';

export default defineComponent({
  name: 'DashboardPage',
  setup() {
    return {
      authStore: useAuthStore(),
      uiStore: useUiStore(),
    };
  },
  data() {
    return {
      loadingSummary: false,
      activityLoading: false,
      summary: {
        version: null,
        recent_audit: [],
        storage: [],
        channels: {
          channel_count: 0,
          warning_channel_count: 0,
          issues: [],
        },
      },
      activity: [],
      epgIssue: null,
      pollTimer: null,
      pollCancelled: false,
      pollInFlight: false,
    };
  },
  computed: {
    combinedIssues() {
      const list = [...(this.summary.channels?.issues || [])];
      if (this.epgIssue) {
        list.push(this.epgIssue);
      }
      return list;
    },
  },
  methods: {
    fallbackDevice(userAgent) {
      const value = String(userAgent || '').trim();
      return value || 'Unknown';
    },
    formatAuditTimestamp(value) {
      if (!value) {
        return '-';
      }
      try {
        return new Date(value).toLocaleString(undefined, {
          year: 'numeric',
          month: '2-digit',
          day: '2-digit',
          hour: '2-digit',
          minute: '2-digit',
          second: '2-digit',
          hour12: this.uiStore.timeFormat === '12h',
        });
      } catch {
        return value;
      }
    },
    formatBytes(bytes) {
      const value = Number(bytes || 0);
      if (!Number.isFinite(value) || value <= 0) {
        return '0 B';
      }
      const units = ['B', 'KB', 'MB', 'GB', 'TB'];
      let index = 0;
      let amount = value;
      while (amount >= 1024 && index < units.length - 1) {
        amount /= 1024;
        index += 1;
      }
      return `${amount.toFixed(amount >= 10 || index === 0 ? 0 : 1)} ${units[index]}`;
    },
    formatPercent(used, total) {
      const u = Number(used || 0);
      const t = Number(total || 0);
      if (!t) {
        return '0%';
      }
      return `${Math.min(100, Math.max(0, (u / t) * 100)).toFixed(1)}%`;
    },
    utilizationRatio(used, total) {
      const u = Number(used || 0);
      const t = Number(total || 0);
      if (!t) {
        return 0;
      }
      return Math.min(1, Math.max(0, u / t));
    },
    activityAgeLabel(seconds) {
      const value = Math.max(0, Number(seconds || 0));
      if (value < 60) {
        return `${value}s`;
      }
      const minutes = Math.floor(value / 60);
      return `${minutes}m`;
    },
    async loadSummary(options = {}) {
      const silent = !!options.silent;
      if (!silent) {
        this.loadingSummary = true;
      }
      try {
        const response = await axios.get('/tic-api/dashboard/summary');
        this.summary = response.data.data || this.summary;
      } catch (error) {
        console.error('Failed to load dashboard summary:', error);
        this.$q.notify({color: 'negative', message: 'Failed to load dashboard summary'});
      } finally {
        if (!silent) {
          this.loadingSummary = false;
        }
      }
    },
    async loadActivity() {
      this.activityLoading = true;
      try {
        const response = await axios.get('/tic-api/dashboard/activity');
        this.activity = response.data.data || [];
      } catch (error) {
        console.error('Failed to load dashboard activity:', error);
      } finally {
        this.activityLoading = false;
      }
    },
    async loadEpgIssueSummary() {
      this.epgIssue = null;
      const roles = this.authStore.user?.roles || [];
      if (!roles.includes('admin')) {
        return;
      }
      try {
        const response = await axios.get('/tic-api/epgs/get');
        const epgs = response.data.data || [];
        const failing = epgs.filter((epg) => epg?.health?.status === 'error');
        if (failing.length) {
          this.epgIssue = {
            issue_key: 'epg_download_failed',
            label: 'EPG URL downloads have issues',
            count: failing.length,
            route: '/epgs',
          };
        }
      } catch (error) {
        console.error('Failed to load EPG health:', error);
      }
    },
    async runDashboardPoll() {
      if (this.pollCancelled) {
        return;
      }
      if (this.pollInFlight) {
        return;
      }
      this.pollInFlight = true;
      try {
        await Promise.all([
          this.loadSummary({silent: true}),
          this.loadActivity(),
          this.loadEpgIssueSummary(),
        ]);
      } finally {
        this.pollInFlight = false;
        if (!this.pollCancelled) {
          this.pollTimer = setTimeout(() => this.runDashboardPoll(), 25000);
        }
      }
    },
    stopDashboardPoll() {
      this.pollCancelled = true;
      if (this.pollTimer) {
        clearTimeout(this.pollTimer);
        this.pollTimer = null;
      }
    },
    startDashboardPoll() {
      this.stopDashboardPoll();
      this.pollCancelled = false;
      this.runDashboardPoll();
    },
    goTo(path) {
      this.$router.push(path);
    },
  },
  async mounted() {
    await this.loadSummary();
    await this.loadEpgIssueSummary();
    await this.loadActivity();
    this.startDashboardPoll();
  },
  beforeUnmount() {
    this.stopDashboardPoll();
  },
});
</script>

<style scoped>
.dashboard-card {
  border: 1px solid var(--tic-elevated-border);
  box-shadow: var(--tic-elevated-shadow);
  border-radius: var(--tic-radius-md);
}

.dashboard-card--activity {
  min-height: 360px;
}

.storage-utilization-text {
  text-align: right;
}

.channels-summary {
  padding-left: 12px;
}

.channels-issues {
  padding-left: 12px;
}
</style>
