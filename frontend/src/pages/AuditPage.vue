<template>
  <q-page>
    <div :class="$q.screen.lt.sm ? 'q-pa-none' : 'q-pa-md'">
      <q-card flat>
        <q-card-section :class="$q.platform.is.mobile ? 'q-px-none' : ''">
          <div class="row q-col-gutter-sm items-end q-mb-sm">
            <div :class="$q.screen.lt.sm ? 'col-12' : 'col-12 col-sm-6 col-md-4'">
              <TicSearchInput
                v-model="search"
                class="section-toolbar-field"
                label="Search audit logs"
                placeholder="User, endpoint, event, details..."
              />
            </div>
            <div :class="$q.screen.lt.sm ? 'col-12' : 'col-12 col-sm-4 col-md-3'">
              <TicSelectInput
                v-model="eventType"
                class="section-toolbar-field"
                label="Event"
                :options="eventTypeOptions"
                option-label="label"
                option-value="value"
                emit-value
                map-options
                clearable
              />
            </div>
          </div>

          <q-list bordered separator class="rounded-borders audit-list">
            <q-item v-for="entry in entries" :key="entry.id" class="audit-list-item" top>
              <template v-if="!$q.screen.lt.md">
                <q-item-section>
                  <q-item-label class="text-weight-medium">
                    {{ entry.activity_label || 'Other activity' }}
                  </q-item-label>
                  <q-item-label caption class="q-mt-xs">
                    {{ formatAuditTimestamp(entry.created_at) }}
                    <span class="q-mx-xs">|</span>
                    User: {{ displayUsername(entry) }}
                    <span class="q-mx-xs">|</span>
                    Device: {{ displayDevice(entry) }}
                  </q-item-label>
                  <q-item-label caption class="text-grey-7 q-mt-xs">
                    Event: {{ entry.event_type || '-' }}
                    <span v-if="entry.audit_mode" class="q-mx-xs">|</span>
                    <span v-if="entry.audit_mode">Mode: {{ formatAuditMode(entry.audit_mode) }}</span>
                    <span v-if="entry.ip_address" class="q-mx-xs">|</span>
                    <span v-if="entry.ip_address">IP: {{ entry.ip_address }}</span>
                  </q-item-label>
                  <q-item-label caption class="text-grey-7 q-mt-xs endpoint-ellipsis">
                    Endpoint: {{ entry.endpoint || '-' }}
                  </q-item-label>
                  <q-item-label v-if="entry.details" caption class="text-grey-7 q-mt-xs">
                    {{ entry.details }}
                  </q-item-label>
                </q-item-section>
              </template>

              <template v-else>
                <q-item-section>
                  <TicListItemCard>
                    <template #header-left>
                      <div class="text-weight-medium">
                        {{ entry.activity_label || 'Other activity' }}
                      </div>
                      <div class="text-caption text-grey-7">
                        {{ formatAuditTimestamp(entry.created_at) }}
                      </div>
                    </template>
                    <div class="text-caption q-mt-xs">User: {{ displayUsername(entry) }}</div>
                    <div class="text-caption">Device: {{ displayDevice(entry) }}</div>
                    <div class="text-caption">Event: {{ entry.event_type || '-' }}</div>
                    <div v-if="entry.audit_mode" class="text-caption">Mode: {{ formatAuditMode(entry.audit_mode) }}
                    </div>
                    <div class="text-caption endpoint-ellipsis">Endpoint: {{ entry.endpoint || '-' }}</div>
                    <div v-if="entry.ip_address" class="text-caption">IP: {{ entry.ip_address }}</div>
                    <div v-if="entry.details" class="text-caption text-grey-7 q-mt-xs">{{ entry.details }}</div>
                  </TicListItemCard>
                </q-item-section>
              </template>
            </q-item>

            <q-item v-if="!loading && !entries.length">
              <q-item-section>
                <q-item-label class="text-grey-7">
                  No audit logs found.
                </q-item-label>
              </q-item-section>
            </q-item>
          </q-list>

          <q-infinite-scroll
            v-if="!loading && hasMore"
            ref="infiniteRef"
            :offset="80"
            scroll-target="body"
            @load="onLoadOlder"
          >
            <template #loading>
              <div class="row flex-center q-my-md">
                <q-spinner-dots size="30px" color="primary" />
              </div>
            </template>
          </q-infinite-scroll>

          <q-inner-loading :showing="loading">
            <q-spinner-dots size="42px" color="primary" />
          </q-inner-loading>
        </q-card-section>
      </q-card>
    </div>
  </q-page>
</template>

<script>
import axios from 'axios';
import {defineComponent} from 'vue';
import {useUiStore} from 'stores/ui';
import {TicListItemCard, TicSearchInput, TicSelectInput} from 'components/ui';

const PAGE_SIZE = 50;

export default defineComponent({
  name: 'AuditPage',
  components: {
    TicListItemCard,
    TicSearchInput,
    TicSelectInput,
  },
  setup() {
    return {
      uiStore: useUiStore(),
    };
  },
  data() {
    return {
      entries: [],
      loading: false,
      hasMore: true,
      search: '',
      eventType: null,
      eventTypeOptions: [{label: 'All events', value: null}],
      pollTimer: null,
      pollCancelled: false,
      searchDebounceTimer: null,
    };
  },
  watch: {
    search() {
      this.scheduleRefresh();
    },
    eventType() {
      this.scheduleRefresh();
    },
  },
  methods: {
    fallbackDeviceLabel(userAgent) {
      const ua = String(userAgent || '').trim();
      return ua || 'Unknown';
    },
    displayDevice(entry) {
      const label = String(entry?.device_label || '').trim();
      if (label && label.toLowerCase() !== 'unknown') {
        return label;
      }
      const hasUser = String(entry?.username || '').trim() ||
        (entry?.user_id !== null && entry?.user_id !== undefined && entry?.user_id !== '');
      if (!hasUser && String(entry?.endpoint || '').startsWith('/tic-hls-proxy/')) {
        return 'TVHeadend';
      }
      return this.fallbackDeviceLabel(entry?.user_agent);
    },
    displayUsername(entry) {
      const username = String(entry?.username || '').trim();
      if (username) {
        return username;
      }
      if (entry?.user_id !== null && entry?.user_id !== undefined && entry?.user_id !== '') {
        return `User ID ${entry.user_id}`;
      }
      if (String(entry?.endpoint || '').startsWith('/tic-hls-proxy/')) {
        return 'TVH backend';
      }
      return 'Unknown user';
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
    formatAuditMode(mode) {
      if (mode === 'session_tracked') {
        return 'Session tracked';
      }
      if (mode === 'start_only') {
        return 'Start only';
      }
      return mode || '-';
    },
    buildFilterQuery() {
      const params = {
        limit: PAGE_SIZE,
      };
      if (this.search?.trim()) {
        params.search = this.search.trim();
      }
      if (this.eventType) {
        params.event_type = this.eventType;
      }
      return params;
    },
    resetState() {
      this.entries = [];
      this.hasMore = true;
    },
    updateEventTypeOptions() {
      const seen = new Set();
      const options = [{label: 'All events', value: null}];
      this.entries.forEach((entry) => {
        const event = String(entry.event_type || '').trim();
        if (!event || seen.has(event)) {
          return;
        }
        seen.add(event);
        options.push({label: event, value: event});
      });
      this.eventTypeOptions = options;
    },
    async fetchInitial() {
      this.loading = true;
      this.stopPolling();
      try {
        const response = await axios.get('/tic-api/audit/logs', {
          params: this.buildFilterQuery(),
        });
        this.entries = response.data.data || [];
        this.hasMore = (this.entries.length || 0) >= PAGE_SIZE;
        this.updateEventTypeOptions();
      } catch (error) {
        console.error('Failed to load audit logs:', error);
        this.$q.notify({color: 'negative', message: 'Failed to load audit logs'});
      } finally {
        this.loading = false;
        this.startPolling();
      }
    },
    async onLoadOlder(done) {
      if (!this.entries.length || !this.hasMore) {
        done(true);
        return;
      }
      const tail = this.entries[this.entries.length - 1];
      const params = {
        ...this.buildFilterQuery(),
        before_created_at: tail.created_at,
        before_id: tail.id,
      };
      try {
        const response = await axios.get('/tic-api/audit/logs', {params});
        const older = response.data.data || [];
        if (!older.length) {
          this.hasMore = false;
          done(true);
          return;
        }
        const existing = new Set(this.entries.map((entry) => entry.id));
        const additions = older.filter((entry) => !existing.has(entry.id));
        this.entries = [...this.entries, ...additions];
        this.hasMore = older.length >= PAGE_SIZE;
        this.updateEventTypeOptions();
        done(!this.hasMore);
      } catch (error) {
        console.error('Failed to load older audit logs:', error);
        done(true);
      }
    },
    async pollOnce() {
      if (this.pollCancelled) {
        return;
      }
      const head = this.entries[0];
      if (!head) {
        this.pollTimer = setTimeout(() => this.pollOnce(), 1000);
        return;
      }
      const params = {
        timeout: 25,
        since_created_at: head.created_at,
        since_id: head.id,
      };
      if (this.search?.trim()) {
        params.search = this.search.trim();
      }
      if (this.eventType) {
        params.event_type = this.eventType;
      }
      try {
        const response = await axios.get('/tic-api/audit/logs/poll', {params});
        const updates = response.data.data || [];
        if (updates.length) {
          const existing = new Set(this.entries.map((entry) => entry.id));
          const additions = updates.filter((entry) => !existing.has(entry.id));
          if (additions.length) {
            this.entries = [...additions, ...this.entries];
            this.updateEventTypeOptions();
          }
        }
      } catch (error) {
        console.error('Audit poll failed:', error);
      } finally {
        if (!this.pollCancelled) {
          this.pollTimer = setTimeout(() => this.pollOnce(), 50);
        }
      }
    },
    startPolling() {
      this.stopPolling();
      this.pollCancelled = false;
      this.pollTimer = setTimeout(() => this.pollOnce(), 250);
    },
    stopPolling() {
      this.pollCancelled = true;
      if (this.pollTimer) {
        clearTimeout(this.pollTimer);
        this.pollTimer = null;
      }
    },
    refreshLogs() {
      this.resetState();
      this.fetchInitial();
    },
    scheduleRefresh() {
      if (this.searchDebounceTimer) {
        clearTimeout(this.searchDebounceTimer);
      }
      this.searchDebounceTimer = setTimeout(() => {
        this.refreshLogs();
      }, 350);
    },
  },
  mounted() {
    this.fetchInitial();
  },
  beforeUnmount() {
    this.stopPolling();
    if (this.searchDebounceTimer) {
      clearTimeout(this.searchDebounceTimer);
      this.searchDebounceTimer = null;
    }
  },
});
</script>

<style scoped>
.audit-list-item {
  align-items: flex-start;
}

.endpoint-ellipsis {
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}
</style>
