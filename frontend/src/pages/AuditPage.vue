<template>
  <q-page>
    <div :class="$q.screen.lt.sm ? 'q-pa-none' : 'q-pa-md'">
      <q-card flat>
        <q-card-section :class="$q.platform.is.mobile ? 'q-px-none' : ''">
          <TicListToolbar
            class="q-mb-sm"
            :search="{label: 'Search audit logs', placeholder: 'User, endpoint, event, details...'}"
            :search-value="search"
            :filters="auditToolbarFilters"
            :collapse-filters-on-mobile="false"
            @update:search-value="search = $event"
            @filter-change="onAuditToolbarFilterChange"
          />

          <q-list class="audit-list">
            <q-item v-for="entry in entries" :key="entry.entry_key || `${entry.entry_type}:${entry.id}`" class="audit-list-item" top>
              <q-item-section>
                <TicListItemCard v-bind="entryCardProps(entry)" :hide-header="true">
                  <div class="text-weight-medium">
                    {{ entry.activity_label || 'Other activity' }}
                  </div>
                  <div class="text-caption text-grey-7 q-mt-xs">
                    {{ formatAuditTimestamp(entry.created_at) }}
                    <span class="q-mx-xs">|</span>
                    User: {{ displayUsername(entry) }}
                    <span class="q-mx-xs">|</span>
                    Device: {{ displayDevice(entry) }}
                  </div>
                  <div class="text-caption text-grey-7 q-mt-xs">
                    Type: {{ formatEntryType(entry.entry_type) }}
                    <span class="q-mx-xs">|</span>
                    Event: {{ entry.event_type || '-' }}
                    <span v-if="entry.audit_mode" class="q-mx-xs">|</span>
                    <span v-if="entry.audit_mode">Mode: {{ formatAuditMode(entry.audit_mode) }}</span>
                    <span v-if="entry.channel_id" class="q-mx-xs">|</span>
                    <span v-if="entry.channel_id">Channel: {{ entry.channel_id }}</span>
                    <span v-if="entry.severity" class="q-mx-xs">|</span>
                    <span v-if="entry.severity">Severity: {{ entry.severity }}</span>
                    <span v-if="entry.ip_address" class="q-mx-xs">|</span>
                    <span v-if="entry.ip_address">IP: {{ entry.ip_address }}</span>
                  </div>
                  <div class="text-caption text-grey-7 q-mt-xs endpoint-ellipsis">
                    Endpoint: {{ entry.endpoint || '-' }}
                  </div>
                  <div v-if="entry.details" class="text-caption text-grey-7 q-mt-xs">
                    {{ entry.details }}
                  </div>
                </TicListItemCard>
              </q-item-section>
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
import {TicListItemCard, TicListToolbar} from 'components/ui';

const PAGE_SIZE = 50;

export default defineComponent({
  name: 'AuditPage',
  components: {
    TicListItemCard,
    TicListToolbar,
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
      entryType: null,
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
    entryType() {
      this.scheduleRefresh();
    },
  },
  computed: {
    auditToolbarFilters() {
      return [
        {
          key: 'entryType',
          modelValue: this.entryType,
          label: 'Type',
          options: [
            {label: 'All activity', value: null},
            {label: 'User Stream Audit Logs', value: 'stream_audit'},
            {label: 'CSO Events', value: 'cso_event_log'},
          ],
          optionLabel: 'label',
          optionValue: 'value',
          emitValue: true,
          mapOptions: true,
          clearable: true,
          dense: true,
          behavior: this.$q.screen.lt.md ? 'dialog' : 'menu',
        },
        {
          key: 'event',
          modelValue: this.eventType,
          label: 'Event',
          options: this.eventTypeOptions,
          optionLabel: 'label',
          optionValue: 'value',
          emitValue: true,
          mapOptions: true,
          clearable: true,
          dense: true,
          behavior: this.$q.screen.lt.md ? 'dialog' : 'menu',
        },
      ];
    },
  },
  methods: {
    onAuditToolbarFilterChange({key, value}) {
      if (key === 'entryType') {
        this.entryType = value ?? null;
      }
      if (key === 'event') {
        this.eventType = value ?? null;
      }
    },
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
      if (entry?.entry_type === 'cso_event_log') {
        return 'System';
      }
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
    formatEntryType(value) {
      if (value === 'cso_event_log') {
        return 'CSO Event';
      }
      if (value === 'stream_audit') {
        return 'User Stream Audit Log';
      }
      return value || '-';
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
    entryCardProps(entry) {
      const severity = String(entry?.severity || '').trim().toLowerCase();
      if (severity === 'error') {
        return {
          accentColor: 'var(--tic-list-card-error-border)',
          surfaceColor: 'var(--tic-list-card-error-bg)',
          headerColor: 'var(--tic-list-card-error-header)',
        };
      }
      if (severity === 'warning') {
        return {
          accentColor: 'var(--tic-list-card-issues-border)',
          surfaceColor: 'var(--tic-list-card-issues-bg)',
          headerColor: 'var(--tic-list-card-issues-header)',
        };
      }
      if (severity === 'info') {
        return {
          accentColor: 'transparent',
          surfaceColor: 'var(--tic-list-card-default-bg)',
          headerColor: 'var(--tic-list-card-default-header-bg)',
        };
      }
      return {
        accentColor: 'var(--tic-list-card-default-border, transparent)',
        surfaceColor: 'var(--tic-list-card-default-bg)',
        headerColor: 'var(--tic-list-card-default-header-bg)',
      };
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
      if (this.entryType) {
        params.entry_types = this.entryType;
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
        before_entry_type: tail.entry_type,
      };
      try {
        const response = await axios.get('/tic-api/audit/logs', {params});
        const older = response.data.data || [];
        if (!older.length) {
          this.hasMore = false;
          done(true);
          return;
        }
        const existing = new Set(this.entries.map((entry) => entry.entry_key || `${entry.entry_type}:${entry.id}`));
        const additions = older.filter((entry) => !existing.has(entry.entry_key || `${entry.entry_type}:${entry.id}`));
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
        since_entry_type: head.entry_type,
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
          const existing = new Set(this.entries.map((entry) => entry.entry_key || `${entry.entry_type}:${entry.id}`));
          const additions = updates.filter((entry) => !existing.has(entry.entry_key || `${entry.entry_type}:${entry.id}`));
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
