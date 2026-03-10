<template>
  <q-page>
    <div :class="$q.screen.lt.sm ? 'q-pa-none' : 'q-pa-md'">
      <q-card flat>
        <q-card-section :class="$q.platform.is.mobile ? 'q-px-none' : ''">
          <TicListToolbar
            class="q-mb-sm audit-toolbar"
            :search="{label: 'Search audit logs', placeholder: 'User, endpoint, event, details...'}"
            :search-value="search"
            :filters="auditToolbarFilters"
            :collapse-filters-on-mobile="false"
            @update:search-value="search = $event"
            @filter-change="onAuditToolbarFilterChange"
          />
          <div class="row q-col-gutter-sm q-mb-sm">
            <div class="col-12 col-md-3">
              <q-input
                v-model="fromTsInput"
                dense
                outlined
                type="datetime-local"
                label="From"
              />
            </div>
            <div class="col-12 col-md-3">
              <q-input
                v-model="toTsInput"
                dense
                outlined
                type="datetime-local"
                label="To"
              />
            </div>
            <div class="col-12 col-md-6 flex items-center">
              <div class="text-caption text-grey-7">
                Set a time range to load all matching logs without infinite scrolling.
              </div>
            </div>
          </div>

          <q-list class="audit-list">
            <q-item v-for="entry in entries" :key="entry.entry_key || `${entry.entry_type}:${entry.id}`"
                    class="audit-list-item" top>
              <q-item-section>
                <TicListItemCard v-bind="entryCardProps(entry)" :hide-header="true">
                  <div class="text-weight-medium">
                    {{ displayAuditTitle(entry) }}
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
            v-if="!loading && hasMore && !isTimeRangeMode"
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
import {useSettingsStore} from 'stores/settings';
import {useUiStore} from 'stores/ui';
import {TicListItemCard, TicListToolbar} from 'components/ui';
import {getAuditActivityTitle} from '../utils/auditActivity';

const PAGE_SIZE = 50;

export default defineComponent({
  name: 'AuditPage',
  components: {
    TicListItemCard,
    TicListToolbar,
  },
  setup() {
    return {
      settingsStore: useSettingsStore(),
      uiStore: useUiStore(),
    };
  },
  data() {
    return {
      entries: [],
      loading: false,
      hasMore: true,
      search: '',
      entryType: [],
      eventType: [],
      severity: [],
      fromTsInput: '',
      toTsInput: '',
      channelIdFilter: null,
      eventTypeOptions: [],
      eventOptionsLoading: false,
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
    severity() {
      this.scheduleRefresh();
    },
    fromTsInput() {
      this.scheduleRefresh();
    },
    toTsInput() {
      this.scheduleRefresh();
    },
  },
  computed: {
    isTimeRangeMode() {
      return Boolean((this.fromTsInput || '').trim() || (this.toTsInput || '').trim());
    },
    appDebugEnabled() {
      return Boolean(this.settingsStore.appDebugEnabled);
    },
    severityOptions() {
      const options = [
        {label: 'Info', value: 'info'},
        {label: 'Warning', value: 'warning'},
        {label: 'Error', value: 'error'},
      ];
      if (this.appDebugEnabled) {
        options.splice(1, 0, {label: 'Debug', value: 'debug'});
      }
      return options;
    },
    effectiveSeverityFilter() {
      if (this.appDebugEnabled) {
        return this.severity.length ? this.severity.join(',') : null;
      }
      if (this.severity.includes('debug')) {
        return 'info,warning,error';
      }
      return this.severity.length ? this.severity.join(',') : 'info,warning,error';
    },
    auditToolbarFilters() {
      return [
        {
          key: 'entryType',
          modelValue: this.entryType,
          label: 'Filter by Type',
          options: [
            {label: 'User Stream Audit Logs', value: 'stream_audit'},
            {label: 'CSO Events', value: 'cso_event_log'},
          ],
          optionLabel: 'label',
          optionValue: 'value',
          emitValue: true,
          mapOptions: true,
          clearable: true,
          multiple: true,
          collapseSelections: true,
          dense: true,
          behavior: this.$q.screen.lt.md ? 'dialog' : 'menu',
        },
        {
          key: 'event',
          modelValue: this.eventType,
          label: 'Filter by Event',
          options: this.eventTypeOptions,
          optionLabel: 'label',
          optionValue: 'value',
          emitValue: true,
          mapOptions: true,
          clearable: true,
          multiple: true,
          collapseSelections: true,
          loading: this.eventOptionsLoading,
          dense: true,
          behavior: this.$q.screen.lt.md ? 'dialog' : 'menu',
        },
        {
          key: 'severity',
          modelValue: this.severity,
          label: 'Filter by Severity',
          options: this.severityOptions,
          optionLabel: 'label',
          optionValue: 'value',
          emitValue: true,
          mapOptions: true,
          clearable: true,
          multiple: true,
          collapseSelections: true,
          dense: true,
          behavior: this.$q.screen.lt.md ? 'dialog' : 'menu',
        },
      ];
    },
  },
  methods: {
    displayAuditTitle(entry) {
      return getAuditActivityTitle(entry);
    },
    onAuditToolbarFilterChange({key, value}) {
      if (key === 'entryType') {
        this.entryType = Array.isArray(value) ? value : [];
      }
      if (key === 'event') {
        this.eventType = Array.isArray(value) ? value : [];
      }
      if (key === 'severity') {
        this.severity = Array.isArray(value) ? value : [];
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
      if (severity === 'debug') {
        return {
          accentColor: 'var(--tic-list-card-default-border, transparent)',
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
      if (this.channelIdFilter !== null && this.channelIdFilter !== undefined && this.channelIdFilter !== '') {
        params.channel_id = this.channelIdFilter;
      }
      if (this.search?.trim()) {
        params.search = this.search.trim();
      }
      if (this.eventType.length) {
        params.event_type = this.eventType.join(',');
      }
      if (this.effectiveSeverityFilter) {
        params.severity = this.effectiveSeverityFilter;
      }
      if (this.entryType.length) {
        params.entry_types = this.entryType.join(',');
      }
      const fromTsIso = this.localInputToIso(this.fromTsInput);
      const toTsIso = this.localInputToIso(this.toTsInput);
      if (fromTsIso) {
        params.from_ts = fromTsIso;
      }
      if (toTsIso) {
        params.to_ts = toTsIso;
      }
      if (this.isTimeRangeMode) {
        params.include_all = 1;
        params.limit = 5000;
      }
      return params;
    },
    resetState() {
      this.entries = [];
      this.hasMore = true;
    },
    updateEventTypeOptions(eventTypes = []) {
      const selected = Array.isArray(this.eventType) ? this.eventType : [];
      const seen = new Set();
      const values = [];
      [...selected, ...eventTypes].forEach((event) => {
        const value = String(event || '').trim();
        if (!value || seen.has(value)) {
          return;
        }
        seen.add(value);
        values.push(value);
      });
      this.eventTypeOptions = values.map((event) => ({label: event, value: event}));
    },
    async refreshEventTypeOptions() {
      this.eventOptionsLoading = true;
      try {
        const params = {
          ...this.buildFilterQuery(),
        };
        delete params.event_type;
        const response = await axios.get('/tic-api/audit/filter-options', {params});
        this.updateEventTypeOptions(response?.data?.data?.event_types || []);
      } catch (error) {
        console.error('Failed to load audit filter options:', error);
        this.updateEventTypeOptions();
      } finally {
        this.eventOptionsLoading = false;
      }
    },
    async fetchInitial() {
      this.loading = true;
      this.stopPolling();
      try {
        await this.refreshEventTypeOptions();
        const response = await axios.get('/tic-api/audit/logs', {
          params: this.buildFilterQuery(),
        });
        this.entries = response.data.data || [];
        this.hasMore = this.isTimeRangeMode ? false : (this.entries.length || 0) >= PAGE_SIZE;
      } catch (error) {
        console.error('Failed to load audit logs:', error);
        this.$q.notify({color: 'negative', message: 'Failed to load audit logs'});
      } finally {
        this.loading = false;
        this.startPolling();
      }
    },
    async onLoadOlder(done) {
      if (this.isTimeRangeMode || !this.entries.length || !this.hasMore) {
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
      if (this.pollCancelled || this.isTimeRangeMode) {
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
      if (this.eventType.length) {
        params.event_type = this.eventType.join(',');
      }
      if (this.effectiveSeverityFilter) {
        params.severity = this.effectiveSeverityFilter;
      }
      if (this.entryType.length) {
        params.entry_types = this.entryType.join(',');
      }
      if (this.channelIdFilter !== null && this.channelIdFilter !== undefined && this.channelIdFilter !== '') {
        params.channel_id = this.channelIdFilter;
      }
      try {
        const response = await axios.get('/tic-api/audit/logs/poll', {params});
        const updates = response.data.data || [];
        if (updates.length) {
          const existing = new Set(this.entries.map((entry) => entry.entry_key || `${entry.entry_type}:${entry.id}`));
          const additions = updates.filter(
            (entry) => !existing.has(entry.entry_key || `${entry.entry_type}:${entry.id}`));
          if (additions.length) {
            this.entries = [...additions, ...this.entries];
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
      if (this.isTimeRangeMode) {
        return;
      }
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
    isoToLocalInput(value) {
      const raw = String(value || '').trim();
      if (!raw) return '';
      const parsed = new Date(raw);
      if (Number.isNaN(parsed.getTime())) return '';
      const pad = (n) => String(n).padStart(2, '0');
      const yyyy = parsed.getFullYear();
      const mm = pad(parsed.getMonth() + 1);
      const dd = pad(parsed.getDate());
      const hh = pad(parsed.getHours());
      const min = pad(parsed.getMinutes());
      return `${yyyy}-${mm}-${dd}T${hh}:${min}`;
    },
    localInputToIso(value) {
      const raw = String(value || '').trim();
      if (!raw) return '';
      const parsed = new Date(raw);
      if (Number.isNaN(parsed.getTime())) return '';
      return parsed.toISOString();
    },
    applyRouteQueryDefaults() {
      const query = this.$route?.query || {};
      if (typeof query.search === 'string') {
        this.search = query.search;
      }
      if (typeof query.event_type === 'string') {
        this.eventType = query.event_type ? query.event_type.split(',').filter(Boolean) : [];
      }
      if (typeof query.severity === 'string') {
        this.severity = query.severity ? query.severity.split(',').filter(Boolean) : [];
      }
      if (typeof query.entry_types === 'string') {
        this.entryType = query.entry_types ? query.entry_types.split(',').filter(Boolean) : [];
      }
      if (query.channel_id !== undefined) {
        const parsed = Number.parseInt(String(query.channel_id), 10);
        this.channelIdFilter = Number.isNaN(parsed) ? null : parsed;
      }
      if (typeof query.from_ts === 'string') {
        this.fromTsInput = this.isoToLocalInput(query.from_ts);
      }
      if (typeof query.to_ts === 'string') {
        this.toTsInput = this.isoToLocalInput(query.to_ts);
      }
    },
  },
  async mounted() {
    try {
      await this.settingsStore.refreshSettings({minAgeMs: 3000});
    } catch (error) {
      console.error('Failed to load application settings:', error);
    }
    this.applyRouteQueryDefaults();
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
.audit-toolbar :deep(.section-toolbar-row) {
  display: grid;
  grid-template-columns: auto auto;
  align-items: end;
  gap: 8px 16px;
  justify-content: space-between;
}

.audit-toolbar :deep(.section-toolbar-left) {
  min-width: 0;
}

.audit-toolbar :deep(.section-toolbar-right) {
  flex-wrap: nowrap;
  justify-content: flex-end;
  gap: 8px;
}

.audit-toolbar :deep(.section-toolbar-search-wrap) {
  width: 360px;
  min-width: 280px;
  max-width: 420px;
  flex: 0 1 360px;
}

.audit-toolbar :deep(.section-toolbar-filter-wrap) {
  min-width: 150px;
  max-width: 190px;
  flex: 0 0 176px;
}

.audit-toolbar :deep(.section-toolbar-field .q-field) {
  margin-bottom: 0;
}

.audit-list-item {
  align-items: flex-start;
}

.endpoint-ellipsis {
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

@media (max-width: 1023px) {
  .audit-toolbar :deep(.section-toolbar-row) {
    grid-template-columns: 1fr;
  }

  .audit-toolbar :deep(.section-toolbar-right) {
    flex-wrap: wrap;
    justify-content: flex-start;
  }

  .audit-toolbar :deep(.section-toolbar-filter-wrap) {
    min-width: 180px;
    max-width: none;
    flex: 1 1 220px;
  }
}

@media (max-width: 599px) {
  .audit-toolbar :deep(.section-toolbar-right) {
    width: 100%;
  }
}
</style>
