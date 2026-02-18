<template>
  <TicDialogWindow
    v-model="isOpen"
    :title="dialogTitle"
    width="1100px"
    close-tooltip="Close"
    @hide="onDialogHide"
  >
    <div class="epg-review-dialog">
      <div class="selector-toolbar bg-card-head">
        <div class="row q-col-gutter-sm items-end">
          <div :class="$q.screen.lt.sm ? 'col-12' : 'col-12 col-sm-6 col-md-4'">
            <TicSearchInput
              v-model="searchValue"
              class="section-toolbar-field"
              label="Search channels"
              placeholder="Channel name or channel ID"
              :debounce="300"
              :clearable="true"
            />
          </div>
          <div :class="$q.screen.lt.sm ? 'col-12' : 'col-12 col-sm-6 col-md-3'">
            <TicSelectInput
              v-model="hasDataFilter"
              class="section-toolbar-field"
              label="Coverage"
              :options="coverageFilterOptions"
              option-label="label"
              option-value="value"
              :emit-value="true"
              :map-options="true"
              :clearable="false"
              :dense="true"
              :behavior="$q.screen.lt.md ? 'dialog' : 'menu'"
            />
          </div>
        </div>

        <div class="row q-col-gutter-xs q-mt-sm">
          <div class="col-auto">
            <q-chip dense color="primary" text-color="white">Channels: {{ summary.total_channels }}</q-chip>
          </div>
          <div class="col-auto">
            <q-chip dense color="positive" text-color="white">With data: {{ summary.channels_with_future_data }}
            </q-chip>
          </div>
          <div class="col-auto">
            <q-chip dense color="warning" text-color="black">No data: {{ summary.channels_without_future_data }}
            </q-chip>
          </div>
          <div class="col-auto">
            <q-chip dense color="secondary" text-color="white">Programmes: {{ summary.total_programmes }}</q-chip>
          </div>
        </div>
      </div>

      <div id="epg-review-scroll" class="selector-scroll">
        <q-infinite-scroll
          ref="infiniteScrollRef"
          :disable="allLoaded || loadingMore"
          :offset="160"
          scroll-target="#epg-review-scroll"
          @load="loadMore"
        >
          <q-list v-if="rows.length" separator bordered class="rounded-borders selector-channel-list">
            <q-item v-for="row in rows" :key="row.epg_channel_row_id" top>
              <q-item-section avatar>
                <div class="stream-logo-wrap">
                  <img v-if="row.icon_url" :src="row.icon_url" alt="logo" class="stream-logo-img" />
                  <q-icon v-else name="tv" size="18px" color="grey-6" />
                </div>
              </q-item-section>

              <q-item-section>
                <q-item-label class="text-weight-medium">{{ row.name || row.channel_id }}</q-item-label>
                <q-item-label caption>
                  <span class="text-weight-medium">Channel ID:</span> {{ row.channel_id || '-' }}
                  <span class="q-mx-xs">•</span>
                  <span class="text-weight-medium">Now->future:</span> {{ row.programmes_now_to_future }}
                  <span class="q-mx-xs">•</span>
                  <span class="text-weight-medium">Total:</span> {{ row.total_programmes }}
                </q-item-label>
                <q-item-label caption>
                  <span class="text-weight-medium">Coverage horizon:</span>
                  {{ formatHorizon(row.future_horizon_hours) }}
                </q-item-label>

                <q-item-label caption class="q-mt-xs">
                  <span class="text-weight-medium">Now:</span>
                  {{ formatNowProgramme(row.now_programme) }}
                </q-item-label>

                <q-item-label caption>
                  <span class="text-weight-medium">Next:</span>
                  {{ formatUpcoming(row.next_programmes) }}
                </q-item-label>
              </q-item-section>

              <q-item-section side top>
                <q-chip
                  dense
                  :color="row.has_future_data ? 'positive' : 'warning'"
                  :text-color="row.has_future_data ? 'white' : 'black'"
                >
                  {{ row.has_future_data ? 'Has Data' : 'No Data' }}
                </q-chip>
              </q-item-section>
            </q-item>
          </q-list>

          <template #loading>
            <div class="row flex-center q-my-md">
              <q-spinner-dots size="30px" color="primary" />
            </div>
          </template>
        </q-infinite-scroll>

        <div v-if="!loadingInitial && !rows.length" class="selector-empty">
          <q-icon name="search_off" size="2em" color="grey-6" />
          <div>No channels found for the current filters.</div>
        </div>

        <q-inner-loading :showing="loadingInitial">
          <q-spinner-dots size="42px" color="primary" />
        </q-inner-loading>
      </div>
    </div>
  </TicDialogWindow>
</template>

<script>
import axios from 'axios';
import {
  TicDialogWindow,
  TicSearchInput,
  TicSelectInput,
} from 'components/ui';

const PAGE_SIZE = 100;

export default {
  name: 'EpgReviewDialog',
  components: {
    TicDialogWindow,
    TicSearchInput,
    TicSelectInput,
  },
  props: {
    epgId: {
      type: [String, Number],
      required: true,
    },
    epgName: {
      type: String,
      default: '',
    },
  },
  emits: ['hide'],
  data() {
    return {
      isOpen: false,
      rows: [],
      loadingInitial: false,
      loadingMore: false,
      totalMatchingCount: 0,
      loadOffset: 0,
      searchValue: '',
      hasDataFilter: 'any',
      summary: {
        total_channels: 0,
        channels_with_future_data: 0,
        channels_without_future_data: 0,
        total_programmes: 0,
      },
    };
  },
  computed: {
    dialogTitle() {
      const base = this.epgName || `EPG ${this.epgId}`;
      return `Review EPG: ${base}`;
    },
    coverageFilterOptions() {
      return [
        {label: 'All channels', value: 'any'},
        {label: 'With guide data', value: 'with_data'},
        {label: 'Without guide data', value: 'without_data'},
      ];
    },
    allLoaded() {
      if (this.totalMatchingCount < 1) {
        return false;
      }
      return this.loadOffset >= this.totalMatchingCount;
    },
  },
  watch: {
    searchValue() {
      this.resetAndReload();
    },
    hasDataFilter() {
      this.resetAndReload();
    },
  },
  methods: {
    show() {
      this.isOpen = true;
      this.resetState();
      this.fetchInitial();
    },
    hide() {
      this.isOpen = false;
    },
    onDialogHide() {
      this.$emit('hide');
    },
    resetState() {
      this.rows = [];
      this.loadOffset = 0;
      this.totalMatchingCount = 0;
      this.summary = {
        total_channels: 0,
        channels_with_future_data: 0,
        channels_without_future_data: 0,
        total_programmes: 0,
      };
    },
    async fetchInitial() {
      this.loadingInitial = true;
      try {
        const payload = await this.fetchPage(0);
        this.rows = payload.rows || [];
        this.loadOffset = this.rows.length;
        this.totalMatchingCount = Number(payload.total_count || 0);
        this.summary = payload.summary || this.summary;
      } catch (error) {
        console.error('Failed to load EPG review data:', error);
        this.$q.notify({
          color: 'negative',
          message: 'Failed to load EPG review data',
          icon: 'report_problem',
        });
      } finally {
        this.loadingInitial = false;
      }
    },
    async loadMore(index, done) {
      if (this.loadingMore || this.allLoaded) {
        done();
        return;
      }
      this.loadingMore = true;
      try {
        const payload = await this.fetchPage(this.loadOffset);
        const nextRows = payload.rows || [];
        this.rows = this.rows.concat(nextRows);
        this.loadOffset = this.rows.length;
        this.totalMatchingCount = Number(payload.total_count || this.totalMatchingCount);
        this.summary = payload.summary || this.summary;
      } catch (error) {
        console.error('Failed to load more EPG review rows:', error);
        this.$q.notify({
          color: 'negative',
          message: 'Failed to load more channels',
          icon: 'report_problem',
        });
      } finally {
        this.loadingMore = false;
        done();
      }
    },
    async fetchPage(offset) {
      const response = await axios.get(`/tic-api/epgs/review/${this.epgId}/channels`, {
        params: {
          offset,
          limit: PAGE_SIZE,
          search: this.searchValue?.trim() || undefined,
          has_data: this.hasDataFilter,
        },
      });
      return response.data?.data || {rows: [], total_count: 0, summary: this.summary};
    },
    resetAndReload() {
      if (!this.isOpen) {
        return;
      }
      this.resetState();
      this.fetchInitial();
      this.$nextTick(() => {
        if (this.$refs.infiniteScrollRef && typeof this.$refs.infiniteScrollRef.reset === 'function') {
          this.$refs.infiniteScrollRef.reset();
        }
      });
    },
    formatTime(ts) {
      if (!ts) {
        return '';
      }
      try {
        return new Date(Number(ts) * 1000).toLocaleString();
      } catch {
        return '';
      }
    },
    formatNowProgramme(nowProgramme) {
      if (!nowProgramme) {
        return 'No programme currently scheduled';
      }
      return `${nowProgramme.title || '(Untitled)'} (${this.formatTime(nowProgramme.start_ts)} - ${this.formatTime(
        nowProgramme.stop_ts)})`;
    },
    formatUpcoming(nextProgrammes) {
      const rows = Array.isArray(nextProgrammes) ? nextProgrammes.filter(Boolean) : [];
      if (!rows.length) {
        return 'No upcoming programme data';
      }
      return rows.slice(0, 3).
        map((programme) => `${programme.title || '(Untitled)'} @ ${this.formatTime(programme.start_ts)}`).
        join(' | ');
    },
    formatHorizon(hours) {
      if (hours === null || hours === undefined) {
        return 'No future data';
      }
      if (hours < 24) {
        return `${hours}h`;
      }
      const days = (Number(hours) / 24).toFixed(1);
      return `${days}d`;
    },
  },
};
</script>

<style scoped>
.epg-review-dialog {
  display: flex;
  flex-direction: column;
  gap: 12px;
}

.selector-toolbar {
  padding: 12px;
  border-radius: var(--tic-radius-md);
}

.selector-scroll {
  max-height: calc(100vh - 220px);
  overflow-y: auto;
  overflow-x: hidden;
}

.selector-empty {
  min-height: 180px;
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  color: var(--q-grey-7);
  gap: 8px;
  text-align: center;
}

.stream-logo-wrap {
  width: 42px;
  height: 42px;
  border-radius: var(--tic-radius-sm);
  border: 1px solid rgba(0, 0, 0, 0.1);
  display: flex;
  align-items: center;
  justify-content: center;
  background: rgba(0, 0, 0, 0.03);
  overflow: hidden;
}

.stream-logo-img {
  width: 100%;
  height: 100%;
  object-fit: contain;
}

@media (max-width: 599px) {
  .selector-toolbar {
    padding: 10px;
  }

  .selector-scroll {
    max-height: calc(100vh - 210px);
  }
}
</style>
