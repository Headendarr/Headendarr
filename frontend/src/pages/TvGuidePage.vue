<template>
  <q-page padding>
    <q-card flat>
      <q-card-section class="row items-center q-gutter-md">
        <div class="text-h5">TV Guide</div>
        <q-space />
        <q-input
          dense
          outlined
          v-model="searchQuery"
          placeholder="Search channels"
          style="min-width: 220px"
        />
        <q-select
          dense
          outlined
          v-model="selectedGroup"
          :options="groupOptions"
          option-label="label"
          option-value="value"
          emit-value
          map-options
          label="Channel group"
          style="min-width: 220px"
        />
        <q-btn color="primary" label="Refresh" @click="fetchGuide" />
      </q-card-section>

      <q-separator />

      <q-card-section class="q-pa-none">
        <div class="guide">
          <div class="guide__header">
            <div class="guide__channel-col">Channel</div>
            <div class="guide__timeline-col">
              <div
                class="guide__scroll guide__scroll--draggable"
                ref="scrollHeader"
              >
                <div class="guide__timeline" :style="{width: timelineWidth + 'px'}">
                  <div
                    v-for="slot in timeSlots"
                    :key="slot.left"
                    class="guide__tick"
                    :class="{'guide__tick--hour': slot.isHour}"
                    :style="{left: slot.left + 'px', width: slot.width + 'px'}"
                  >
                    {{ slot.label }}
                  </div>
                </div>
              </div>
            </div>
          </div>

          <div class="guide__body" ref="guideBody">
            <div
              v-for="channel in filteredChannels"
              :key="channel.id"
              class="guide__row"
              :style="{height: rowHeight(channel) + 'px'}"
            >
              <div class="guide__channel-col">
                <div class="guide__channel-row">
                  <div
                    class="guide__channel-logo-wrap"
                    @click.stop="previewChannel(channel)"
                    @mouseenter="hoveredChannelId = channel.id"
                    @mouseleave="hoveredChannelId = null"
                  >
                    <img
                      v-if="channel.logo_url"
                      class="guide__channel-logo"
                      :src="channel.logo_url"
                      alt=""
                    />
                    <q-icon
                      v-else
                      name="play_arrow"
                      size="20px"
                      class="guide__channel-play-icon"
                    />
                    <div
                      class="guide__channel-logo-overlay"
                      :class="{'guide__channel-logo-overlay--active': hoveredChannelId === channel.id}"
                    >
                      <q-icon name="play_arrow" size="22px" />
                    </div>
                  </div>
                  <div class="guide__channel-meta">
                    <div class="text-weight-medium">{{ channel.name }}</div>
                    <div class="text-caption text-grey-7">#{{ channel.number }}</div>
                  </div>
                </div>
              </div>
              <div class="guide__timeline-col">
                <div class="guide__scroll" ref="scrollBody" @scroll="syncScroll">
                  <div
                    class="guide__timeline"
                    :style="{width: timelineWidth + 'px', height: rowHeight(channel) + 'px'}"
                  >
                    <div
                      v-if="nowLineVisible"
                      class="guide__now-line"
                      :style="{left: nowLineLeft + 'px'}"
                    />
                    <div
                      v-for="programme in programmesByChannel[channel.id]"
                      :key="programme.id"
                      :ref="(el) => setProgrammeRef(programme.id, el)"
                      class="guide__program"
                      :class="programmeClass(programme, channel)"
                      :style="programmeStyle(programme)"
                      @click="toggleProgramme(programme)"
                    >
                      <div
                        v-if="getRecordingForProgramme(programme, channel)"
                        class="guide__program-badge"
                        :class="recordingBadgeClass(getRecordingForProgramme(programme, channel))"
                      >
                        {{ recordingBadgeLabel(getRecordingForProgramme(programme, channel)) }}
                      </div>
                      <div class="guide__program-title">{{ programme.title }}</div>
                      <div class="guide__program-time">
                        {{ formatTime(programme.start_ts) }} - {{ formatTime(programme.stop_ts) }}
                      </div>
                      <div v-if="expandedProgramId === programme.id" class="guide__program-details">
                        <div class="guide__program-desc">
                          {{ programme.desc || 'No description available.' }}
                        </div>
                        <div class="guide__program-actions">
                          <q-btn
                            dense
                            flat
                            icon="play_arrow"
                            label="Watch now"
                            v-if="isLive(programme)"
                            @click.stop="previewChannel(channel)"
                          />
                          <q-btn
                            v-if="!getRecordingForProgramme(programme, channel)"
                            dense
                            flat
                            icon="fiber_manual_record"
                            label="Record"
                            @click.stop="recordProgramme(channel, programme)"
                          />
                          <q-btn
                            v-if="getRecordingForProgramme(programme, channel)"
                            dense
                            flat
                            icon="cancel"
                            label="Cancel Recording"
                            @click.stop="cancelProgrammeRecording(getRecordingForProgramme(programme, channel))"
                          />
                          <q-btn
                            dense
                            flat
                            icon="repeat"
                            label="Record series"
                            @click.stop="recordSeries(channel, programme)"
                          />
                          <q-btn
                            dense
                            flat
                            icon="link"
                            label="Copy stream URL"
                            v-if="isLive(programme)"
                            @click.stop="copyStreamUrl(channel)"
                          />
                        </div>
                      </div>
                    </div>
                  </div>
                </div>
              </div>
            </div>

            <div v-if="!loading && filteredChannels.length === 0" class="q-pa-lg text-grey-6">
              No channels with EPG data found.
            </div>
          </div>
        </div>
      </q-card-section>
    </q-card>
  </q-page>
</template>

<script>
import {defineComponent, ref, computed, onMounted, onBeforeUnmount, nextTick} from 'vue';
import axios from 'axios';
import {useVideoStore} from 'stores/video';
import {useQuasar} from 'quasar';

export default defineComponent({
  name: 'TvGuidePage',
  setup() {
    const videoStore = useVideoStore();
    const $q = useQuasar();
    const loading = ref(false);
    const searchQuery = ref('');
    const selectedGroup = ref('all');
    const channels = ref([]);
    const programmes = ref([]);
    const recordings = ref([]);
    const hoveredChannelId = ref(null);
    const expandedProgramId = ref(null);
    const expandedSizes = ref({});
    const programmeRefs = new Map();
    const guideBody = ref(null);
    const now = Math.floor(Date.now() / 1000);
    const nowTs = ref(now);
    const quarterSeconds = 15 * 60;
    const minStartTs = Math.floor(now / quarterSeconds) * quarterSeconds;
    const startTs = ref(minStartTs);
    const endTs = ref(startTs.value + 6 * 3600);
    const scrollHeader = ref(null);
    const scrollBody = ref([]);
    const draggingHeader = ref(false);
    const dragStartX = ref(0);
    const dragStartScrollLeft = ref(0);
    const loadingMore = ref(false);
    const syncingScroll = ref(false);
    const headerEventsBound = ref(false);
    const scrollSyncHandle = ref(null);
    const lastHeaderLeft = ref(0);
    const headerEventHandlers = ref({});
    const nowTimerId = ref(null);

    const pxPerMinute = 6;
    const extendWindowSeconds = 6 * 3600;
    const scrollEdgeThreshold = 200;
    const tickMinutes = 15;

    const timelineWidth = computed(() => {
      return Math.max(600, ((endTs.value - startTs.value) / 60) * pxPerMinute);
    });

    const timeSlots = computed(() => {
      const items = [];
      const start = new Date(startTs.value * 1000);
      const end = new Date(endTs.value * 1000);
      const current = new Date(start);
      current.setSeconds(0, 0);
      current.setMinutes(Math.floor(current.getMinutes() / 15) * 15);
      while (current <= end) {
        const diffMinutes = (current.getTime() / 1000 - startTs.value) / 60;
        const isHour = current.getMinutes() === 0;
        items.push({
          label: isHour ? current.toLocaleTimeString([], {hour: '2-digit', minute: '2-digit'}) : '',
          left: diffMinutes * pxPerMinute,
          width: tickMinutes * pxPerMinute,
          isHour,
        });
        current.setMinutes(current.getMinutes() + tickMinutes);
      }
      return items;
    });

    const programmesByChannel = computed(() => {
      const map = {};
      for (const channel of channels.value) {
        map[channel.id] = [];
      }
      for (const programme of programmes.value) {
        if (!map[programme.channel_id]) {
          map[programme.channel_id] = [];
        }
        map[programme.channel_id].push(programme);
      }
      for (const key of Object.keys(map)) {
        map[key].sort((a, b) => a.start_ts - b.start_ts);
      }
      return map;
    });

    const groupOptions = computed(() => {
      const groups = new Set();
      for (const channel of channels.value) {
        for (const tag of channel.tags || []) {
          groups.add(tag);
        }
      }
      const options = [{label: 'All groups', value: 'all'}];
      Array.from(groups)
        .sort((a, b) => a.localeCompare(b))
        .forEach((group) => options.push({label: group, value: group}));
      return options;
    });

    const filteredChannels = computed(() => {
      const query = searchQuery.value.toLowerCase();
      return channels.value.filter((channel) => {
        if (query && !channel.name.toLowerCase().includes(query)) return false;
        if (selectedGroup.value !== 'all') {
          return (channel.tags || []).includes(selectedGroup.value);
        }
        return true;
      });
    });

    const programmeStyle = (programme) => {
      const left = ((programme.start_ts - startTs.value) / 60) * pxPerMinute;
      const baseWidth = Math.max(140, ((programme.stop_ts - programme.start_ts) / 60) * pxPerMinute);
      const expanded = expandedProgramId.value === programme.id;
      const size = expanded ? expandedSizes.value[programme.id] : null;
      const width = expanded ? Math.max(baseWidth, size?.width || 320) : baseWidth;
      const height = expanded ? Math.max(170, size?.height || 170) : 58;
      const style = {
        left: `${left}px`,
        width: `${width}px`,
        height: `${height}px`,
      };
      if (expanded) {
        const minimumLeft = 8;
        style.left = `${Math.max(left, minimumLeft)}px`;
      }
      return style;
    };

    const formatTime = (ts) => {
      const date = new Date(ts * 1000);
      return date.toLocaleTimeString([], {hour: '2-digit', minute: '2-digit'});
    };

    const mergeProgrammes = (incoming) => {
      const map = new Map(programmes.value.map((programme) => [programme.id, programme]));
      for (const programme of incoming || []) {
        map.set(programme.id, programme);
      }
      programmes.value = Array.from(map.values());
    };

    const bindHeaderEvents = () => {
      if (headerEventsBound.value) return;
      const headerEl = document.querySelector('.guide__header .guide__scroll');
      if (!headerEl) return;
      scrollHeader.value = headerEl;
      headerEl.onscroll = syncHeaderScroll;
      headerEl.onwheel = onHeaderWheel;
      headerEl.addEventListener('scroll', syncHeaderScroll, {passive: true});
      headerEl.addEventListener('wheel', onHeaderWheel, {passive: false});
      headerEl.addEventListener('mousedown', onHeaderPointerDown);
      headerEl.addEventListener('touchstart', onHeaderTouchStart, {passive: true});
      headerEl.addEventListener('touchmove', onHeaderTouchMove);
      headerEl.addEventListener('touchend', onHeaderTouchEnd);

      const onPointerDown = (event) => {
        draggingHeader.value = true;
        dragStartX.value = event.clientX;
        dragStartScrollLeft.value = headerEl.scrollLeft;
        headerEl.setPointerCapture(event.pointerId);
      };

      const onPointerMove = (event) => {
        if (!draggingHeader.value) return;
        const deltaX = event.clientX - dragStartX.value;
        const nextScroll = Math.max(0, dragStartScrollLeft.value - deltaX);
        headerEl.scrollLeft = nextScroll;
      };

      const onPointerUp = (event) => {
        draggingHeader.value = false;
        if (headerEl.hasPointerCapture(event.pointerId)) {
          headerEl.releasePointerCapture(event.pointerId);
        }
      };

      headerEl.addEventListener('pointerdown', onPointerDown);
      headerEl.addEventListener('pointermove', onPointerMove);
      headerEl.addEventListener('pointerup', onPointerUp);
      headerEl.addEventListener('pointercancel', onPointerUp);

      headerEventHandlers.value = {
        onPointerDown,
        onPointerMove,
        onPointerUp,
      };
      headerEventsBound.value = true;
    };

    const startHeaderScrollSync = () => {
      const sync = () => {
        const headerEl = document.querySelector('.guide__header .guide__scroll');
        const bodyEls = Array.from(document.querySelectorAll('.guide__body .guide__scroll'));
        if (headerEl && bodyEls.length) {
          const currentLeft = headerEl.scrollLeft;
          if (currentLeft !== lastHeaderLeft.value) {
            lastHeaderLeft.value = currentLeft;
            bodyEls.forEach((el) => {
              el.scrollLeft = currentLeft;
            });
            maybeExtendWindow(bodyEls[0]);
          }
        }
        scrollSyncHandle.value = requestAnimationFrame(sync);
      };
      if (!scrollSyncHandle.value) {
        scrollSyncHandle.value = requestAnimationFrame(sync);
      }
    };

    const stopHeaderScrollSync = () => {
      if (scrollSyncHandle.value) {
        cancelAnimationFrame(scrollSyncHandle.value);
        scrollSyncHandle.value = null;
      }
    };

    const fetchGuide = async () => {
      loading.value = true;
      try {
        const response = await axios.get('/tic-api/guide/grid', {
          params: {start_ts: startTs.value, end_ts: endTs.value},
        });
        channels.value = response.data.channels || [];
        programmes.value = response.data.programmes || [];
        await nextTick();
        bindHeaderEvents();
      } catch (error) {
        console.error('Failed to load guide:', error);
      } finally {
        loading.value = false;
      }
    };

    const loadRecordings = async () => {
      try {
        const response = await axios.get('/tic-api/recordings');
        recordings.value = response.data.data || [];
      } catch (error) {
        console.error('Failed to load recordings:', error);
      }
    };

    const fetchGuideRange = async (rangeStart, rangeEnd, {prepend = false} = {}) => {
      if (rangeStart < minStartTs) {
        rangeStart = minStartTs;
        prepend = false;
      }
      if (rangeStart >= rangeEnd) return;
      if (loadingMore.value) return;
      loadingMore.value = true;
      try {
        const response = await axios.get('/tic-api/guide/grid', {
          params: {start_ts: rangeStart, end_ts: rangeEnd},
        });
        if (!channels.value.length && response.data.channels) {
          channels.value = response.data.channels || [];
        }
        mergeProgrammes(response.data.programmes || []);

        const oldStart = startTs.value;
        const oldEnd = endTs.value;
        startTs.value = Math.min(startTs.value, rangeStart);
        endTs.value = Math.max(endTs.value, rangeEnd);

        const bodyEls = getBodyScrollEls();
        if (prepend && bodyEls.length && oldStart !== startTs.value) {
          const minutesAdded = (oldStart - startTs.value) / 60;
          const addedWidth = minutesAdded * pxPerMinute;
          await nextTick();
          bodyEls.forEach((el) => {
            el.scrollLeft += addedWidth;
          });
          if (scrollHeader.value) {
            scrollHeader.value.scrollLeft = bodyEls[0].scrollLeft;
          }
        }

        if (!prepend && scrollHeader.value && bodyEls.length) {
          scrollHeader.value.scrollLeft = bodyEls[0].scrollLeft;
        }
      } catch (error) {
        console.error('Failed to load guide range:', error);
      } finally {
        loadingMore.value = false;
      }
    };

    const previewChannel = async (channel) => {
      try {
        const response = await axios.get(`/tic-api/channels/${channel.id}/preview`);
        if (response.data.success) {
          videoStore.showPlayer({
            url: response.data.preview_url,
            title: channel.name,
            type: response.data.stream_type || 'auto',
          });
          return;
        }
        $q.notify({color: 'negative', message: response.data.message || 'Failed to load preview'});
      } catch (error) {
        console.error('Preview failed:', error);
        $q.notify({color: 'negative', message: 'Failed to load preview'});
      }
    };

    const copyStreamUrl = async (channel) => {
      try {
        const response = await axios.get(`/tic-api/channels/${channel.id}/preview`);
        if (response.data.success) {
          await navigator.clipboard.writeText(response.data.preview_url);
          $q.notify({color: 'positive', message: 'Stream URL copied'});
          return;
        }
        $q.notify({color: 'negative', message: response.data.message || 'Failed to copy stream URL'});
      } catch (error) {
        console.error('Copy stream URL failed:', error);
        $q.notify({color: 'negative', message: 'Failed to copy stream URL'});
      }
    };

    const recordProgramme = async (channel, programme) => {
      try {
        await axios.post('/tic-api/recordings', {
          channel_id: channel.id,
          title: programme.title,
          description: programme.desc,
          start_ts: programme.start_ts,
          stop_ts: programme.stop_ts,
          epg_programme_id: programme.id,
        });
        await loadRecordings();
      } catch (error) {
        console.error('Recording failed:', error);
      }
    };

    const cancelProgrammeRecording = async (recording) => {
      if (!recording?.id) return;
      try {
        await axios.post(`/tic-api/recordings/${recording.id}/cancel`);
        await loadRecordings();
      } catch (error) {
        console.error('Cancel recording failed:', error);
      }
    };

    const recordSeries = async (channel, programme) => {
      try {
        await axios.post('/tic-api/recording-rules', {
          channel_id: channel.id,
          title_match: programme.title,
          lookahead_days: 7,
        });
      } catch (error) {
        console.error('Rule failed:', error);
      }
    };

    const syncScroll = (event) => {
      if (syncingScroll.value) return;
      syncingScroll.value = true;
      const targetLeft = event.target.scrollLeft;
      if (scrollHeader.value) {
        scrollHeader.value.scrollLeft = targetLeft;
      }
      getBodyScrollEls().forEach((el) => {
        if (el !== event.target) {
          el.scrollLeft = targetLeft;
        }
      });
      maybeExtendWindow(event.target);
      syncingScroll.value = false;
    };

    const syncHeaderScroll = (event) => {
      if (syncingScroll.value) return;
      syncingScroll.value = true;
      const targetLeft = event.target.scrollLeft;
      getBodyScrollEls().forEach((el) => {
        el.scrollLeft = targetLeft;
      });
      maybeExtendWindow(getBodyScrollEls()[0]);
      syncingScroll.value = false;
    };

    const onHeaderWheel = (event) => {
      const bodyEls = getBodyScrollEls();
      if (!scrollHeader.value || !bodyEls.length) return;
      event.preventDefault();
      const delta = Math.abs(event.deltaX) > Math.abs(event.deltaY) ? event.deltaX : event.deltaY;
      const nextScroll = Math.max(0, scrollHeader.value.scrollLeft + delta);
      scrollHeader.value.scrollLeft = nextScroll;
      bodyEls.forEach((el) => {
        el.scrollLeft = nextScroll;
      });
      maybeExtendWindow(bodyEls[0]);
    };

    const onGlobalScroll = (event) => {
      const headerEl = event.target?.closest?.('.guide__header .guide__scroll');
      if (!headerEl) return;
      scrollHeader.value = headerEl;
      syncHeaderScroll({target: headerEl});
    };

    const onGlobalWheel = (event) => {
      const headerEl = event.target?.closest?.('.guide__header .guide__scroll');
      if (!headerEl) return;
      scrollHeader.value = headerEl;
      onHeaderWheel(event);
    };

    const maybeExtendWindow = (scrollEl) => {
      if (!scrollEl || loadingMore.value) return;
      const {scrollLeft, clientWidth} = scrollEl;
      const visibleStart = startTs.value + (scrollLeft / pxPerMinute) * 60;
      const visibleEnd = visibleStart + (clientWidth / pxPerMinute) * 60;
      const bufferSeconds = 30 * 60;

      if (visibleStart < startTs.value + bufferSeconds && startTs.value > minStartTs) {
        const newStart = Math.max(minStartTs, startTs.value - extendWindowSeconds);
        fetchGuideRange(newStart, startTs.value, {prepend: true});
        return;
      }

      if (visibleEnd > endTs.value - bufferSeconds) {
        const newEnd = endTs.value + extendWindowSeconds;
        fetchGuideRange(endTs.value, newEnd, {prepend: false});
      }
    };

    const onHeaderPointerDown = (event) => {
      if (!scrollHeader.value) return;
      draggingHeader.value = true;
      dragStartX.value = event.clientX;
      dragStartScrollLeft.value = scrollHeader.value.scrollLeft;
      document.addEventListener('mousemove', onHeaderPointerMove);
      document.addEventListener('mouseup', onHeaderPointerUp);
    };

    const onHeaderPointerMove = (event) => {
      const bodyEls = getBodyScrollEls();
      if (!draggingHeader.value || !scrollHeader.value || !bodyEls.length) return;
      const deltaX = event.clientX - dragStartX.value;
      const nextScroll = Math.max(0, dragStartScrollLeft.value - deltaX);
      scrollHeader.value.scrollLeft = nextScroll;
      bodyEls.forEach((el) => {
        el.scrollLeft = nextScroll;
      });
      maybeExtendWindow(bodyEls[0]);
    };

    const onHeaderPointerUp = () => {
      draggingHeader.value = false;
      document.removeEventListener('mousemove', onHeaderPointerMove);
      document.removeEventListener('mouseup', onHeaderPointerUp);
    };

    const onHeaderTouchStart = (event) => {
      if (!scrollHeader.value || !event.touches?.length) return;
      draggingHeader.value = true;
      dragStartX.value = event.touches[0].clientX;
      dragStartScrollLeft.value = scrollHeader.value.scrollLeft;
    };

    const onHeaderTouchMove = (event) => {
      const bodyEls = getBodyScrollEls();
      if (!draggingHeader.value || !scrollHeader.value || !bodyEls.length || !event.touches?.length) return;
      const deltaX = event.touches[0].clientX - dragStartX.value;
      const nextScroll = Math.max(0, dragStartScrollLeft.value - deltaX);
      scrollHeader.value.scrollLeft = nextScroll;
      bodyEls.forEach((el) => {
        el.scrollLeft = nextScroll;
      });
      maybeExtendWindow(bodyEls[0]);
    };

    const onHeaderTouchEnd = () => {
      draggingHeader.value = false;
    };

    const toggleProgramme = (programme) => {
      const wasExpanded = expandedProgramId.value === programme.id;
      expandedProgramId.value = wasExpanded ? null : programme.id;
      if (!wasExpanded) {
        nextTick(() => {
          const programmeEl = programmeRefs.get(programme.id);
          if (programmeEl) {
            const desiredWidth = Math.max(programmeEl.scrollWidth, programmeEl.offsetWidth);
            const desiredHeight = Math.max(programmeEl.scrollHeight, programmeEl.offsetHeight);
            expandedSizes.value = {
              ...expandedSizes.value,
              [programme.id]: {
                width: desiredWidth,
                height: desiredHeight,
              },
            };
          }
          const bodyEl = guideBody.value;
          const headerEl = scrollHeader.value;
          const bodyEls = getBodyScrollEls();
          const scrollLeft = bodyEls[0]?.scrollLeft || 0;
          const programmeLeft = ((programme.start_ts - startTs.value) / 60) * pxPerMinute;
          const baseWidth = Math.max(140, ((programme.stop_ts - programme.start_ts) / 60) * pxPerMinute);
          const programmeWidth = Math.max(baseWidth, expandedSizes.value[programme.id]?.width || 320);
          const programmeRight = programmeLeft + programmeWidth;
          const viewportWidth = bodyEl?.clientWidth || 0;
          if (programmeLeft < scrollLeft + 10) {
            const targetLeft = Math.max(0, programmeLeft - 10);
            bodyEls.forEach((el) => { el.scrollLeft = targetLeft; });
            if (headerEl) headerEl.scrollLeft = targetLeft;
          } else if (programmeRight > scrollLeft + viewportWidth - 10) {
            const targetLeft = Math.max(0, programmeRight - viewportWidth + 10);
            bodyEls.forEach((el) => { el.scrollLeft = targetLeft; });
            if (headerEl) headerEl.scrollLeft = targetLeft;
          }
        });
      }
    };

    const setProgrammeRef = (programmeId, el) => {
      if (el) {
        programmeRefs.set(programmeId, el);
      } else {
        programmeRefs.delete(programmeId);
      }
    };

    const isLive = (programme) => {
      const now = Math.floor(Date.now() / 1000);
      return programme.start_ts <= now && programme.stop_ts >= now;
    };

    const rowHeight = (channel) => {
      const channelProgrammes = programmesByChannel.value[channel.id] || [];
      const expanded = channelProgrammes.some((programme) => programme.id === expandedProgramId.value);
      if (!expanded) return 70;
      const size = expandedSizes.value[expandedProgramId.value];
      return Math.max(200, (size?.height || 170) + 30);
    };

    const nowLineLeft = computed(() => {
      if (nowTs.value < startTs.value) return 0;
      return ((nowTs.value - startTs.value) / 60) * pxPerMinute;
    });

    const nowLineVisible = computed(() => {
      if (nowTs.value < startTs.value || nowTs.value > endTs.value) return false;
      const scrollLeft = lastHeaderLeft.value || 0;
      const headerWidth = scrollHeader.value?.clientWidth || 0;
      const visibleStart = startTs.value + (scrollLeft / pxPerMinute) * 60;
      const visibleEnd = visibleStart + (headerWidth / pxPerMinute) * 60;
      return nowTs.value >= visibleStart && nowTs.value <= visibleEnd;
    });

    let pollingActive = true;
    const pollRecordings = async () => {
      while (pollingActive) {
        try {
          const response = await axios.get('/tic-api/recordings/poll', {
            params: {wait: 1, timeout: 25},
          });
          recordings.value = response.data.data || [];
        } catch (error) {
          await new Promise((resolve) => setTimeout(resolve, 1000));
        }
      }
    };

    onMounted(async () => {
      await nextTick();
      bindHeaderEvents();
      startHeaderScrollSync();
      document.addEventListener('scroll', onGlobalScroll, true);
      document.addEventListener('wheel', onGlobalWheel, {passive: false});
      nowTimerId.value = setInterval(() => {
        nowTs.value = Math.floor(Date.now() / 1000);
      }, 60000);
      fetchGuide();
      await loadRecordings();
      pollRecordings();
    });
    onBeforeUnmount(() => {
      pollingActive = false;
      stopHeaderScrollSync();
      if (nowTimerId.value) {
        clearInterval(nowTimerId.value);
        nowTimerId.value = null;
      }
      document.removeEventListener('mousemove', onHeaderPointerMove);
      document.removeEventListener('mouseup', onHeaderPointerUp);
      if (scrollHeader.value) {
        scrollHeader.value.onscroll = null;
        scrollHeader.value.onwheel = null;
        scrollHeader.value.removeEventListener('scroll', syncHeaderScroll);
        scrollHeader.value.removeEventListener('wheel', onHeaderWheel);
        scrollHeader.value.removeEventListener('mousedown', onHeaderPointerDown);
        scrollHeader.value.removeEventListener('touchstart', onHeaderTouchStart);
        scrollHeader.value.removeEventListener('touchmove', onHeaderTouchMove);
        scrollHeader.value.removeEventListener('touchend', onHeaderTouchEnd);
        if (headerEventHandlers.value.onPointerDown) {
          scrollHeader.value.removeEventListener('pointerdown', headerEventHandlers.value.onPointerDown);
          scrollHeader.value.removeEventListener('pointermove', headerEventHandlers.value.onPointerMove);
          scrollHeader.value.removeEventListener('pointerup', headerEventHandlers.value.onPointerUp);
          scrollHeader.value.removeEventListener('pointercancel', headerEventHandlers.value.onPointerUp);
        }
      }
      document.removeEventListener('scroll', onGlobalScroll, true);
      document.removeEventListener('wheel', onGlobalWheel);
    });

    const recordingsIndex = computed(() => {
      const map = new Map();
      (recordings.value || []).forEach((rec) => {
        const status = String(rec?.status || '').toLowerCase();
        if (status === 'canceled' || status === 'deleted') {
          return;
        }
        if (rec?.epg_programme_id) {
          map.set(`epg:${rec.epg_programme_id}`, rec);
        }
        if (rec?.channel_id && rec?.start_ts && rec?.stop_ts) {
          map.set(`slot:${rec.channel_id}:${rec.start_ts}:${rec.stop_ts}`, rec);
        }
      });
      return map;
    });

    const getRecordingForProgramme = (programme, channel) => {
      if (!programme) return null;
      const byEpg = recordingsIndex.value.get(`epg:${programme.id}`);
      if (byEpg) return byEpg;
      if (channel?.id && programme.start_ts && programme.stop_ts) {
        return recordingsIndex.value.get(`slot:${channel.id}:${programme.start_ts}:${programme.stop_ts}`) || null;
      }
      return null;
    };

    const recordingBadgeLabel = (rec) => {
      if (!rec?.status) return 'Scheduled';
      const status = String(rec.status).toLowerCase();
      if (status === 'recording' || status === 'running' || status === 'in_progress') {
        return 'Recording';
      }
      if (status === 'canceled' || status === 'deleted') {
        return 'Canceled';
      }
      return 'Scheduled';
    };

    const recordingBadgeClass = (rec) => {
      if (!rec?.status) return 'guide__program-badge--scheduled';
      const status = String(rec.status).toLowerCase();
      if (status === 'recording' || status === 'running' || status === 'in_progress') {
        return 'guide__program-badge--recording';
      }
      if (status === 'canceled' || status === 'deleted') {
        return 'guide__program-badge--canceled';
      }
      return 'guide__program-badge--scheduled';
    };

    const programmeClass = (programme, channel) => {
      const rec = getRecordingForProgramme(programme, channel);
      return {
        'guide__program--expanded': expandedProgramId.value === programme.id,
        'guide__program--scheduled': rec && recordingBadgeLabel(rec) === 'Scheduled',
        'guide__program--recording': rec && recordingBadgeLabel(rec) === 'Recording',
      };
    };

    return {
      loading,
      searchQuery,
      selectedGroup,
      groupOptions,
      channels,
      programmes,
      recordings,
      hoveredChannelId,
      expandedProgramId,
      scrollHeader,
      scrollBody,
      guideBody,
      bindHeaderEvents,
      filteredChannels,
      programmesByChannel,
      timelineWidth,
      timeSlots,
      programmeStyle,
      nowLineLeft,
      nowLineVisible,
      formatTime,
      fetchGuide,
      previewChannel,
      copyStreamUrl,
      recordProgramme,
      cancelProgrammeRecording,
      recordSeries,
      syncScroll,
      syncHeaderScroll,
      onHeaderWheel,
      onGlobalScroll,
      onGlobalWheel,
      onHeaderPointerDown,
      onHeaderTouchStart,
      onHeaderTouchMove,
      onHeaderTouchEnd,
      toggleProgramme,
      isLive,
      rowHeight,
      setProgrammeRef,
      getRecordingForProgramme,
      recordingBadgeLabel,
      recordingBadgeClass,
      programmeClass,
    };
  },
});
</script>

<style scoped>
.guide {
  display: flex;
  flex-direction: column;
}

.guide__header,
.guide__row {
  display: grid;
  grid-template-columns: 260px 1fr;
  border-bottom: 1px solid #e0e0e0;
}

.guide__channel-col {
  padding: 12px;
  background: #fafafa;
  border-right: 1px solid #e0e0e0;
}

.guide__channel-row {
  display: flex;
  align-items: center;
  gap: 12px;
}

.guide__channel-logo-wrap {
  position: relative;
  width: 46px;
  height: 46px;
  border-radius: 8px;
  background: #fff;
  border: 1px solid #e0e0e0;
  display: flex;
  align-items: center;
  justify-content: center;
  cursor: pointer;
  overflow: hidden;
  padding: 1px;
  box-sizing: border-box;
}

.guide__channel-logo {
  width: 100%;
  height: 100%;
  max-width: 100%;
  max-height: 100%;
  object-fit: contain;
  display: block;
}

.guide__channel-play-icon {
  color: #1976d2;
}

.guide__channel-logo-overlay {
  position: absolute;
  inset: 0;
  display: flex;
  align-items: center;
  justify-content: center;
  background: rgba(25, 118, 210, 0.08);
  opacity: 0;
  transition: opacity 0.2s ease;
}

.guide__channel-logo-overlay--active {
  opacity: 1;
}

.guide__channel-meta {
  display: flex;
  flex-direction: column;
  gap: 2px;
}

.guide__timeline-col {
  position: relative;
  overflow: hidden;
}

.guide__now-line {
  position: absolute;
  top: 0;
  bottom: 0;
  width: 2px;
  background: #ff5252;
  z-index: 4;
  pointer-events: none;
}

.guide__scroll {
  overflow-x: auto;
  overflow-y: hidden;
}

.guide__scroll--draggable {
  cursor: grab;
  user-select: none;
}

.guide__scroll--draggable:active {
  cursor: grabbing;
}

.guide__timeline {
  position: relative;
  height: 70px;
}

.guide__tick {
  position: absolute;
  top: 0;
  height: 100%;
  width: 60px;
  border-left: 1px solid #e5e7eb;
  padding: 6px;
  font-size: 0.7rem;
  color: #80848f;
  box-sizing: border-box;
}

.guide__tick--hour {
  border-left-color: #cfd4dc;
  font-weight: 600;
  color: #5d6471;
}

.guide__program {
  position: absolute;
  top: 6px;
  height: 58px;
  background: #eef2f7;
  color: #1b1f29;
  border-radius: 8px;
  padding: 6px 8px;
  box-sizing: border-box;
  overflow: hidden;
  cursor: pointer;
  border: 1px solid #dbe2ec;
}

.guide__program--scheduled {
  border-color: #b9c6ee;
  background: #e6edfb;
}

.guide__program--recording {
  border-color: #f5a3a3;
  background: #fdecec;
}

.guide__program-badge {
  position: absolute;
  top: 6px;
  right: 6px;
  font-size: 0.65rem;
  font-weight: 600;
  padding: 2px 8px;
  border-radius: 999px;
  background: #5f85f5;
  color: #fff;
  letter-spacing: 0.2px;
}

.guide__program-badge--recording {
  background: #e74c3c;
}

.guide__program-badge--scheduled {
  background: #5f85f5;
}

.guide__program-badge--canceled {
  background: #9aa3b2;
}

.guide__program-title {
  font-weight: 600;
  font-size: 0.85rem;
  line-height: 1.1;
}

.guide__program-time {
  font-size: 0.7rem;
  color: rgba(27, 31, 41, 0.7);
}

.guide__program-actions {
  display: flex;
  gap: 6px;
  margin-top: 6px;
  flex-wrap: wrap;
}

.guide__program--expanded {
  z-index: 5;
  box-shadow: 0 8px 18px rgba(27, 31, 41, 0.12);
  background: #e2e9f5;
  overflow: visible;
  padding-bottom: 16px;
}

.guide__program-details {
  margin-top: 6px;
  font-size: 0.75rem;
  color: rgba(27, 31, 41, 0.8);
}

.guide__program-desc {
  margin-bottom: 6px;
  max-height: none;
  overflow: visible;
}

@media (hover: none), (pointer: coarse) {
  .guide__channel-logo-overlay {
    opacity: 1;
  }
}
</style>
