<template>
  <q-page>

    <div :class="$q.screen.lt.sm ? 'q-pa-none' : 'q-pa-md'">

      <div class="row">
        <div class="col-12 help-main help-main--full">

          <q-card flat>
            <q-card-section
              v-if="!isPhoneLayout"
              :class="$q.platform.is.mobile ? 'q-px-none' : ''"
            >
              <div class="row items-center q-col-gutter-sm justify-between tv-guide-toolbar">
                <div v-if="!isPhoneLayout" class="col-12 col-sm-4 col-md-3">
                  <TicSearchInput
                    v-model="searchQuery"
                    label="Search Channels"
                    placeholder="Channel name"
                  />
                </div>
                <div v-if="!isPhoneLayout" class="col-12 col-sm-4 col-md-3">
                  <TicSelectInput
                    v-model="selectedGroup"
                    label="Channel Group"
                    :options="groupOptions"
                    option-label="label"
                    option-value="value"
                    :emit-value="true"
                    :map-options="true"
                    :clearable="false"
                    :dense="true"
                    :behavior="isMobileLayout ? 'dialog' : 'menu'"
                    class="tv-guide-filter-select"
                  />
                </div>
              </div>
            </q-card-section>

            <q-separator v-if="!isPhoneLayout" />

            <q-card-section class="q-pa-none">
              <div
                class="guide"
                :class="{
                  'guide--compact': isCompactLayout,
                  'guide--mobile': isMobileLayout,
                  'guide--mobile-collapsed': isMobileLayout && channelRailCollapsed,
                }"
                :style="{
                  '--guide-sticky-top': guideStickyTop + 'px',
                  '--guide-channel-width': channelColumnWidth + 'px',
                  '--guide-program-height': programmeBlockHeight + 'px',
                }"
              >
                <div class="guide__header">
                  <div v-if="isPhoneLayout" class="guide__actions-strip bg-card-head">
                    <div class="row items-center justify-end">
                      <TicActionButton
                        :icon="mobileActionsExpanded ? 'expand_less' : 'expand_more'"
                        color="grey-8"
                        :tooltip="mobileActionsExpanded ? 'Hide actions' : 'Show actions'"
                        @click="toggleMobileActions"
                      />
                    </div>
                    <q-slide-transition>
                      <div v-show="mobileActionsExpanded" class="row q-col-gutter-sm q-pt-xs">
                        <div class="col-12">
                          <TicSearchInput
                            v-model="searchQuery"
                            label="Search Channels"
                            placeholder="Channel name"
                          />
                        </div>
                        <div class="col-12">
                          <TicSelectInput
                            v-model="selectedGroup"
                            label="Channel Group"
                            :options="groupOptions"
                            option-label="label"
                            option-value="value"
                            :emit-value="true"
                            :map-options="true"
                            :clearable="false"
                            :dense="true"
                            behavior="dialog"
                            class="tv-guide-filter-select"
                          />
                        </div>
                      </div>
                    </q-slide-transition>
                  </div>

                  <div class="guide__header-main">
                    <div class="guide__channel-col guide__channel-col--header">
                      <div class="guide__channel-header-title">Channel</div>
                      <div v-if="isMobileLayout" class="guide__channel-header-toggle">
                        <TicButton
                          v-if="!channelRailCollapsed"
                          label="Hide"
                          icon="chevron_left"
                          dense
                          variant="flat"
                          color="primary"
                          tooltip="Hide channel rail"
                          @click="toggleChannelRail"
                        />
                        <TicActionButton
                          v-else
                          icon="chevron_right"
                          color="primary"
                          tooltip="Show channel rail"
                          @click="toggleChannelRail"
                        />
                      </div>
                    </div>
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
                </div>

                <div class="guide__body" ref="guideBody">
                  <div
                    v-for="channel in filteredChannels"
                    :key="channel.id"
                    class="guide__row"
                    :style="{height: rowHeight(channel) + 'px'}"
                  >
                    <div class="guide__channel-col">
                      <div
                        class="guide__channel-row"
                        :class="{
                          'guide__channel-row--mobile-expanded': isMobileLayout && !channelRailCollapsed,
                        }"
                      >
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
                        <template v-if="isMobileLayout && !channelRailCollapsed">
                          <div class="guide__channel-number guide__channel-number--mobile text-caption text-grey-7">
                            #{{ channel.number }}
                          </div>
                          <div class="guide__channel-name guide__channel-name--mobile text-weight-medium">
                            {{ channel.name }}
                          </div>
                        </template>
                        <div class="guide__channel-meta" v-else-if="!isMobileLayout">
                          <div class="guide__channel-number text-caption text-grey-7">#{{ channel.number }}</div>
                          <div class="guide__channel-name text-weight-medium">{{ channel.name }}</div>
                        </div>
                        <div class="guide__channel-mini text-caption text-grey-7" v-else>
                          #{{ channel.number }}
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
                            @click="toggleProgramme(programme, channel)"
                          >
                            <div class="guide__program-content" :style="programmeContentStyle(programme)">
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
                              <div v-if="!isMobileLayout && expandedProgramId === programme.id"
                                   class="guide__program-details">
                                <div class="guide__program-desc">
                                  {{ programme.desc || 'No description available.' }}
                                </div>
                                <div class="guide__program-actions">
                                  <TicButton
                                    icon="play_arrow"
                                    label="Watch now"
                                    v-if="isLive(programme)"
                                    dense
                                    variant="flat"
                                    @click.stop="previewChannel(channel)"
                                  />
                                  <TicButton
                                    v-if="!getRecordingForProgramme(programme, channel)"
                                    dense
                                    variant="flat"
                                    icon="fiber_manual_record"
                                    label="Record"
                                    color="negative"
                                    @click.stop="recordProgramme(channel, programme)"
                                  />
                                  <TicButton
                                    v-if="getRecordingForProgramme(programme, channel)"
                                    dense
                                    variant="flat"
                                    icon="cancel"
                                    label="Cancel Recording"
                                    color="negative"
                                    @click.stop="cancelProgrammeRecording(getRecordingForProgramme(programme, channel))"
                                  />
                                  <TicButton
                                    dense
                                    variant="flat"
                                    icon="repeat"
                                    label="Record series"
                                    @click.stop="recordSeries(channel, programme)"
                                  />
                                  <TicButton
                                    dense
                                    variant="flat"
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
                  </div>

                  <div v-if="!loading && filteredChannels.length === 0" class="q-pa-lg text-grey-6">
                    No channels with EPG data found.
                  </div>
                </div>
              </div>
            </q-card-section>
          </q-card>

        </div>
      </div>
    </div>

    <TicDialogPopup
      v-model="mobileProgrammeDialogOpen"
      title="Programme Details"
      width="720px"
      max-width="96vw">
      <div v-if="mobileProgrammeDetails" class="guide-programme-popup">
        <div class="guide-programme-popup__title">{{ mobileProgrammeDetails.programme.title }}</div>
        <div class="guide-programme-popup__meta text-caption text-grey-7">
          {{ mobileProgrammeDetails.channel.name }} ·
          #{{ mobileProgrammeDetails.channel.number }} ·
          {{ formatTime(mobileProgrammeDetails.programme.start_ts) }} - {{ formatTime(
          mobileProgrammeDetails.programme.stop_ts) }}
        </div>
        <div class="guide-programme-popup__desc">
          {{ mobileProgrammeDetails.programme.desc || 'No description available.' }}
        </div>
      </div>

      <template #actions>
        <div class="row q-col-gutter-sm full-width">
          <div class="col-6">
            <TicButton
              v-if="mobileProgrammeDetails && isLive(mobileProgrammeDetails.programme)"
              label="Watch now"
              icon="play_arrow"
              color="primary"
              class="full-width"
              @click="previewChannel(mobileProgrammeDetails.channel)"
            />
          </div>
          <div class="col-6">
            <TicButton
              v-if="mobileProgrammeDetails && !getRecordingForProgramme(mobileProgrammeDetails.programme, mobileProgrammeDetails.channel)"
              label="Record"
              icon="fiber_manual_record"
              color="negative"
              class="full-width"
              @click="recordProgramme(mobileProgrammeDetails.channel, mobileProgrammeDetails.programme)"
            />
            <TicButton
              v-else-if="mobileProgrammeDetails"
              label="Cancel"
              icon="cancel"
              color="negative"
              class="full-width"
              @click="cancelProgrammeRecording(getRecordingForProgramme(mobileProgrammeDetails.programme, mobileProgrammeDetails.channel))"
            />
          </div>
          <div class="col-6">
            <TicButton
              v-if="mobileProgrammeDetails"
              label="Record series"
              icon="repeat"
              color="primary"
              class="full-width"
              @click="recordSeries(mobileProgrammeDetails.channel, mobileProgrammeDetails.programme)"
            />
          </div>
          <div class="col-6">
            <TicButton
              v-if="mobileProgrammeDetails && isLive(mobileProgrammeDetails.programme)"
              label="Copy stream URL"
              icon="link"
              color="primary"
              class="full-width"
              @click="copyStreamUrl(mobileProgrammeDetails.channel)"
            />
          </div>
        </div>
      </template>
    </TicDialogPopup>

  </q-page>
</template>

<script>
import {defineComponent, ref, computed, onMounted, onBeforeUnmount, nextTick, watch} from 'vue';
import axios from 'axios';
import {useVideoStore} from 'stores/video';
import {useUiStore} from 'stores/ui';
import {useQuasar} from 'quasar';
import {useMobile} from 'src/composables/useMobile';
import {TicActionButton, TicButton, TicDialogPopup, TicSearchInput, TicSelectInput} from 'components/ui';

export default defineComponent({
  name: 'TvGuidePage',
  components: {
    TicActionButton,
    TicButton,
    TicDialogPopup,
    TicSearchInput,
    TicSelectInput,
  },
  setup() {
    const videoStore = useVideoStore();
    const uiStore = useUiStore();
    const $q = useQuasar();
    const {isMobile} = useMobile();
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
    const guideStickyTop = ref(0);
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
    const guideRefreshTimerId = ref(null);
    const channelRailCollapsed = ref(false);
    const channelRailPinnedOpen = ref(false);
    const mobileProgrammeDialogOpen = ref(false);
    const mobileProgrammeDetails = ref(null);
    const mobileActionsExpanded = ref(true);
    const lastWindowScrollY = ref(0);
    const mobileActionToggleGuardUntil = ref(0);

    const pxPerMinute = 6;
    const extendWindowSeconds = 6 * 3600;
    const tickMinutes = 15;

    const isCompactLayout = computed(() => $q.screen.lt.md);
    const isMobileLayout = computed(() => isMobile.value || $q.screen.lt.md);
    const isPhoneLayout = computed(() => $q.screen.lt.sm);
    const use12HourTime = computed(() => uiStore.timeFormat === '12h');

    const formatDisplayTime = (ts) => {
      const date = new Date(ts * 1000);
      if (use12HourTime.value) {
        return date.toLocaleTimeString([], {hour: 'numeric', minute: '2-digit', hour12: true});
      }
      return date.toLocaleTimeString([], {hour: '2-digit', minute: '2-digit', hour12: false});
    };

    const programmeBlockHeight = computed(() => {
      if (isMobileLayout.value) return 50;
      if (isCompactLayout.value) return 54;
      return 58;
    });

    const channelColumnWidth = computed(() => {
      if (isMobileLayout.value && channelRailCollapsed.value) return 44;
      if (isMobileLayout.value) return 106;
      if (isCompactLayout.value) return 172;
      return 260;
    });

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
          label: formatDisplayTime(current.getTime() / 1000),
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
      Array.from(groups).
        sort((a, b) => a.localeCompare(b)).
        forEach((group) => options.push({label: group, value: group}));
      return options;
    });

    const filteredChannels = computed(() => {
      const query = searchQuery.value.toLowerCase();
      const filtered = channels.value.filter((channel) => {
        if (query && !channel.name.toLowerCase().includes(query)) return false;
        if (selectedGroup.value !== 'all') {
          return (channel.tags || []).includes(selectedGroup.value);
        }
        return true;
      });
      return filtered.sort((a, b) => {
        const aNum = Number.parseInt(a.number, 10);
        const bNum = Number.parseInt(b.number, 10);
        const aHas = Number.isFinite(aNum);
        const bHas = Number.isFinite(bNum);
        if (aHas && bHas && aNum !== bNum) return aNum - bNum;
        if (aHas && !bHas) return -1;
        if (!aHas && bHas) return 1;
        return a.name.localeCompare(b.name);
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
        height: `${Math.max(height, programmeBlockHeight.value)}px`,
      };
      if (expanded) {
        const minimumLeft = 8;
        style.left = `${Math.max(left, minimumLeft)}px`;
      }
      return style;
    };

    const programmeContentStyle = (programme) => {
      const programmeLeft = ((programme.start_ts - startTs.value) / 60) * pxPerMinute;
      const baseWidth = Math.max(140, ((programme.stop_ts - programme.start_ts) / 60) * pxPerMinute);
      const expanded = expandedProgramId.value === programme.id;
      const size = expanded ? expandedSizes.value[programme.id] : null;
      const programmeWidth = expanded ? Math.max(baseWidth, size?.width || 320) : baseWidth;
      const hiddenLeft = Math.max(0, lastHeaderLeft.value - programmeLeft);
      const minVisibleWidth = isMobileLayout.value ? 56 : 72;
      const maxShift = Math.max(0, programmeWidth - minVisibleWidth);
      const shift = Math.min(hiddenLeft + 6, maxShift);
      return {
        '--guide-program-content-shift': `${shift}px`,
      };
    };

    const formatTime = (ts) => {
      return formatDisplayTime(ts);
    };

    const mergeProgrammes = (incoming) => {
      const map = new Map(programmes.value.map((programme) => [programme.id, programme]));
      for (const programme of incoming || []) {
        map.set(programme.id, programme);
      }
      programmes.value = Array.from(map.values());
    };

    const getBodyScrollEls = () => {
      return Array.from(document.querySelectorAll('.guide__body .guide__scroll'));
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
            handleRailAutoCollapse(currentLeft);
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

    const refreshCurrentGuideWindow = async () => {
      if (loading.value || loadingMore.value) return;
      try {
        const response = await axios.get('/tic-api/guide/grid', {
          params: {start_ts: startTs.value, end_ts: endTs.value},
        });

        channels.value = response.data.channels || [];
        const incoming = response.data.programmes || [];
        const incomingIds = new Set(incoming.map((programme) => programme.id));

        // Replace rows inside the current window while keeping any rows outside it.
        programmes.value = programmes.value.filter((programme) => {
          const inWindow = programme.start_ts < endTs.value && programme.stop_ts > startTs.value;
          if (!inWindow) return true;
          return incomingIds.has(programme.id);
        });
        mergeProgrammes(incoming);
      } catch (error) {
        console.error('Failed to refresh guide window:', error);
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
        $q.notify({color: 'positive', message: 'Recording scheduled'});
        await loadRecordings();
      } catch (error) {
        console.error('Recording failed:', error);
        $q.notify({color: 'negative', message: 'Failed to schedule recording'});
      }
    };

    const cancelProgrammeRecording = async (recording) => {
      if (!recording?.id) return;
      try {
        await axios.post(`/tic-api/recordings/${recording.id}/cancel`);
        $q.notify({color: 'positive', message: 'Recording canceled'});
        await loadRecordings();
      } catch (error) {
        console.error('Cancel recording failed:', error);
        $q.notify({color: 'negative', message: 'Failed to cancel recording'});
      }
    };

    const recordSeries = async (channel, programme) => {
      try {
        await axios.post('/tic-api/recording-rules', {
          channel_id: channel.id,
          title_match: programme.title,
          lookahead_days: 7,
        });
        $q.notify({color: 'positive', message: 'Recording rule created'});
      } catch (error) {
        console.error('Rule failed:', error);
        $q.notify({color: 'negative', message: 'Failed to create recording rule'});
      }
    };

    const handleRailAutoCollapse = (scrollLeft) => {
      if (!isMobileLayout.value) {
        channelRailCollapsed.value = false;
        channelRailPinnedOpen.value = false;
        return;
      }
      if (scrollLeft <= 8) {
        channelRailCollapsed.value = false;
        channelRailPinnedOpen.value = false;
        return;
      }
      if (!channelRailPinnedOpen.value) {
        channelRailCollapsed.value = true;
      }
    };

    const toggleChannelRail = () => {
      if (!isMobileLayout.value) return;
      if (channelRailCollapsed.value) {
        channelRailCollapsed.value = false;
        channelRailPinnedOpen.value = true;
        return;
      }
      channelRailCollapsed.value = true;
      channelRailPinnedOpen.value = false;
    };

    const toggleMobileActions = () => {
      if (!isPhoneLayout.value) return;
      mobileActionsExpanded.value = !mobileActionsExpanded.value;
      mobileActionToggleGuardUntil.value = Date.now() + 350;
      lastWindowScrollY.value = window.scrollY || 0;
    };

    const onWindowScrollForMobileActions = () => {
      if (!isPhoneLayout.value) {
        mobileActionsExpanded.value = true;
        lastWindowScrollY.value = window.scrollY || 0;
        return;
      }

      if (Date.now() < mobileActionToggleGuardUntil.value) {
        lastWindowScrollY.value = window.scrollY || 0;
        return;
      }

      const nextScrollY = window.scrollY || 0;
      if (mobileActionsExpanded.value && nextScrollY > lastWindowScrollY.value + 10) {
        mobileActionsExpanded.value = false;
      }
      lastWindowScrollY.value = nextScrollY;
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
      handleRailAutoCollapse(targetLeft);
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
      handleRailAutoCollapse(targetLeft);
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
      handleRailAutoCollapse(nextScroll);
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
      handleRailAutoCollapse(nextScroll);
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
      handleRailAutoCollapse(nextScroll);
    };

    const onHeaderTouchEnd = () => {
      draggingHeader.value = false;
    };

    const toggleProgramme = (programme, channel) => {
      if (isMobileLayout.value) {
        mobileProgrammeDetails.value = {programme, channel};
        mobileProgrammeDialogOpen.value = true;
        return;
      }
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
            bodyEls.forEach((el) => {
              el.scrollLeft = targetLeft;
            });
            if (headerEl) headerEl.scrollLeft = targetLeft;
          } else if (programmeRight > scrollLeft + viewportWidth - 10) {
            const targetLeft = Math.max(0, programmeRight - viewportWidth + 10);
            bodyEls.forEach((el) => {
              el.scrollLeft = targetLeft;
            });
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
      if (!expanded) return isMobileLayout.value ? 62 : 70;
      const size = expandedSizes.value[expandedProgramId.value];
      return Math.max(isMobileLayout.value ? 168 : 200, (size?.height || 170) + 30);
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

    const updateGuideStickyTop = () => {
      const headerEl = document.querySelector('header.q-header') || document.querySelector('header');
      guideStickyTop.value = headerEl ? Math.round(headerEl.getBoundingClientRect().height) : 0;
    };

    onMounted(async () => {
      await nextTick();
      updateGuideStickyTop();
      window.addEventListener('resize', updateGuideStickyTop);
      bindHeaderEvents();
      startHeaderScrollSync();
      document.addEventListener('scroll', onGlobalScroll, true);
      document.addEventListener('wheel', onGlobalWheel, {passive: false});
      window.addEventListener('scroll', onWindowScrollForMobileActions, {passive: true});
      lastWindowScrollY.value = window.scrollY || 0;
      nowTimerId.value = setInterval(() => {
        nowTs.value = Math.floor(Date.now() / 1000);
      }, 60000);
      guideRefreshTimerId.value = setInterval(() => {
        refreshCurrentGuideWindow();
      }, 5 * 60 * 1000);
      fetchGuide();
      await loadRecordings();
      pollRecordings();
    });
    onBeforeUnmount(() => {
      pollingActive = false;
      stopHeaderScrollSync();
      window.removeEventListener('resize', updateGuideStickyTop);
      if (nowTimerId.value) {
        clearInterval(nowTimerId.value);
        nowTimerId.value = null;
      }
      if (guideRefreshTimerId.value) {
        clearInterval(guideRefreshTimerId.value);
        guideRefreshTimerId.value = null;
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
      window.removeEventListener('scroll', onWindowScrollForMobileActions);
    });

    watch(isPhoneLayout, (isPhone) => {
      if (!isPhone) {
        mobileActionsExpanded.value = true;
      }
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
        'guide__program--expanded': !isMobileLayout.value && expandedProgramId.value === programme.id,
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
      guideStickyTop,
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
      programmeContentStyle,
      nowLineLeft,
      nowLineVisible,
      formatTime,
      fetchGuide,
      refreshCurrentGuideWindow,
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
      toggleChannelRail,
      isLive,
      rowHeight,
      isCompactLayout,
      isMobileLayout,
      isPhoneLayout,
      channelColumnWidth,
      programmeBlockHeight,
      channelRailCollapsed,
      mobileActionsExpanded,
      toggleMobileActions,
      mobileProgrammeDialogOpen,
      mobileProgrammeDetails,
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

.guide__actions-strip {
  border-bottom: 1px solid var(--guide-border);
  padding: 6px 10px 8px;
}

.guide__row {
  display: grid;
  grid-template-columns: var(--guide-channel-width, 260px) 1fr;
  border-bottom: 1px solid var(--guide-border);
}

.guide__header {
  position: sticky;
  top: var(--guide-sticky-top, 0px);
  z-index: 7;
  background: var(--guide-channel-bg);
}

.guide__header-main {
  display: grid;
  grid-template-columns: var(--guide-channel-width, 260px) 1fr;
  border-bottom: 1px solid var(--guide-border);
}

.guide__channel-col {
  padding: 10px 12px;
  background: var(--guide-channel-bg);
  border-right: 1px solid var(--guide-channel-border);
}

.guide__header .guide__channel-col {
  padding-top: 13px;
}

.guide__channel-col--header {
  display: flex;
  flex-direction: column;
  justify-content: space-between;
  align-items: flex-start;
}

.guide__channel-header-title {
  font-weight: 600;
}

.guide__channel-header-toggle {
  margin-top: 4px;
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
  border-radius: 6px;
  background: var(--guide-logo-bg);
  border: 1px solid var(--guide-logo-border);
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
  color: var(--guide-play-icon);
}

.guide__channel-logo-overlay {
  position: absolute;
  inset: 0;
  display: flex;
  align-items: center;
  justify-content: center;
  background: var(--guide-play-overlay);
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
  min-width: 0;
}

.guide__channel-name {
  line-height: 1.2;
}

.guide__channel-mini {
  font-size: 0.66rem;
  font-weight: 600;
  line-height: 1;
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
  background: var(--guide-now-line);
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
  border-left: 0.5px solid var(--guide-border);
  padding: 6px;
  font-size: 0.7rem;
  color: var(--guide-tick);
  box-sizing: border-box;
}

.guide__header .guide__tick {
  padding-top: 9px;
}

.guide__tick--hour {
  border-left-color: var(--guide-border-strong);
  border-left-width: 2px;
  font-weight: 600;
  color: var(--guide-tick-hour);
}

.guide__program {
  position: absolute;
  top: 6px;
  height: var(--guide-program-height, 58px);
  background: var(--guide-program-bg);
  color: var(--guide-program-text);
  border-radius: 6px;
  padding: 6px 8px;
  box-sizing: border-box;
  overflow: hidden;
  cursor: pointer;
  border: 1px solid var(--guide-program-border);
}

.guide__program-content {
  transform: translateX(var(--guide-program-content-shift, 0px));
}

.guide__program--scheduled {
  border-color: var(--guide-program-scheduled-border);
  background: var(--guide-program-scheduled-bg);
}

.guide__program--recording {
  border-color: var(--guide-program-recording-border);
  background: var(--guide-program-recording-bg);
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
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.guide__program-time {
  font-size: 0.7rem;
  color: var(--guide-program-time);
}

.guide__program-actions {
  display: flex;
  gap: 6px;
  margin-top: 6px;
  flex-wrap: wrap;
}

.guide__program--expanded {
  z-index: 5;
  box-shadow: var(--guide-program-shadow);
  background: var(--guide-program-expanded-bg);
  overflow: visible;
  padding-bottom: 16px;
}

.guide__program-details {
  margin-top: 6px;
  font-size: 0.75rem;
  color: var(--guide-program-details);
}

.guide__program-desc {
  margin-bottom: 6px;
  max-height: none;
  overflow: visible;
}

.guide-programme-popup__title {
  font-weight: 600;
  font-size: 1rem;
  line-height: 1.2;
}

.guide-programme-popup__meta {
  margin-top: 6px;
}

.guide-programme-popup__desc {
  margin-top: 10px;
  font-size: 0.86rem;
  line-height: 1.35;
}

:deep(.tv-guide-filter-select .q-field--outlined .q-field__control) {
  min-height: 40px;
}

:deep(.tv-guide-toolbar .tv-guide-filter-select .tic-select-input-field) {
  padding-bottom: 0 !important;
}

@media (max-width: 1023px) {
  .guide__channel-col {
    padding: 8px 10px;
  }

  .guide__channel-col--header {
    align-items: flex-start;
  }

  .guide--mobile-collapsed .guide__channel-col--header {
    align-items: center;
  }

  .guide--mobile-collapsed .guide__channel-header-title {
    display: none;
  }

  .guide--mobile-collapsed .guide__channel-header-toggle {
    margin-top: 0;
  }

  .guide__channel-row {
    display: flex;
    flex-direction: column;
    align-items: center;
    gap: 4px;
  }

  .guide__channel-row--mobile-expanded {
    display: grid;
    width: 100%;
    grid-template-columns: 30px 1fr;
    grid-template-areas:
      'logo number'
      'name name';
    align-items: center;
    column-gap: 6px;
    row-gap: 2px;
  }

  .guide__channel-logo-wrap {
    width: 30px;
    height: 30px;
  }

  .guide__channel-row--mobile-expanded .guide__channel-logo-wrap {
    grid-area: logo;
  }

  .guide__channel-number--mobile {
    grid-area: number;
    font-size: 0.66rem;
    font-weight: 600;
    line-height: 1;
    text-align: right;
    justify-self: end;
  }

  .guide__channel-name--mobile {
    grid-area: name;
    font-size: 0.7rem;
    line-height: 1.1;
    max-width: 100%;
    white-space: nowrap;
    text-overflow: ellipsis;
    overflow: hidden;
    text-align: left;
  }

  .guide--mobile-collapsed .guide__channel-row {
    justify-content: center;
    display: flex;
  }

  .guide__timeline {
    height: 60px;
  }

  .guide__tick {
    font-size: 0.64rem;
    padding: 4px;
  }

  .guide__program {
    top: 5px;
    padding: 5px 6px;
  }

  .guide__program-title {
    font-size: 0.76rem;
  }

  .guide__program-time {
    font-size: 0.64rem;
  }
}

@media (max-width: 599px) {
  .guide__channel-col {
    padding: 8px 6px;
  }

  .guide__channel-row--mobile-expanded {
    grid-template-columns: 28px 1fr;
  }

  .guide__channel-logo-wrap {
    width: 28px;
    height: 28px;
  }

  .guide__channel-number--mobile {
    font-size: 0.62rem;
  }

  .guide__channel-name--mobile {
    font-size: 0.66rem;
  }

  .guide__channel-number {
    font-size: 0.62rem;
  }

  .guide__channel-name {
    font-size: 0.68rem;
    max-width: 90px;
    white-space: nowrap;
    text-overflow: ellipsis;
    overflow: hidden;
  }

  .guide__timeline {
    height: 54px;
  }

  .guide__tick {
    padding: 3px;
    font-size: 0.58rem;
  }

  .guide__program {
    top: 4px;
    padding: 4px 5px;
    border-radius: 5px;
  }

  .guide__program-title {
    font-size: 0.68rem;
  }

  .guide__program-time {
    font-size: 0.58rem;
  }

  .guide-programme-popup__title {
    font-size: 0.94rem;
  }

  .guide-programme-popup__desc {
    font-size: 0.8rem;
  }
}

@media (hover: none), (pointer: coarse) {
  .guide__channel-logo-overlay {
    opacity: 1;
  }
}
</style>
