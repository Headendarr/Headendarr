<template>
  <div
    v-if="videoStore.isVisible"
    ref="playerRoot"
    class="floating-player"
    :class='{
      "floating-player--mobile": isMobile,
      "floating-player--controls-visible": controlsVisible,
      "floating-player--fullscreen": fullscreenActive,
    }'
    :style="playerStyle"
  >
    <div
      class="floating-player__header"
      @mousedown="startDrag"
      @touchstart="startDragTouch"
    >
      <div class="floating-player__title">
        <div class="floating-player__title-text">
          {{ videoStore.streamTitle || 'Stream Preview' }}
        </div>
        <div v-if="streamDetailsText" class="floating-player__meta">
          {{ streamDetailsText }}
        </div>
      </div>
      <div class="floating-player__header-dropdowns">
        <TicButtonDropdown
          v-if="hasPlaybackProfiles"
          dense
          variant="flat"
          color="white"
          :label="useIconOnlyDropdowns ? undefined : currentPlaybackProfileLabel"
          :icon='useIconOnlyDropdowns ? "tune" : "video_settings"'
          class="floating-player__header-dropdown"
          content-class="floating-player__header-dropdown-menu"
          :content-style='{zIndex: "10001"}'
        >
          <q-list dense class="floating-player__header-menu">
            <q-item
              v-for="profile in videoStore.playbackProfiles"
              :key="profile.id"
              clickable
              :active="profile.id === videoStore.selectedPlaybackProfile"
              active-class="floating-player__header-item--active"
              @click="selectPlaybackProfile(profile.id)"
            >
              <q-item-section>
                <q-item-label>{{ profile.label }}</q-item-label>
                <q-item-label v-if="profile.description" caption>{{ profile.description }}</q-item-label>
              </q-item-section>
            </q-item>
          </q-list>
        </TicButtonDropdown>
        <TicButtonDropdown
          v-if="isVodPlayback"
          dense
          variant="flat"
          color="white"
          :label="useIconOnlyDropdowns ? undefined : `${playbackRateLabel}x`"
          icon="speed"
          class="floating-player__header-dropdown"
          content-class="floating-player__header-dropdown-menu"
          :content-style='{zIndex: "10001"}'
        >
          <q-list dense class="floating-player__header-menu">
            <q-item
              v-for="option in playbackSpeedOptions"
              :key="option"
              clickable
              :active="option === playbackRate"
              active-class="floating-player__header-item--active"
              @click="setPlaybackRate(option)"
            >
              <q-item-section>
                <q-item-label>{{ option }}x</q-item-label>
              </q-item-section>
            </q-item>
          </q-list>
        </TicButtonDropdown>
      </div>
      <div class="floating-player__actions">
        <q-btn
          v-if="pipSupported"
          dense
          flat
          round
          :icon='pipActive ? "picture_in_picture" : "picture_in_picture_alt"'
          @click.stop="togglePiP"
        >
          <q-tooltip v-if="!isMobile" class="bg-white text-primary">Picture in Picture</q-tooltip>
        </q-btn>
        <q-btn
          dense
          flat
          round
          :icon='fullscreenActive ? "fullscreen_exit" : "fullscreen"'
          @click.stop="toggleFullScreen"
        >
          <q-tooltip v-if="!isMobile" class="bg-white text-primary">Full screen</q-tooltip>
        </q-btn>
        <q-btn
          dense
          flat
          round
          icon="close"
          @touchstart.stop
          @click.stop="closePlayer"
        >
          <q-tooltip v-if="!isMobile" class="bg-white text-primary">Close</q-tooltip>
        </q-btn>
      </div>
    </div>

    <div v-if="errorMessage" class="floating-player__error-floating">
      {{ errorMessage }}
    </div>

    <div
      ref="videoArea"
      class="floating-player__body"
      @mouseenter="handleDesktopControlsEnter"
      @mouseleave="handleDesktopControlsLeave"
      @mousemove="handleDesktopControlsMove"
      @focusin="handleControlsFocusIn"
      @focusout="handleControlsFocusOut"
      @touchstart="handleMobileTouch"
    >
      <div v-if="isLoading" class="floating-player__overlay">
        <q-spinner size="32px" color="white" />
      </div>
      <video
        ref="videoEl"
        :class='["floating-player__video", { "floating-player__video--loading": isLoading }]'
        playsinline
        @click="handleVideoSurfaceClick"
      />

      <div class="floating-player__controls" :aria-hidden="!controlsVisible">
        <div class="floating-player__controls-scrim"></div>
        <div class="floating-player__controls-inner">
          <div class="floating-player__controls-row floating-player__controls-row--top">
            <div class="floating-player__controls-left">
              <q-btn
                dense
                flat
                round
                :icon='isPlaying ? "pause" : "play_arrow"'
                class="floating-player__control-btn"
                @click.stop="togglePlayback"
              />
              <div class="floating-player__time-group">
                <span class="floating-player__time-value">{{ formattedCurrentTime }}</span>
                <span class="floating-player__time-separator">/</span>
                <span class="floating-player__time-value">{{ formattedTotalDuration }}</span>
              </div>
            </div>

            <div class="floating-player__controls-right">
              <div
                v-if="!isMobile"
                class="floating-player__volume-group"
                tabindex="0"
                @mouseenter="handleDesktopControlsEnter"
                @focusin="handleControlsFocusIn"
              >
                <div
                  class="floating-player__volume-slider-wrap"
                  :style="volumeSliderStyle"
                >
                  <input
                    class="floating-player__slider floating-player__slider--volume"
                    type="range"
                    min="0"
                    max="1"
                    step="0.05"
                    :value="sliderVolumeValue"
                    aria-label="Volume"
                    @input="handleVolumeInput"
                  />
                </div>
                <q-btn
                  dense
                  flat
                  round
                  :icon="volumeIcon"
                  class="floating-player__control-btn"
                  @click.stop="toggleMute"
                />
              </div>

              <q-btn
                v-else
                dense
                flat
                round
                :icon="volumeIcon"
                class="floating-player__control-btn"
                @click.stop="toggleMute"
              />

              <q-btn
                v-if="hasCaptions"
                dense
                flat
                round
                :icon='captionsEnabled ? "closed_caption" : "closed_caption_disabled"'
                class="floating-player__control-btn"
                @click.stop="toggleCaptions"
              />
            </div>
          </div>

          <div class="floating-player__seek-row">
            <div
              class="floating-player__seek-slider-wrap"
              :style="seekSliderStyle"
            >
              <input
                class="floating-player__slider floating-player__slider--seek"
                type="range"
                min="0"
                :max="seekMaxSeconds"
                step="1"
                :disabled="!canSeek"
                :value="displaySeekValue"
                aria-label="Seek"
                @input="handleSeekInput"
                @change="commitSeekInput"
                @mousedown="beginSeekDrag"
                @touchstart="beginSeekDrag"
                @mouseup="finishSeekDrag"
                @touchend="finishSeekDrag"
                @keydown.left.prevent="stepSeek(-10)"
                @keydown.right.prevent="stepSeek(10)"
              />
            </div>
          </div>
        </div>
      </div>
    </div>

    <div
      class="floating-player__resize-handle floating-player__resize-handle--br"
      @mousedown.stop='startResize("br", $event)'
      @touchstart.prevent.stop='startResizeTouch("br", $event)'
    ></div>
    <div
      class="floating-player__resize-handle floating-player__resize-handle--bl"
      @mousedown.stop='startResize("bl", $event)'
      @touchstart.prevent.stop='startResizeTouch("bl", $event)'
    ></div>
    <div
      class="floating-player__resize-handle floating-player__resize-handle--tr"
      @mousedown.stop='startResize("tr", $event)'
      @touchstart.prevent.stop='startResizeTouch("tr", $event)'
    ></div>
    <div
      class="floating-player__resize-handle floating-player__resize-handle--tl"
      @mousedown.stop='startResize("tl", $event)'
      @touchstart.prevent.stop='startResizeTouch("tl", $event)'
    ></div>
  </div>
</template>

<script setup>
import axios from 'axios';
import {computed, nextTick, onBeforeUnmount, onUnmounted, ref, watch} from 'vue';
import TicButtonDropdown from 'components/ui/buttons/TicButtonDropdown.vue';
import {useVideoStore} from 'stores/video';
import Hls from 'hls.js';
import mpegts from 'mpegts.js';
import {useMobile} from 'src/composables/useMobile';
import {buildVodPlaybackProfiles, resolveVodPlayerStreamType} from 'src/utils/vodPlaybackProfiles';

const videoStore = useVideoStore();
const {isMobile} = useMobile();

const playerRoot = ref(null);
const videoArea = ref(null);
const videoEl = ref(null);
const isLoading = ref(false);
const errorMessage = ref('');
const dragState = ref(null);
const resizeState = ref(null);
const hlsInstance = ref(null);
const mpegtsInstance = ref(null);
const hlsInstances = new Set();
const mpegtsInstances = new Set();
const initToken = ref(0);
const currentSessionId = ref(null);
const volumeHandler = ref(null);
const applyingPersistedVolume = ref(false);
const videoErrorHandler = ref(null);
const videoPlayHandler = ref(null);
const videoPlayingHandler = ref(null);
const videoCanPlayHandler = ref(null);
const videoPauseHandler = ref(null);
const videoWaitingHandler = ref(null);
const metadataHandler = ref(null);
const resizeHandler = ref(null);
const videoSeekingHandler = ref(null);
const timeUpdateHandler = ref(null);
const durationChangeHandler = ref(null);
const endedHandler = ref(null);
const loadTimeout = ref(null);
const loadingStartedAt = ref(0);
const errorAutoClearTimer = ref(null);
const suppressTransientErrors = ref(false);
const playbackStarted = ref(false);
const pendingSeekTime = ref(null);
const playbackRate = ref(1);
const hlsRecoveryState = ref({
  mediaRecoveryAttempts: 0,
  networkRecoveryAttempts: 0,
  lastMediaRecoveryAt: 0,
});
let playerInitSequence = Promise.resolve();
let activeInitRunId = 0;
const streamDetails = ref({
  resolution: '',
  videoCodec: '',
  audioCodec: '',
  bitrate: null,
});
const heartbeatTimer = ref(null);
const activePlaybackContext = ref(null);
const suppressManagedSeekUntil = ref(0);
const currentTimeSeconds = ref(0);
const mediaDurationSeconds = ref(0);
const isPlaying = ref(false);
const volumeValue = ref(1);
const lastVolumeBeforeMute = ref(1);
const captionsEnabled = ref(false);
const captionsTrackCount = ref(0);
const fullscreenActive = ref(false);
const pipActive = ref(false);
const desktopControlsActive = ref(false);
const mobileControlsVisible = ref(false);
const controlsHideTimer = ref(null);
const seekDraftSeconds = ref(null);
const isSeekDragging = ref(false);
const activeVodMetadataUrl = ref('');
const vodPreviewMetadataRetryTimer = ref(null);
const textTracksRef = ref(null);
const textTrackChangeHandler = ref(null);
const textTrackAddHandler = ref(null);
const textTrackRemoveHandler = ref(null);
const candidateFailoverInProgress = ref(false);

const pipSupported = computed(() => typeof document !== 'undefined' && !!document.pictureInPictureEnabled);
const hasPlaybackProfiles = computed(
  () => Array.isArray(videoStore.playbackProfiles) && videoStore.playbackProfiles.length > 1);
const currentPlaybackProfile = computed(() => {
  return (videoStore.playbackProfiles || []).find((profile) => profile.id === videoStore.selectedPlaybackProfile) ||
    null;
});
const currentPlaybackProfileLabel = computed(() => currentPlaybackProfile.value?.label || 'Original');
const isVodPlayback = computed(() => {
  if (Number(videoStore.durationSeconds || 0) > 0) {
    return true;
  }
  return String(videoStore.streamUrl || '').includes('/tic-api/cso/vod/');
});
const playbackSpeedOptions = [0.5, 1, 1.25, 1.5, 2];
const playbackRateLabel = computed(() => {
  const value = Number(playbackRate.value || 1);
  return Number.isInteger(value) ? String(value) : String(value).replace(/0+$/, '').replace(/\.$/, '');
});
const useIconOnlyDropdowns = computed(() => isMobile.value || Number(videoStore.size?.width || 0) < 480);
const hasCaptions = computed(() => captionsTrackCount.value > 0);
const controlsVisible = computed(() => isMobile.value ? mobileControlsVisible.value : desktopControlsActive.value);
const usesRestartSeek = computed(() => usesRestartBasedSeek());
const totalDurationSeconds = computed(() => {
  const backendDuration = Number(videoStore.durationSeconds || 0);
  if (usesRestartSeek.value && backendDuration > 0) {
    return backendDuration;
  }
  if (backendDuration > 0 && isVodPlayback.value) {
    return backendDuration;
  }
  const mediaDuration = Number(mediaDurationSeconds.value || 0);
  if (Number.isFinite(mediaDuration) && mediaDuration > 0) {
    return mediaDuration;
  }
  return 0;
});
const effectiveCurrentTime = computed(() => {
  const raw = Number(isSeekDragging.value ? seekDraftSeconds.value : currentTimeSeconds.value) || 0;
  const max = Number(totalDurationSeconds.value || 0);
  if (max > 0) {
    return Math.max(0, Math.min(raw, max));
  }
  return Math.max(0, raw);
});
const formattedCurrentTime = computed(() => formatTime(effectiveCurrentTime.value));
const formattedTotalDuration = computed(() => {
  if (totalDurationSeconds.value <= 0) {
    return '--:--';
  }
  return formatTime(totalDurationSeconds.value);
});
const canSeek = computed(() => totalDurationSeconds.value > 0 && isVodPlayback.value);
const seekMaxSeconds = computed(() => Math.max(0, Math.floor(totalDurationSeconds.value || 0)));
const displaySeekValue = computed(() => {
  if (!canSeek.value) {
    return 0;
  }
  return Math.max(0, Math.min(Math.floor(effectiveCurrentTime.value), seekMaxSeconds.value));
});
const seekSliderStyle = computed(() => {
  const max = Number(seekMaxSeconds.value || 0);
  const value = Number(displaySeekValue.value || 0);
  const progress = max > 0 ? Math.max(0, Math.min(100, (value / max) * 100)) : 0;
  return {'--slider-progress': `${progress}%`};
});
const sliderVolumeValue = computed(() => {
  if (volumeValue.value <= 0) {
    return '0';
  }
  return String(Math.max(0, Math.min(1, volumeValue.value)));
});
const volumeSliderStyle = computed(() => {
  const progress = Math.max(0, Math.min(100, (Number(sliderVolumeValue.value) || 0) * 100));
  return {'--slider-progress': `${progress}%`};
});
const volumeIcon = computed(() => {
  if (volumeValue.value <= 0) {
    return 'volume_off';
  }
  if (volumeValue.value < 0.5) {
    return 'volume_down';
  }
  return 'volume_up';
});

const streamDetailsText = computed(() => {
  const parts = [];
  if (streamDetails.value.resolution) {
    parts.push(streamDetails.value.resolution);
  }
  const codecLabel = [streamDetails.value.videoCodec, streamDetails.value.audioCodec].filter(Boolean).join('/');
  if (codecLabel) {
    parts.push(codecLabel);
  }
  if (streamDetails.value.bitrate) {
    const bitrate = streamDetails.value.bitrate;
    if (bitrate >= 1000000) {
      parts.push(`${(bitrate / 1000000).toFixed(1)} Mbps`);
    } else if (bitrate >= 1000) {
      parts.push(`${Math.round(bitrate / 1000)} Kbps`);
    }
  }
  return parts.join(' · ');
});

const playbackInitKey = computed(() => {
  if (!videoStore.isVisible) {
    return '';
  }
  const url = String(videoStore.streamUrl || '').trim();
  if (!url) {
    return '';
  }
  return `${url}|${videoStore.streamType || 'auto'}`;
});

function formatTime(totalSeconds) {
  const seconds = Math.max(0, Math.floor(Number(totalSeconds) || 0));
  const hours = Math.floor(seconds / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);
  const remainingSeconds = seconds % 60;
  if (hours > 0) {
    return `${hours}:${String(minutes).padStart(2, '0')}:${String(remainingSeconds).padStart(2, '0')}`;
  }
  return `${minutes}:${String(remainingSeconds).padStart(2, '0')}`;
}

async function safePlay(el) {
  try {
    await el.play();
    return {started: true, error: null};
  } catch (error) {
    console.warn('[FloatingPlayer] play failed', error);
    return {started: false, error};
  }
}

async function waitForMpegtsStartupReady(player, el, timeoutMs = 2500) {
  await new Promise((resolve) => {
    let settled = false;
    let timeoutId = null;
    let mediaInfoHandler = null;
    let loadedMetadataHandler = null;
    let canPlayHandler = null;
    let playingHandler = null;

    const cleanup = () => {
      if (timeoutId) {
        clearTimeout(timeoutId);
        timeoutId = null;
      }
      if (player && mediaInfoHandler && player.off) {
        try {
          player.off(mpegts.Events.MEDIA_INFO, mediaInfoHandler);
        } catch (error) {
          console.warn('Failed to detach MPEG-TS media info handler:', error);
        }
      }
      if (el && loadedMetadataHandler) {
        el.removeEventListener('loadedmetadata', loadedMetadataHandler);
      }
      if (el && canPlayHandler) {
        el.removeEventListener('canplay', canPlayHandler);
      }
      if (el && playingHandler) {
        el.removeEventListener('playing', playingHandler);
      }
    };

    const finish = () => {
      if (settled) {
        return;
      }
      settled = true;
      cleanup();
      resolve();
    };

    mediaInfoHandler = () => finish();
    loadedMetadataHandler = () => finish();
    canPlayHandler = () => finish();
    playingHandler = () => finish();

    if (player && player.on) {
      player.on(mpegts.Events.MEDIA_INFO, mediaInfoHandler);
    }
    if (el) {
      el.addEventListener('loadedmetadata', loadedMetadataHandler, {once: true});
      el.addEventListener('canplay', canPlayHandler, {once: true});
      el.addEventListener('playing', playingHandler, {once: true});
      if (el.readyState >= 1) {
        finish();
        return;
      }
    }

    timeoutId = setTimeout(() => finish(), timeoutMs);
  });
}

function clearErrorMessage() {
  if (errorAutoClearTimer.value) {
    clearTimeout(errorAutoClearTimer.value);
    errorAutoClearTimer.value = null;
  }
  errorMessage.value = '';
}

function getUrlStartSeconds(url) {
  if (!url) {
    return 0;
  }
  try {
    const parsed = new URL(url, window.location.origin);
    const raw = Number(parsed.searchParams.get('start') || parsed.searchParams.get('start_seconds') || 0);
    if (!Number.isFinite(raw) || raw < 0) {
      return 0;
    }
    return Math.floor(raw);
  } catch {
    return 0;
  }
}

function updatePlaybackUrl(url, profileId, startSeconds = null) {
  if (!url) {
    return '';
  }
  try {
    const parsed = new URL(url, window.location.origin);
    if (profileId) {
      parsed.searchParams.set('profile', profileId);
    } else {
      parsed.searchParams.delete('profile');
    }
    const startValue = Number(startSeconds);
    if (Number.isFinite(startValue) && startValue > 0) {
      parsed.searchParams.set('start', String(Math.floor(startValue)));
    } else {
      parsed.searchParams.delete('start');
      parsed.searchParams.delete('start_seconds');
    }
    return parsed.toString();
  } catch (error) {
    console.warn('[FloatingPlayer] failed to update playback URL', error);
    return url;
  }
}

function usesRestartBasedSeek(url = videoStore.streamUrl, forcedSeekMode = videoStore.seekMode) {
  const seekMode = String(forcedSeekMode || '').toLowerCase();
  if (seekMode !== 'hls_restart' && seekMode !== 'time_restart') {
    return false;
  }
  return String(url || '').includes('/tic-api/cso/vod/');
}

function clearVodPreviewMetadataRetry() {
  if (vodPreviewMetadataRetryTimer.value) {
    clearTimeout(vodPreviewMetadataRetryTimer.value);
    vodPreviewMetadataRetryTimer.value = null;
  }
}

function scheduleVodPreviewMetadataRetry(previewMetadataUrl, attemptNumber) {
  if (attemptNumber > 4) {
    return;
  }
  clearVodPreviewMetadataRetry();
  const delayMs = Math.min(3000, 800 * attemptNumber);
  vodPreviewMetadataRetryTimer.value = setTimeout(() => {
    vodPreviewMetadataRetryTimer.value = null;
    if (!videoStore.isVisible || String(videoStore.previewMetadataUrl || '').trim() !== previewMetadataUrl) {
      return;
    }
    void loadVodPreviewMetadata(attemptNumber);
  }, delayMs);
}

async function loadVodPreviewMetadata(attemptNumber = 1) {
  const previewMetadataUrl = String(videoStore.previewMetadataUrl || '').trim();
  if (!videoStore.isVisible || !previewMetadataUrl || !previewMetadataUrl.includes('/tic-api/cso/vod/')) {
    return;
  }
  const hasDuration = Number(videoStore.durationSeconds || 0) > 0;
  const hasResolution = Number(videoStore.sourceResolution?.width || 0) > 0 &&
    Number(videoStore.sourceResolution?.height || 0) > 0;
  if (hasDuration && hasResolution) {
    return;
  }
  if (activeVodMetadataUrl.value === previewMetadataUrl) {
    return;
  }
  clearVodPreviewMetadataRetry();
  activeVodMetadataUrl.value = previewMetadataUrl;
  try {
    const response = await axios.post('/tic-api/vod/preview-metadata', {
      preview_url: previewMetadataUrl,
    });
    const payload = response?.data || {};
    if (!payload?.success || String(videoStore.previewMetadataUrl || '').trim() !== previewMetadataUrl) {
      return;
    }
    const streamType = resolveVodPlayerStreamType(payload?.stream_type || videoStore.streamType);
    const sourceResolution = payload?.source_resolution || null;
    const playbackProfiles = buildVodPlaybackProfiles(sourceResolution, streamType);
    videoStore.setVodMetadata({
      sourceResolution,
      durationSeconds: Number(payload?.duration_seconds || 0) || null,
      playbackProfiles,
    });
    if (payload?.pending) {
      scheduleVodPreviewMetadataRetry(previewMetadataUrl, attemptNumber + 1);
    }
  } catch (error) {
    console.warn('[FloatingPlayer] failed to load VOD preview metadata', error);
    scheduleVodPreviewMetadataRetry(previewMetadataUrl, attemptNumber + 1);
  } finally {
    if (activeVodMetadataUrl.value === previewMetadataUrl) {
      activeVodMetadataUrl.value = '';
    }
  }
}

function currentAbsolutePlaybackTime(el = getVideoElement()) {
  const relativeCurrentTime = el && Number.isFinite(el.currentTime) && el.currentTime > 0 ? el.currentTime : 0;
  if (usesRestartBasedSeek()) {
    return getUrlStartSeconds(videoStore.streamUrl) + relativeCurrentTime;
  }
  return relativeCurrentTime;
}

function queueRestartSeek(absoluteSeconds) {
  const nextStart = Math.max(0, Math.floor(Number(absoluteSeconds) || 0));
  const nextUrl = updatePlaybackUrl(videoStore.streamUrl, currentPlaybackProfile.value?.profile || '', nextStart);
  if (!nextUrl || nextUrl === videoStore.streamUrl) {
    currentTimeSeconds.value = nextStart;
    return;
  }
  suppressManagedSeekUntil.value = Date.now() + 1200;
  pendingSeekTime.value = null;
  currentTimeSeconds.value = nextStart;
  videoStore.setPlaybackProfile(
    videoStore.selectedPlaybackProfile,
    nextUrl,
    currentPlaybackProfile.value?.streamType || videoStore.streamType,
    currentPlaybackProfile.value?.seekMode || videoStore.seekMode,
  );
}

function restorePendingSeek(el) {
  const targetTime = Number(pendingSeekTime.value);
  if (!el || !Number.isFinite(targetTime) || targetTime <= 0) {
    return;
  }
  if (usesRestartBasedSeek()) {
    pendingSeekTime.value = null;
    return;
  }
  try {
    el.currentTime = targetTime;
  } catch (error) {
    console.info('[FloatingPlayer] delayed seek restore pending', error);
    return;
  }
  pendingSeekTime.value = null;
}

function applyPlaybackRate(el = getVideoElement()) {
  if (!el) {
    return;
  }
  el.playbackRate = Number(playbackRate.value || 1) || 1;
}

function setPlaybackRate(rate) {
  playbackRate.value = Number(rate || 1) || 1;
  applyPlaybackRate();
}

function setErrorMessage(message, autoClearMs = 0) {
  if (errorAutoClearTimer.value) {
    clearTimeout(errorAutoClearTimer.value);
    errorAutoClearTimer.value = null;
  }
  errorMessage.value = message || '';
  if (message && autoClearMs > 0) {
    const expected = message;
    errorAutoClearTimer.value = setTimeout(() => {
      if (errorMessage.value === expected) {
        errorMessage.value = '';
      }
      errorAutoClearTimer.value = null;
    }, autoClearMs);
  }
}

function nextPreviewCandidateIndex() {
  const nextIndex = Number(videoStore.activeCandidateIndex || 0) + 1;
  if (!Array.isArray(videoStore.streamCandidates) || nextIndex >= videoStore.streamCandidates.length) {
    return -1;
  }
  return nextIndex;
}

function failoverToNextPreviewCandidate() {
  const nextIndex = nextPreviewCandidateIndex();
  if (nextIndex < 0 || candidateFailoverInProgress.value) {
    return false;
  }
  candidateFailoverInProgress.value = true;
  setErrorMessage('Trying the next preview source...', 2500);
  currentSessionId.value = generateConnectionId();
  videoStore.setActiveCandidate(nextIndex);
  return true;
}

function getVideoElement() {
  return videoEl.value || document.querySelector('.floating-player__video');
}

function persistedVolumeValue() {
  if (isMobile.value) {
    return 1;
  }
  const value = Number(videoStore.volume);
  if (!Number.isFinite(value)) {
    return 1;
  }
  return Math.min(1, Math.max(0, value));
}

function syncVolumeState(el) {
  if (!el) {
    return;
  }
  volumeValue.value = el.muted ? 0 : Math.min(1, Math.max(0, Number(el.volume) || 0));
  if (volumeValue.value > 0) {
    lastVolumeBeforeMute.value = volumeValue.value;
  }
}

function applyPersistedVolume(el) {
  if (!el) {
    return;
  }
  applyingPersistedVolume.value = true;
  const persistedVolume = persistedVolumeValue();
  el.volume = persistedVolume;
  el.muted = persistedVolume <= 0;
  syncVolumeState(el);
  if (isMobile.value) {
    videoStore.setVolume(1);
  }
  setTimeout(() => {
    applyingPersistedVolume.value = false;
  }, 0);
}

function resetStreamDetails() {
  streamDetails.value = {
    resolution: '',
    videoCodec: '',
    audioCodec: '',
    bitrate: null,
  };
}

function resetPlaybackUiState() {
  currentTimeSeconds.value = 0;
  mediaDurationSeconds.value = 0;
  isPlaying.value = false;
  volumeValue.value = persistedVolumeValue();
  lastVolumeBeforeMute.value = Math.max(0.5, volumeValue.value || 1);
  captionsEnabled.value = false;
  captionsTrackCount.value = 0;
  seekDraftSeconds.value = null;
  isSeekDragging.value = false;
}

function resetHlsRecoveryState() {
  hlsRecoveryState.value = {
    mediaRecoveryAttempts: 0,
    networkRecoveryAttempts: 0,
    lastMediaRecoveryAt: 0,
  };
}

function updateResolutionFromEl(el) {
  if (el?.videoWidth && el?.videoHeight) {
    streamDetails.value = {
      ...streamDetails.value,
      resolution: `${el.videoWidth}x${el.videoHeight}`,
    };
  }
}

function parseCodecLabel(codec) {
  if (!codec) return '';
  const lowered = codec.toLowerCase();
  if (lowered.startsWith('avc') || lowered.includes('h264')) return 'H.264';
  if (lowered.startsWith('hev') || lowered.startsWith('hvc') || lowered.includes('h265')) return 'H.265';
  if (lowered.startsWith('av01')) return 'AV1';
  if (lowered.startsWith('vp9')) return 'VP9';
  if (lowered.startsWith('vp8')) return 'VP8';
  if (lowered.startsWith('mp4a') || lowered.includes('aac')) return 'AAC';
  if (lowered.startsWith('ac-3')) return 'AC3';
  if (lowered.startsWith('ec-3')) return 'EAC3';
  if (lowered.startsWith('opus')) return 'Opus';
  if (lowered.startsWith('mp3')) return 'MP3';
  return codec;
}

function parseCodecString(codecString) {
  if (!codecString) return {video: '', audio: ''};
  const parts = codecString.split(',').map((part) => part.trim()).filter(Boolean);
  let video = '';
  let audio = '';
  for (const part of parts) {
    const lowered = part.toLowerCase();
    if (!video && (
      lowered.startsWith('avc') ||
      lowered.startsWith('hev') ||
      lowered.startsWith('hvc') ||
      lowered.startsWith('av01') ||
      lowered.startsWith('vp')
    )) {
      video = parseCodecLabel(part);
    } else if (!audio && (
      lowered.startsWith('mp4a') ||
      lowered.startsWith('ac-3') ||
      lowered.startsWith('ec-3') ||
      lowered.startsWith('opus') ||
      lowered.startsWith('mp3')
    )) {
      audio = parseCodecLabel(part);
    }
  }
  return {video, audio};
}

function applyCodecInfo(videoCodec, audioCodec) {
  streamDetails.value = {
    ...streamDetails.value,
    videoCodec: videoCodec || streamDetails.value.videoCodec,
    audioCodec: audioCodec || streamDetails.value.audioCodec,
  };
}

function applyBitrate(bitrate) {
  if (!bitrate || Number.isNaN(bitrate)) return;
  streamDetails.value = {
    ...streamDetails.value,
    bitrate,
  };
}

function syncCurrentTime(el) {
  if (!el || isSeekDragging.value) {
    return;
  }
  currentTimeSeconds.value = currentAbsolutePlaybackTime(el);
}

function syncDuration(el) {
  if (!el) {
    return;
  }
  const nativeDuration = Number(el.duration);
  mediaDurationSeconds.value = Number.isFinite(nativeDuration) && nativeDuration > 0 ? nativeDuration : 0;
}

function collectCaptionTracks() {
  const el = getVideoElement();
  const tracks = [];
  if (!el?.textTracks) {
    return tracks;
  }
  for (let index = 0; index < el.textTracks.length; index += 1) {
    const track = el.textTracks[index];
    if (track) {
      tracks.push(track);
    }
  }
  return tracks;
}

function refreshCaptionState() {
  const tracks = collectCaptionTracks();
  captionsTrackCount.value = tracks.length;
  captionsEnabled.value = tracks.some((track) => track.mode === 'showing');
}

function detachTextTrackListeners() {
  const list = textTracksRef.value;
  if (!list) {
    return;
  }
  if (textTrackChangeHandler.value) {
    list.removeEventListener?.('change', textTrackChangeHandler.value);
  }
  if (textTrackAddHandler.value) {
    list.removeEventListener?.('addtrack', textTrackAddHandler.value);
  }
  if (textTrackRemoveHandler.value) {
    list.removeEventListener?.('removetrack', textTrackRemoveHandler.value);
  }
  textTracksRef.value = null;
}

function attachTextTrackListeners(el) {
  detachTextTrackListeners();
  if (!el?.textTracks) {
    refreshCaptionState();
    return;
  }
  textTracksRef.value = el.textTracks;
  if (!textTrackChangeHandler.value) {
    textTrackChangeHandler.value = () => refreshCaptionState();
  }
  if (!textTrackAddHandler.value) {
    textTrackAddHandler.value = () => refreshCaptionState();
  }
  if (!textTrackRemoveHandler.value) {
    textTrackRemoveHandler.value = () => refreshCaptionState();
  }
  el.textTracks.addEventListener?.('change', textTrackChangeHandler.value);
  el.textTracks.addEventListener?.('addtrack', textTrackAddHandler.value);
  el.textTracks.addEventListener?.('removetrack', textTrackRemoveHandler.value);
  refreshCaptionState();
}

const playerStyle = computed(() => {
  const {width, height} = videoStore.size;
  const {right, bottom, left, top} = videoStore.position;
  const style = {
    width: `${width}px`,
    height: `${height}px`,
  };
  if (right != null) style.right = `${right}px`;
  if (bottom != null) style.bottom = `${bottom}px`;
  if (left != null) style.left = `${left}px`;
  if (top != null) style.top = `${top}px`;
  return style;
});

function showDesktopControls() {
  if (isMobile.value) {
    return;
  }
  desktopControlsActive.value = true;
}

function hideDesktopControls() {
  if (isMobile.value) {
    return;
  }
  if (document.activeElement && videoArea.value?.contains(document.activeElement)) {
    return;
  }
  desktopControlsActive.value = false;
}

function clearControlsHideTimer() {
  if (controlsHideTimer.value) {
    clearTimeout(controlsHideTimer.value);
    controlsHideTimer.value = null;
  }
}

function scheduleMobileControlsHide() {
  if (!isMobile.value) {
    return;
  }
  clearControlsHideTimer();
  controlsHideTimer.value = setTimeout(() => {
    mobileControlsVisible.value = false;
    controlsHideTimer.value = null;
  }, 10000);
}

function showMobileControls() {
  mobileControlsVisible.value = true;
  scheduleMobileControlsHide();
}

function handleDesktopControlsEnter() {
  showDesktopControls();
}

function handleDesktopControlsLeave() {
  hideDesktopControls();
}

function handleDesktopControlsMove() {
  if (!isMobile.value) {
    showDesktopControls();
  }
}

function handleControlsFocusIn() {
  if (isMobile.value) {
    showMobileControls();
    return;
  }
  showDesktopControls();
}

function handleControlsFocusOut() {
  setTimeout(() => {
    if (isMobile.value) {
      showMobileControls();
      return;
    }
    if (!videoArea.value?.contains(document.activeElement)) {
      desktopControlsActive.value = false;
    }
  }, 0);
}

function handleMobileTouch() {
  if (isMobile.value) {
    showMobileControls();
  }
}

function handleVideoSurfaceClick() {
  if (isMobile.value) {
    showMobileControls();
  }
}

function cleanupPlayer() {
  console.info('[FloatingPlayer] cleanupPlayer start');
  stopPlaybackHeartbeat(true);
  clearErrorMessage();
  suppressTransientErrors.value = true;
  playbackStarted.value = false;
  resetHlsRecoveryState();
  detachTextTrackListeners();
  clearControlsHideTimer();
  for (const hls of Array.from(hlsInstances)) {
    try {
      hls.stopLoad();
      hls.detachMedia();
    } catch (error) {
      console.warn('Failed to stop HLS instance:', error);
    }
    try {
      hls.destroy();
    } catch (error) {
      console.warn('Failed to destroy HLS instance:', error);
    }
    hlsInstances.delete(hls);
  }
  hlsInstance.value = null;

  for (const player of Array.from(mpegtsInstances)) {
    try {
      player.pause();
      player.unload?.();
      player.detachMediaElement?.();
    } catch (error) {
      console.warn('Failed to stop MPEG-TS instance:', error);
    }
    try {
      player.destroy();
    } catch (error) {
      console.warn('Failed to destroy MPEG-TS instance:', error);
    }
    mpegtsInstances.delete(player);
  }
  mpegtsInstance.value = null;
  const el = getVideoElement();
  if (el) {
    videoStore.setVolume(el.muted ? 0 : el.volume);
    if (volumeHandler.value) {
      el.removeEventListener('volumechange', volumeHandler.value);
      volumeHandler.value = null;
    }
    if (videoErrorHandler.value) {
      el.removeEventListener('error', videoErrorHandler.value);
      videoErrorHandler.value = null;
    }
    if (videoPlayingHandler.value) {
      el.removeEventListener('playing', videoPlayingHandler.value);
      videoPlayingHandler.value = null;
    }
    if (videoPlayHandler.value) {
      el.removeEventListener('play', videoPlayHandler.value);
      videoPlayHandler.value = null;
    }
    if (videoCanPlayHandler.value) {
      el.removeEventListener('canplay', videoCanPlayHandler.value);
      videoCanPlayHandler.value = null;
    }
    if (videoPauseHandler.value) {
      el.removeEventListener('pause', videoPauseHandler.value);
      videoPauseHandler.value = null;
    }
    if (videoWaitingHandler.value) {
      el.removeEventListener('waiting', videoWaitingHandler.value);
      el.removeEventListener('stalled', videoWaitingHandler.value);
      videoWaitingHandler.value = null;
    }
    if (metadataHandler.value) {
      el.removeEventListener('loadedmetadata', metadataHandler.value);
      metadataHandler.value = null;
    }
    if (resizeHandler.value) {
      el.removeEventListener('resize', resizeHandler.value);
      resizeHandler.value = null;
    }
    if (videoSeekingHandler.value) {
      el.removeEventListener('seeking', videoSeekingHandler.value);
      videoSeekingHandler.value = null;
    }
    if (timeUpdateHandler.value) {
      el.removeEventListener('timeupdate', timeUpdateHandler.value);
      timeUpdateHandler.value = null;
    }
    if (durationChangeHandler.value) {
      el.removeEventListener('durationchange', durationChangeHandler.value);
      durationChangeHandler.value = null;
    }
    if (endedHandler.value) {
      el.removeEventListener('ended', endedHandler.value);
      endedHandler.value = null;
    }
    if (loadTimeout.value) {
      clearTimeout(loadTimeout.value);
      loadTimeout.value = null;
    }
    try {
      el.pause();
    } catch (error) {
      console.warn('Failed to pause video:', error);
    }
    el.removeAttribute('src');
    el.load();
  }
  resetStreamDetails();
  resetPlaybackUiState();
  candidateFailoverInProgress.value = false;
  desktopControlsActive.value = false;
  mobileControlsVisible.value = false;
  currentSessionId.value = null;
  console.info('[FloatingPlayer] cleanupPlayer done');
}

function detectStreamType(url, forcedType) {
  if (forcedType && forcedType !== 'auto') {
    return forcedType;
  }
  if (!url) return 'auto';
  const lowered = url.toLowerCase();
  if (lowered.includes('.m3u8')) return 'hls';
  if (lowered.includes('.ts')) return 'mpegts';
  if (lowered.includes('/tic-hls-proxy/') && lowered.includes('/stream/')) {
    return 'mpegts';
  }
  return 'native';
}

function generateConnectionId() {
  if (typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function') {
    return crypto.randomUUID().replace(/-/g, '');
  }
  return `${Date.now().toString(36)}${Math.random().toString(36).slice(2, 10)}`;
}

function ensureProxyConnectionId(url, forcedConnectionId = null) {
  const value = String(url || '').trim();
  if (!value) {
    return value;
  }
  if (!value.toLowerCase().includes('/tic-hls-proxy/')) {
    return value;
  }
  try {
    const parsed = new URL(value, window.location.origin);
    const sessionConnectionId = String(forcedConnectionId || '').trim() || generateConnectionId();
    parsed.searchParams.set('connection_id', sessionConnectionId);
    parsed.searchParams.delete('cid');
    return parsed.toString();
  } catch {
    return value;
  }
}

function resolvePlaybackContext(url, title, connectionId = null) {
  const value = String(url || '').trim();
  if (!value) {
    return null;
  }
  let cid = String(connectionId || '').trim();
  if (!cid) {
    try {
      const parsed = new URL(value, window.location.origin);
      cid = parsed.searchParams.get('connection_id') || parsed.searchParams.get('cid') || '';
    } catch {
      cid = '';
    }
  }
  return {
    url: value,
    title: String(title || '').trim(),
    connection_id: String(cid || '').trim() || null,
  };
}

async function sendPlaybackHeartbeat() {
  const context = activePlaybackContext.value;
  if (!context?.url) {
    return;
  }
  try {
    await axios.post('/tic-api/audit/playback-heartbeat', context);
  } catch (error) {
    console.warn('[FloatingPlayer] playback heartbeat failed', error);
  }
}

function startPlaybackHeartbeat(url, title, connectionId) {
  stopPlaybackHeartbeat(false);
  activePlaybackContext.value = resolvePlaybackContext(url, title, connectionId);
  if (!activePlaybackContext.value) {
    return;
  }
  sendPlaybackHeartbeat();
  heartbeatTimer.value = setInterval(() => {
    sendPlaybackHeartbeat();
  }, 5000);
}

async function sendPlaybackStop(context) {
  if (!context?.url) {
    return;
  }
  try {
    await axios.post('/tic-api/audit/playback-stop', context);
  } catch (error) {
    console.warn('[FloatingPlayer] playback stop audit failed', error);
  }
}

function stopPlaybackHeartbeat(sendStop) {
  if (heartbeatTimer.value) {
    clearInterval(heartbeatTimer.value);
    heartbeatTimer.value = null;
  }
  const context = activePlaybackContext.value;
  activePlaybackContext.value = null;
  if (sendStop && context?.url) {
    sendPlaybackStop(context);
  }
}

function updatePiPState() {
  const el = getVideoElement();
  pipActive.value = !!el && document.pictureInPictureElement === el;
}

function updateFullscreenState() {
  const root = playerRoot.value;
  fullscreenActive.value = !!root && document.fullscreenElement === root;
}

async function initPlayer() {
  const runId = ++activeInitRunId;
  const token = ++initToken.value;
  cleanupPlayer();
  clearErrorMessage();
  suppressTransientErrors.value = true;
  playbackStarted.value = false;
  resetHlsRecoveryState();
  resetPlaybackUiState();
  if (!currentSessionId.value) {
    currentSessionId.value = generateConnectionId();
  }
  const url = ensureProxyConnectionId(videoStore.streamUrl, currentSessionId.value);
  if (!url) {
    return;
  }
  await nextTick();
  if (token !== initToken.value) {
    return;
  }
  const el = getVideoElement();
  if (!el) {
    return;
  }
  videoEl.value = el;
  el.controls = false;
  applyPersistedVolume(el);
  applyPlaybackRate(el);
  loadingStartedAt.value = Date.now();
  currentTimeSeconds.value = getUrlStartSeconds(url);
  if (isMobile.value) {
    showMobileControls();
  }
  if (!volumeHandler.value) {
    volumeHandler.value = () => {
      if (applyingPersistedVolume.value) {
        return;
      }
      syncVolumeState(el);
      videoStore.setVolume(el.muted ? 0 : el.volume);
    };
    el.addEventListener('volumechange', volumeHandler.value);
  }
  if (!metadataHandler.value) {
    metadataHandler.value = () => {
      updateResolutionFromEl(el);
      applyPlaybackRate(el);
      syncDuration(el);
      syncCurrentTime(el);
      attachTextTrackListeners(el);
      restorePendingSeek(el);
    };
    el.addEventListener('loadedmetadata', metadataHandler.value);
  }
  if (!resizeHandler.value) {
    resizeHandler.value = () => updateResolutionFromEl(el);
    el.addEventListener('resize', resizeHandler.value);
  }
  if (!videoSeekingHandler.value) {
    videoSeekingHandler.value = () => {
      if (!usesRestartBasedSeek(url, videoStore.seekMode)) {
        return;
      }
      if (Date.now() < suppressManagedSeekUntil.value) {
        return;
      }
      const relativeTarget = Number(el.currentTime);
      if (!Number.isFinite(relativeTarget) || relativeTarget < 0) {
        return;
      }
      const absoluteTarget = getUrlStartSeconds(videoStore.streamUrl) + relativeTarget;
      if (absoluteTarget < 0) {
        return;
      }
      queueRestartSeek(absoluteTarget);
    };
    el.addEventListener('seeking', videoSeekingHandler.value);
  }
  if (!timeUpdateHandler.value) {
    timeUpdateHandler.value = () => syncCurrentTime(el);
    el.addEventListener('timeupdate', timeUpdateHandler.value);
  }
  if (!durationChangeHandler.value) {
    durationChangeHandler.value = () => syncDuration(el);
    el.addEventListener('durationchange', durationChangeHandler.value);
  }
  if (!videoPauseHandler.value) {
    videoPauseHandler.value = () => {
      isPlaying.value = false;
      isLoading.value = false;
    };
    el.addEventListener('pause', videoPauseHandler.value);
  }
  if (!endedHandler.value) {
    endedHandler.value = () => {
      isPlaying.value = false;
      syncCurrentTime(el);
    };
    el.addEventListener('ended', endedHandler.value);
  }
  if (!videoErrorHandler.value) {
    videoErrorHandler.value = () => {
      const mediaError = el.error;
      if (mediaError) {
        console.warn('[FloatingPlayer] media error', mediaError);
      }
      if (mediaError?.code === 1) {
        return;
      }
      if (mediaError?.code === 3 && detectStreamType(url, videoStore.streamType) === 'hls') {
        return;
      }
      if (suppressTransientErrors.value && !playbackStarted.value) {
        return;
      }
      if (mediaError?.code === 2) {
        if (failoverToNextPreviewCandidate()) {
          return;
        }
        setErrorMessage('Network error while loading the stream.', 4000);
      } else if (mediaError?.code === 3) {
        if (failoverToNextPreviewCandidate()) {
          return;
        }
        setErrorMessage('Stream could not be decoded. The format may be unsupported.', 4000);
      } else if (mediaError?.code === 4) {
        if (failoverToNextPreviewCandidate()) {
          return;
        }
        setErrorMessage('Stream is not supported or is invalid.');
      } else {
        if (failoverToNextPreviewCandidate()) {
          return;
        }
        setErrorMessage('Unable to load stream. Please try again.', 4000);
      }
      isLoading.value = false;
    };
    el.addEventListener('error', videoErrorHandler.value);
  }
  if (!videoPlayHandler.value) {
    videoPlayHandler.value = () => {
      isPlaying.value = true;
      isLoading.value = false;
    };
    el.addEventListener('play', videoPlayHandler.value);
  }
  if (!videoPlayingHandler.value) {
    videoPlayingHandler.value = () => {
      candidateFailoverInProgress.value = false;
      playbackStarted.value = true;
      suppressTransientErrors.value = false;
      resetHlsRecoveryState();
      clearErrorMessage();
      isLoading.value = false;
      isPlaying.value = true;
      syncCurrentTime(el);
      syncDuration(el);
      startPlaybackHeartbeat(url, videoStore.streamTitle || '', currentSessionId.value);
      if (loadTimeout.value) {
        clearTimeout(loadTimeout.value);
        loadTimeout.value = null;
      }
    };
    el.addEventListener('playing', videoPlayingHandler.value);
  }
  if (!videoCanPlayHandler.value) {
    videoCanPlayHandler.value = () => {
      syncCurrentTime(el);
      syncDuration(el);
      if (!el.paused && !el.ended) {
        isLoading.value = false;
      }
    };
    el.addEventListener('canplay', videoCanPlayHandler.value);
  }
  if (!videoWaitingHandler.value) {
    videoWaitingHandler.value = () => {
      if (!errorMessage.value) {
        isLoading.value = true;
      }
    };
    el.addEventListener('waiting', videoWaitingHandler.value);
    el.addEventListener('stalled', videoWaitingHandler.value);
  }
  if (!loadTimeout.value) {
    loadTimeout.value = setTimeout(() => {
      if (!errorMessage.value && isLoading.value) {
        if (failoverToNextPreviewCandidate()) {
          return;
        }
        setErrorMessage('No data received from the stream.', 4000);
        isLoading.value = false;
      }
    }, 60000);
  }
  const type = detectStreamType(url, videoStore.streamType);
  isLoading.value = true;
  console.info('[FloatingPlayer] initPlayer', {url, type});

  try {
    if (type === 'hls' && Hls.isSupported()) {
      const hls = new Hls({
        lowLatencyMode: true,
        manifestLoadingTimeOut: 45000,
        manifestLoadingMaxRetry: 2,
        manifestLoadingRetryDelay: 1000,
        levelLoadingTimeOut: 45000,
        levelLoadingMaxRetry: 2,
        levelLoadingRetryDelay: 1000,
      });
      if (runId !== activeInitRunId || token !== initToken.value) {
        try {
          hls.destroy();
        } catch (error) {
          console.warn('Failed to destroy stale HLS instance:', error);
        }
        return;
      }
      hlsInstance.value = hls;
      hlsInstances.add(hls);
      hls.loadSource(url);
      hls.attachMedia(el);
      hls.on(Hls.Events.MANIFEST_PARSED, async () => {
        const initialLevel = hls.levels?.[hls.currentLevel] || hls.levels?.[0];
        if (initialLevel) {
          const codecString = initialLevel.codecs || initialLevel.codec || initialLevel.attrs?.CODECS;
          const {video, audio} = parseCodecString(codecString);
          applyCodecInfo(video, audio);
          if (initialLevel.width && initialLevel.height) {
            streamDetails.value = {
              ...streamDetails.value,
              resolution: `${initialLevel.width}x${initialLevel.height}`,
            };
          } else if (initialLevel.height) {
            streamDetails.value = {
              ...streamDetails.value,
              resolution: `${initialLevel.height}p`,
            };
          }
          applyBitrate(initialLevel.bitrate);
        }
        const {started, error} = await safePlay(el);
        applyPersistedVolume(el);
        attachTextTrackListeners(el);
        restorePendingSeek(el);
        if (!started) {
          if (error?.name === 'NotAllowedError') {
            setErrorMessage('Press play to start playback.', 5000);
          } else if (error?.name !== 'AbortError') {
            setErrorMessage('Unable to start playback. The stream may be invalid or unsupported.');
          }
        }
        isLoading.value = !started;
        snapToAspectRatio();
      });
      hls.on(Hls.Events.LEVEL_SWITCHED, (_event, data) => {
        const level = hls.levels?.[data.level];
        if (!level) return;
        const codecString = level.codecs || level.codec || level.attrs?.CODECS;
        const {video, audio} = parseCodecString(codecString);
        applyCodecInfo(video, audio);
        if (level.width && level.height) {
          streamDetails.value = {
            ...streamDetails.value,
            resolution: `${level.width}x${level.height}`,
          };
        } else if (level.height) {
          streamDetails.value = {
            ...streamDetails.value,
            resolution: `${level.height}p`,
          };
        }
        applyBitrate(level.bitrate);
      });
      hls.on(Hls.Events.ERROR, (_event, data) => {
        const logMethod = data?.fatal ? console.warn : console.info;
        logMethod('[FloatingPlayer] HLS error', data);
        const status = data?.response?.code;
        const details = data?.details;
        if (data?.fatal && data?.type === Hls.ErrorTypes.NETWORK_ERROR &&
          hlsRecoveryState.value.networkRecoveryAttempts < 2) {
          hlsRecoveryState.value = {
            ...hlsRecoveryState.value,
            networkRecoveryAttempts: hlsRecoveryState.value.networkRecoveryAttempts + 1,
          };
          console.info('[FloatingPlayer] retrying HLS network load', hlsRecoveryState.value.networkRecoveryAttempts);
          hls.startLoad();
          return;
        }
        if (data?.fatal && data?.type === Hls.ErrorTypes.MEDIA_ERROR) {
          const recoveryAttempts = hlsRecoveryState.value.mediaRecoveryAttempts;
          const now = Date.now();
          if (recoveryAttempts < 2) {
            hlsRecoveryState.value = {
              ...hlsRecoveryState.value,
              mediaRecoveryAttempts: recoveryAttempts + 1,
              lastMediaRecoveryAt: now,
            };
            console.info('[FloatingPlayer] recovering HLS media error', recoveryAttempts + 1);
            if (recoveryAttempts > 0 && typeof hls.swapAudioCodec === 'function') {
              hls.swapAudioCodec();
            }
            hls.recoverMediaError();
            return;
          }
        }
        if (status === 401 || status === 403) {
          if (failoverToNextPreviewCandidate()) {
            return;
          }
          setErrorMessage('Stream rejected (unauthorized).');
        } else if (status === 404) {
          if (failoverToNextPreviewCandidate()) {
            return;
          }
          setErrorMessage('Stream not found.', data?.fatal ? 0 : 3500);
        } else if (details === Hls.ErrorDetails.MANIFEST_LOAD_ERROR && data?.fatal) {
          if (failoverToNextPreviewCandidate()) {
            return;
          }
          setErrorMessage('Stream manifest failed to load.', data?.fatal ? 0 : 3500);
        } else if (data?.fatal) {
          if (failoverToNextPreviewCandidate()) {
            return;
          }
          setErrorMessage('Unable to load stream. Please try again.');
        }
        if (data?.fatal || details === Hls.ErrorDetails.MANIFEST_LOAD_ERROR) {
          isLoading.value = false;
          suppressTransientErrors.value = false;
        }
      });
      return;
    }

    if (type === 'mpegts' && mpegts.isSupported()) {
      const player = mpegts.createPlayer({
        type: 'mpegts',
        url,
        isLive: true,
      });
      if (!player) {
        setErrorMessage('Unable to start playback.');
        isLoading.value = false;
        return;
      }
      if (runId !== activeInitRunId || token !== initToken.value) {
        try {
          player.pause();
          player.unload?.();
          player.detachMediaElement?.();
        } catch (error) {
          console.warn('Failed to stop stale MPEG-TS instance:', error);
        }
        try {
          player.destroy();
        } catch (error) {
          console.warn('Failed to destroy stale MPEG-TS instance:', error);
        }
        return;
      }
      mpegtsInstance.value = player;
      mpegtsInstances.add(player);
      player.attachMediaElement(el);
      player.load();
      await waitForMpegtsStartupReady(player, el);
      if (runId !== activeInitRunId || token !== initToken.value) {
        try {
          player.pause();
          player.unload?.();
          player.detachMediaElement?.();
        } catch (error) {
          console.warn('Failed to stop stale MPEG-TS instance after startup delay:', error);
        }
        try {
          player.destroy();
        } catch (error) {
          console.warn('Failed to destroy stale MPEG-TS instance after startup delay:', error);
        }
        return;
      }
      const {started, error} = await safePlay(el);
      applyPersistedVolume(el);
      attachTextTrackListeners(el);
      restorePendingSeek(el);
      if (!started) {
        if (error?.name === 'NotAllowedError') {
          setErrorMessage('Press play to start playback.', 5000);
        } else if (error?.name !== 'AbortError') {
          setErrorMessage('Unable to start playback. The stream may be invalid or unsupported.');
        }
      }
      isLoading.value = !started;
      snapToAspectRatio();
      player.on?.(mpegts.Events.MEDIA_INFO, (info) => {
        if (info?.width && info?.height) {
          streamDetails.value = {
            ...streamDetails.value,
            resolution: `${info.width}x${info.height}`,
          };
        }
        if (info?.videoCodec || info?.audioCodec) {
          applyCodecInfo(
            parseCodecLabel(info.videoCodec),
            parseCodecLabel(info.audioCodec),
          );
        }
        if (typeof info?.bitrate === 'number') {
          applyBitrate(info.bitrate);
        }
      });
      player.on?.(mpegts.Events.ERROR, (err) => {
        console.warn('[FloatingPlayer] MPEGTS error', err);
        suppressTransientErrors.value = false;
        if (failoverToNextPreviewCandidate()) {
          return;
        }
        setErrorMessage('Unable to load stream. Please try again.');
        isLoading.value = false;
      });
      return;
    }

    el.src = url;
    const {started, error} = await safePlay(el);
    applyPersistedVolume(el);
    attachTextTrackListeners(el);
    restorePendingSeek(el);
    if (!started) {
      if (error?.name === 'NotAllowedError') {
        setErrorMessage('Press play to start playback.', 5000);
      } else if (error?.name !== 'AbortError') {
        setErrorMessage('Unable to start playback. The stream may be invalid or unsupported.');
      }
    }
    isLoading.value = !started;
    snapToAspectRatio();
  } catch (error) {
    console.error('Failed to initialize player:', error);
    suppressTransientErrors.value = false;
    if (failoverToNextPreviewCandidate()) {
      return;
    }
    setErrorMessage('Unable to start playback.');
    isLoading.value = false;
  }
}

async function togglePlayback() {
  const el = getVideoElement();
  if (!el) {
    return;
  }
  if (el.paused || el.ended) {
    const {started, error} = await safePlay(el);
    if (!started && error?.name === 'NotAllowedError') {
      setErrorMessage('Press play to start playback.', 5000);
    }
    return;
  }
  el.pause();
}

function handleVolumeInput(event) {
  const el = getVideoElement();
  if (!el) {
    return;
  }
  const nextValue = Math.max(0, Math.min(1, Number(event?.target?.value) || 0));
  el.muted = nextValue <= 0;
  el.volume = nextValue;
  syncVolumeState(el);
  if (nextValue > 0) {
    lastVolumeBeforeMute.value = nextValue;
  }
}

function toggleMute() {
  const el = getVideoElement();
  if (!el) {
    return;
  }
  if (el.muted || el.volume <= 0) {
    const restoreVolume = Math.max(0.05, Math.min(1, lastVolumeBeforeMute.value || 1));
    el.muted = false;
    el.volume = restoreVolume;
  } else {
    lastVolumeBeforeMute.value = Math.max(0.05, el.volume || lastVolumeBeforeMute.value || 1);
    el.muted = true;
  }
  syncVolumeState(el);
}

function toggleCaptions() {
  const tracks = collectCaptionTracks();
  if (!tracks.length) {
    return;
  }
  const enable = !tracks.some((track) => track.mode === 'showing');
  let activated = false;
  tracks.forEach((track) => {
    if (enable && !activated) {
      track.mode = 'showing';
      activated = true;
      return;
    }
    track.mode = 'disabled';
  });
  refreshCaptionState();
}

function beginSeekDrag() {
  isSeekDragging.value = true;
}

function finishSeekDrag() {
  if (!isSeekDragging.value) {
    return;
  }
  isSeekDragging.value = false;
  if (seekDraftSeconds.value != null) {
    commitSeek(seekDraftSeconds.value);
  }
  seekDraftSeconds.value = null;
}

function handleSeekInput(event) {
  const nextValue = Math.max(0, Math.min(seekMaxSeconds.value, Number(event?.target?.value) || 0));
  seekDraftSeconds.value = nextValue;
  if (isMobile.value) {
    showMobileControls();
  } else {
    showDesktopControls();
  }
}

function commitSeekInput(event) {
  commitSeek(Math.max(0, Math.min(seekMaxSeconds.value, Number(event?.target?.value) || 0)));
  isSeekDragging.value = false;
  seekDraftSeconds.value = null;
}

function commitSeek(nextSeconds) {
  if (!canSeek.value) {
    return;
  }
  const el = getVideoElement();
  if (!el) {
    return;
  }
  const target = Math.max(0, Math.min(seekMaxSeconds.value, Math.floor(Number(nextSeconds) || 0)));
  if (usesRestartBasedSeek()) {
    queueRestartSeek(target);
    return;
  }
  try {
    el.currentTime = target;
    currentTimeSeconds.value = target;
  } catch (error) {
    console.warn('[FloatingPlayer] seek failed', error);
  }
}

function stepSeek(deltaSeconds) {
  if (!canSeek.value) {
    return;
  }
  const base = seekDraftSeconds.value != null ? seekDraftSeconds.value : effectiveCurrentTime.value;
  const nextValue = Math.max(0, Math.min(seekMaxSeconds.value, Math.floor(base + deltaSeconds)));
  seekDraftSeconds.value = nextValue;
  commitSeek(nextValue);
}

function closePlayer() {
  console.info('[FloatingPlayer] closePlayer');
  pendingSeekTime.value = null;
  cleanupPlayer();
  videoStore.hidePlayer();
}

function selectPlaybackProfile(profileId) {
  if (!profileId || profileId === videoStore.selectedPlaybackProfile) {
    return;
  }
  const nextProfile = (videoStore.playbackProfiles || []).find((profile) => profile.id === profileId);
  if (!nextProfile) {
    return;
  }
  const el = getVideoElement();
  const absoluteCurrentTime = currentAbsolutePlaybackTime(el);
  if (usesRestartBasedSeek(videoStore.streamUrl, nextProfile.seekMode)) {
    const nextUrl = updatePlaybackUrl(
      videoStore.streamUrl,
      nextProfile.profile || '',
      absoluteCurrentTime > 0 ? absoluteCurrentTime : null,
    );
    suppressManagedSeekUntil.value = Date.now() + 1200;
    pendingSeekTime.value = null;
    currentTimeSeconds.value = absoluteCurrentTime;
    videoStore.setPlaybackProfile(profileId, nextUrl, nextProfile.streamType, nextProfile.seekMode);
    return;
  }
  if (absoluteCurrentTime > 0) {
    pendingSeekTime.value = absoluteCurrentTime;
  } else {
    pendingSeekTime.value = null;
  }
  const nextUrl = updatePlaybackUrl(videoStore.streamUrl, nextProfile.profile || '', null);
  videoStore.setPlaybackProfile(profileId, nextUrl, nextProfile.streamType, nextProfile.seekMode);
}

async function togglePiP() {
  const el = getVideoElement();
  if (!el) return;
  try {
    if (document.pictureInPictureElement) {
      await document.exitPictureInPicture();
    } else {
      await el.requestPictureInPicture();
    }
    updatePiPState();
  } catch (error) {
    console.warn('PiP failed:', error);
  }
}

function toggleFullScreen() {
  const root = playerRoot.value;
  if (!root) return;
  try {
    if (document.fullscreenElement) {
      document.exitFullscreen();
    } else {
      root.requestFullscreen();
    }
  } catch (error) {
    console.warn('Fullscreen failed:', error);
  }
}

function startDrag(event) {
  if (event?.target?.closest?.('.floating-player__actions') ||
    event?.target?.closest?.('.floating-player__header-dropdowns')) {
    return;
  }
  dragState.value = {
    startX: event.clientX,
    startY: event.clientY,
    initial: {...videoStore.position},
  };
  window.addEventListener('mousemove', handleDrag);
  window.addEventListener('mouseup', stopDrag);
}

function startDragTouch(event) {
  if (event?.target?.closest?.('.floating-player__actions') ||
    event?.target?.closest?.('.floating-player__header-dropdowns')) {
    return;
  }
  const touch = event.touches[0];
  if (!touch) {
    return;
  }
  dragState.value = {
    startX: touch.clientX,
    startY: touch.clientY,
    initial: {...videoStore.position},
  };
  window.addEventListener('touchmove', handleDragTouch, {passive: false});
  window.addEventListener('touchend', stopDragTouch);
}

function handleDrag(event) {
  if (!dragState.value) return;
  const dx = event.clientX - dragState.value.startX;
  const dy = event.clientY - dragState.value.startY;
  const next = {
    right: (dragState.value.initial.right ?? 0) - dx,
    bottom: (dragState.value.initial.bottom ?? 0) - dy,
  };
  videoStore.setPosition(next);
}

function handleDragTouch(event) {
  if (!dragState.value) return;
  const touch = event.touches[0];
  const dx = touch.clientX - dragState.value.startX;
  const dy = touch.clientY - dragState.value.startY;
  const next = {
    right: (dragState.value.initial.right ?? 0) - dx,
    bottom: (dragState.value.initial.bottom ?? 0) - dy,
  };
  videoStore.setPosition(next);
}

function stopDrag() {
  dragState.value = null;
  window.removeEventListener('mousemove', handleDrag);
  window.removeEventListener('mouseup', stopDrag);
}

function stopDragTouch() {
  dragState.value = null;
  window.removeEventListener('touchmove', handleDragTouch);
  window.removeEventListener('touchend', stopDragTouch);
}

function startResize(direction, event) {
  const viewportWidth = window.innerWidth || 0;
  const viewportHeight = window.innerHeight || 0;
  const initialRight = videoStore.position.right ?? 0;
  const initialBottom = videoStore.position.bottom ?? 0;
  const initialLeft = videoStore.position.left ?? (viewportWidth - initialRight - videoStore.size.width);
  const initialTop = videoStore.position.top ?? (viewportHeight - initialBottom - videoStore.size.height);
  resizeState.value = {
    startX: event.clientX,
    startY: event.clientY,
    initial: {...videoStore.size},
    initialPos: {
      left: initialLeft,
      top: initialTop,
      right: initialRight,
      bottom: initialBottom,
      viewportWidth,
      viewportHeight,
    },
    direction,
  };
  window.addEventListener('mousemove', handleResize);
  window.addEventListener('mouseup', stopResize);
}

function startResizeTouch(direction, event) {
  const touch = event.touches[0];
  const viewportWidth = window.innerWidth || 0;
  const viewportHeight = window.innerHeight || 0;
  const initialRight = videoStore.position.right ?? 0;
  const initialBottom = videoStore.position.bottom ?? 0;
  const initialLeft = videoStore.position.left ?? (viewportWidth - initialRight - videoStore.size.width);
  const initialTop = videoStore.position.top ?? (viewportHeight - initialBottom - videoStore.size.height);
  resizeState.value = {
    startX: touch.clientX,
    startY: touch.clientY,
    initial: {...videoStore.size},
    initialPos: {
      left: initialLeft,
      top: initialTop,
      right: initialRight,
      bottom: initialBottom,
      viewportWidth,
      viewportHeight,
    },
    direction,
  };
  window.addEventListener('touchmove', handleResizeTouch, {passive: false});
  window.addEventListener('touchend', stopResizeTouch);
}

function handleResize(event) {
  if (!resizeState.value) return;
  const dx = event.clientX - resizeState.value.startX;
  const dy = event.clientY - resizeState.value.startY;
  const {direction, initial, initialPos} = resizeState.value;
  const minWidth = 320;
  const minHeight = 260;
  let width = initial.width;
  let height = initial.height;
  let left = initialPos.left;
  let top = initialPos.top;

  if (direction.includes('r')) {
    width = Math.max(minWidth, initial.width + dx);
  }
  if (direction.includes('l')) {
    width = Math.max(minWidth, initial.width - dx);
    left = initialPos.left + dx;
  }
  if (direction.includes('b')) {
    height = Math.max(minHeight, initial.height + dy);
  }
  if (direction.includes('t')) {
    height = Math.max(minHeight, initial.height - dy);
    top = initialPos.top + dy;
  }

  ({width, height} = adjustSizeToAspect(width, height, direction, minWidth, minHeight));

  const right = Math.max(0, initialPos.viewportWidth - left - width);
  const bottom = Math.max(0, initialPos.viewportHeight - top - height);

  videoStore.setSize({width, height});
  videoStore.setPosition({right, bottom});
}

function handleResizeTouch(event) {
  if (!resizeState.value) return;
  const touch = event.touches[0];
  const dx = touch.clientX - resizeState.value.startX;
  const dy = touch.clientY - resizeState.value.startY;
  const {direction, initial, initialPos} = resizeState.value;
  const minWidth = 320;
  const minHeight = 260;
  let width = initial.width;
  let height = initial.height;
  let left = initialPos.left;
  let top = initialPos.top;

  if (direction.includes('r')) {
    width = Math.max(minWidth, initial.width + dx);
  }
  if (direction.includes('l')) {
    width = Math.max(minWidth, initial.width - dx);
    left = initialPos.left + dx;
  }
  if (direction.includes('b')) {
    height = Math.max(minHeight, initial.height + dy);
  }
  if (direction.includes('t')) {
    height = Math.max(minHeight, initial.height - dy);
    top = initialPos.top + dy;
  }

  ({width, height} = adjustSizeToAspect(width, height, direction, minWidth, minHeight));

  const right = Math.max(0, initialPos.viewportWidth - left - width);
  const bottom = Math.max(0, initialPos.viewportHeight - top - height);

  videoStore.setSize({width, height});
  videoStore.setPosition({right, bottom});
}

function stopResize() {
  resizeState.value = null;
  window.removeEventListener('mousemove', handleResize);
  window.removeEventListener('mouseup', stopResize);
  snapToAspectRatio();
}

function stopResizeTouch() {
  resizeState.value = null;
  window.removeEventListener('touchmove', handleResizeTouch);
  window.removeEventListener('touchend', stopResizeTouch);
  snapToAspectRatio();
}

function snapToAspectRatio() {
  const minWidth = 320;
  const minHeight = 260;
  let {width, height} = videoStore.size;
  if (!width || !height) return;
  ({width, height} = adjustSizeToAspect(width, height, 'auto', minWidth, minHeight));
  videoStore.setSize({width, height});
}

function adjustSizeToAspect(width, height, direction, minWidth, minHeight) {
  const el = getVideoElement();
  if (!el || !el.videoWidth || !el.videoHeight) {
    return {width, height};
  }
  const targetRatio = el.videoWidth / el.videoHeight;
  if (!Number.isFinite(targetRatio) || targetRatio <= 0) {
    return {width, height};
  }

  const header = document.querySelector('.floating-player__header');
  const headerHeight = header ? header.getBoundingClientRect().height : 0;
  const minBodyHeight = Math.max(1, minHeight - headerHeight);
  const bodyHeight = Math.max(minBodyHeight, height - headerHeight);

  if (direction.includes('l') || direction.includes('r')) {
    const newBodyHeight = Math.max(minBodyHeight, Math.round(width / targetRatio));
    height = Math.max(minHeight, newBodyHeight + headerHeight);
  } else if (direction.includes('t') || direction.includes('b')) {
    const newWidth = Math.max(minWidth, Math.round(bodyHeight * targetRatio));
    width = newWidth;
  } else {
    const widthBasedHeight = Math.max(minBodyHeight, Math.round(width / targetRatio));
    const heightBasedWidth = Math.max(minWidth, Math.round(bodyHeight * targetRatio));
    const widthDelta = Math.abs(widthBasedHeight - bodyHeight);
    const heightDelta = Math.abs(heightBasedWidth - width);
    if (widthDelta <= heightDelta) {
      height = Math.max(minHeight, widthBasedHeight + headerHeight);
    } else {
      width = heightBasedWidth;
    }
  }

  width = Math.max(minWidth, width);
  height = Math.max(minHeight, height);
  return {width, height};
}

function handleFullscreenChange() {
  updateFullscreenState();
}

function handlePiPChange() {
  updatePiPState();
}

function requestPlayerInit() {
  playerInitSequence = playerInitSequence.catch(() => {
  }).then(async () => {
    if (!videoStore.isVisible || !videoStore.streamUrl) {
      return;
    }
    await initPlayer();
  });
  return playerInitSequence;
}

watch(
  playbackInitKey,
  (nextKey, previousKey) => {
    if (!nextKey || nextKey === previousKey) {
      return;
    }
    void requestPlayerInit();
  },
);

watch(
  () => videoStore.isVisible,
  (visible) => {
    if (!visible) {
      clearVodPreviewMetadataRetry();
      activeVodMetadataUrl.value = '';
      cleanupPlayer();
      return;
    }
    const defaultPosition = {right: 24, bottom: 24, left: null, top: null};
    const mobilePosition = {right: 0, bottom: 0, left: 0, top: null};
    const defaultSize = {width: 640, height: 360};
    const mobileSize = {width: window.innerWidth, height: Math.min(window.innerHeight * 0.5, 300)};

    if (isMobile.value) {
      videoStore.setPosition(mobilePosition);
      videoStore.setSize(mobileSize);
      showMobileControls();
    } else {
      videoStore.setPosition(defaultPosition);
      videoStore.setSize(defaultSize);
      desktopControlsActive.value = false;
    }

    void loadVodPreviewMetadata();
  },
);

watch(
  () => videoStore.previewMetadataUrl,
  () => {
    clearVodPreviewMetadataRetry();
    activeVodMetadataUrl.value = '';
    if (videoStore.isVisible) {
      void loadVodPreviewMetadata();
    }
  },
);

watch(isMobile, (newIsMobile, oldIsMobile) => {
  if (newIsMobile !== oldIsMobile && videoStore.isVisible) {
    const defaultPosition = {right: 24, bottom: 24, left: null, top: null};
    const mobilePosition = {right: 0, bottom: 0, left: 0, top: null};
    const defaultSize = {width: 640, height: 360};
    const mobileSize = {width: window.innerWidth, height: Math.min(window.innerHeight * 0.5, 300)};

    if (newIsMobile) {
      videoStore.setPosition(mobilePosition);
      videoStore.setSize(mobileSize);
      showMobileControls();
    } else {
      videoStore.setPosition(defaultPosition);
      videoStore.setSize(defaultSize);
      mobileControlsVisible.value = false;
      desktopControlsActive.value = false;
    }
  }
});

watch(
  () => videoStore.seekMode,
  () => {
    const el = getVideoElement();
    if (el) {
      syncCurrentTime(el);
    }
  },
);

if (typeof document !== 'undefined') {
  document.addEventListener('fullscreenchange', handleFullscreenChange);
  document.addEventListener('enterpictureinpicture', handlePiPChange);
  document.addEventListener('leavepictureinpicture', handlePiPChange);
}

onBeforeUnmount(() => {
  clearVodPreviewMetadataRetry();
  cleanupPlayer();
  if (typeof document !== 'undefined') {
    document.removeEventListener('fullscreenchange', handleFullscreenChange);
    document.removeEventListener('enterpictureinpicture', handlePiPChange);
    document.removeEventListener('leavepictureinpicture', handlePiPChange);
  }
});

onUnmounted(() => {
  stopPlaybackHeartbeat(true);
});
</script>

<style scoped>
.floating-player {
  position: fixed;
  right: 24px;
  bottom: 24px;
  z-index: 10000;
  display: flex;
  min-width: 320px;
  min-height: 260px;
  flex-direction: column;
  overflow: visible;
  border: var(--tic-elevated-border);
  border-radius: var(--tic-radius-lg);
  background: #0b0d11;
  box-shadow: 0 18px 40px rgba(0, 0, 0, 0.4);
}

.floating-player--fullscreen {
  border-radius: 0;
}

.floating-player__header {
  display: flex;
  align-items: center;
  gap: 8px;
  padding: 6px 10px;
  cursor: move;
  user-select: none;
  background: rgba(14, 18, 24, 0.96);
  color: #fff;
}

.floating-player__title {
  min-width: 0;
  flex: 1 1 auto;
  display: flex;
  flex-direction: column;
  gap: 2px;
}

.floating-player__title-text,
.floating-player__meta {
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.floating-player__title-text {
  font-size: 0.85rem;
  font-weight: 600;
}

.floating-player__meta {
  font-size: 0.72rem;
  color: rgba(255, 255, 255, 0.72);
}

.floating-player__header-dropdowns,
.floating-player__actions {
  display: flex;
  align-items: center;
  gap: 4px;
  flex: 0 0 auto;
}

.floating-player__header-dropdown {
  flex: 0 0 auto;
}

.floating-player__header-dropdown :deep(.q-btn) {
  min-height: 30px;
  padding: 0 10px;
  border-radius: 999px;
  color: #fff;
  text-transform: none;
}

.floating-player__header-dropdown :deep(.q-btn__content) {
  gap: 6px;
  text-transform: none;
}

.floating-player__header-menu {
  min-width: 220px;
}

.floating-player__header-item--active {
  color: var(--q-primary);
}

.floating-player__actions :deep(.q-btn),
.floating-player__control-btn :deep(.q-btn) {
  color: #fff;
}

.floating-player__error-floating {
  position: absolute;
  left: 10px;
  right: 10px;
  top: 42px;
  z-index: 6;
  pointer-events: none;
  overflow: hidden;
  border-radius: var(--tic-radius-md);
  border: 1px solid rgba(255, 127, 127, 0.45);
  background: rgba(0, 0, 0, 0.58);
  color: #ffb5b5;
  padding: 5px 8px;
  font-size: 0.72rem;
  line-height: 1.25;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.floating-player__body {
  position: relative;
  min-height: 160px;
  flex: 1 1 auto;
  overflow: hidden;
  background: #000;
}

.floating-player__video {
  display: block;
  width: 100%;
  height: 100%;
  background: #000;
  object-fit: contain;
}

.floating-player__video--loading {
  opacity: 0;
  pointer-events: none;
}

.floating-player__overlay {
  position: absolute;
  inset: 0;
  z-index: 4;
  display: flex;
  align-items: center;
  justify-content: center;
  background: rgba(0, 0, 0, 0.35);
}

.floating-player__controls {
  position: absolute;
  inset: auto 0 0 0;
  z-index: 5;
  pointer-events: none;
  opacity: 0;
  transition: opacity 140ms ease;
}

.floating-player--controls-visible .floating-player__controls {
  opacity: 1;
  pointer-events: auto;
}

.floating-player__controls-scrim {
  position: absolute;
  inset: 0;
  background: linear-gradient(to top, rgba(0, 0, 0, 0.76), rgba(0, 0, 0, 0.48) 55%, rgba(0, 0, 0, 0));
}

.floating-player__controls-inner {
  position: relative;
  display: flex;
  flex-direction: column;
  gap: 10px;
  padding: 36px 14px 14px;
}

.floating-player__controls-row {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  min-width: 0;
}

.floating-player__controls-left,
.floating-player__controls-right {
  display: flex;
  align-items: center;
  gap: 6px;
  min-width: 0;
}

.floating-player__control-btn {
  color: #fff;
}

.floating-player__time-group {
  display: inline-flex;
  align-items: center;
  gap: 6px;
  min-width: 0;
  color: #fff;
  font-size: 0.78rem;
  font-weight: 500;
}

.floating-player__time-value {
  white-space: nowrap;
}

.floating-player__time-separator {
  opacity: 0.72;
}

.floating-player__volume-group {
  display: inline-flex;
  align-items: center;
  gap: 0;
  padding-left: 2px;
  border-radius: 999px;
  outline: none;
}

.floating-player__volume-slider-wrap {
  width: 0;
  overflow: hidden;
  margin-right: 6px;
  opacity: 0;
  transition: width 140ms ease, opacity 140ms ease;
}

.floating-player__volume-group:hover .floating-player__volume-slider-wrap,
.floating-player__volume-group:focus-within .floating-player__volume-slider-wrap {
  width: 90px;
  opacity: 1;
}

.floating-player__seek-row {
  padding: 0 2px;
}

.floating-player__seek-slider-wrap {
  width: 100%;
}

.floating-player__slider {
  --slider-track-height: 4px;
  --slider-thumb-size: 12px;
  width: 100%;
  margin: 0;
  padding: calc(var(--slider-thumb-size) / 2) 0;
  border: 0;
  outline: none;
  background: transparent;
  color: inherit;
  -webkit-appearance: none;
  appearance: none;
}

.floating-player__slider:disabled {
  cursor: default;
  opacity: 0.55;
}

.floating-player__slider::-webkit-slider-runnable-track {
  height: var(--slider-track-height);
  border-radius: 999px;
  background: linear-gradient(
    to right,
    var(--slider-fill, var(--q-secondary)) 0%,
    var(--slider-fill, var(--q-secondary)) var(--slider-progress, 0%),
    rgba(255, 255, 255, 0.3) var(--slider-progress, 0%),
    rgba(255, 255, 255, 0.3) 100%
  );
}

.floating-player__slider::-moz-range-track {
  height: var(--slider-track-height);
  border: 0;
  border-radius: 999px;
  background: rgba(255, 255, 255, 0.3);
}

.floating-player__slider::-moz-range-progress {
  height: var(--slider-track-height);
  border-radius: 999px;
  background: var(--slider-fill, var(--q-secondary));
}

.floating-player__slider::-webkit-slider-thumb {
  width: var(--slider-thumb-size);
  height: var(--slider-thumb-size);
  margin-top: calc((var(--slider-track-height) - var(--slider-thumb-size)) / 2);
  border: 0;
  border-radius: 50%;
  background: #fff;
  box-shadow: 0 0 0 1px rgba(0, 0, 0, 0.18);
  -webkit-appearance: none;
  appearance: none;
}

.floating-player__slider::-moz-range-thumb {
  width: var(--slider-thumb-size);
  height: var(--slider-thumb-size);
  border: 0;
  border-radius: 50%;
  background: #fff;
  box-shadow: 0 0 0 1px rgba(0, 0, 0, 0.18);
}

.floating-player__slider--seek {
  --slider-fill: var(--q-secondary);
  --slider-track-height: 4px;
  --slider-thumb-size: 12px;
}

.floating-player__slider--volume {
  --slider-fill: rgba(255, 255, 255, 0.92);
  --slider-track-height: 3px;
  --slider-thumb-size: 10px;
  width: 90px;
  margin-right: 2px;
}

.floating-player__resize-handle {
  position: absolute;
  width: 12px;
  height: 12px;
  background: transparent;
  cursor: nwse-resize;
}

.floating-player__resize-handle--br {
  right: 4px;
  bottom: 4px;
  border-right: 1px solid rgba(255, 255, 255, 0.7);
  border-bottom: 1px solid rgba(255, 255, 255, 0.7);
}

.floating-player__resize-handle--bl {
  left: 4px;
  bottom: 4px;
  cursor: nesw-resize;
  border-left: 1px solid rgba(255, 255, 255, 0.7);
  border-bottom: 1px solid rgba(255, 255, 255, 0.7);
}

.floating-player__resize-handle--tr {
  top: 4px;
  right: 4px;
  cursor: nesw-resize;
  border-top: 1px solid rgba(255, 255, 255, 0.7);
  border-right: 1px solid rgba(255, 255, 255, 0.7);
}

.floating-player__resize-handle--tl {
  top: 4px;
  left: 4px;
  border-top: 1px solid rgba(255, 255, 255, 0.7);
  border-left: 1px solid rgba(255, 255, 255, 0.7);
}

@media (max-width: 1023px) {
  .floating-player__controls-inner {
    padding-top: 28px;
  }
}

@media (max-width: 600px) {
  .floating-player {
    right: 0;
    bottom: 0;
    left: 0;
    top: auto;
    width: 100% !important;
    border-radius: 12px 12px 0 0;
  }

  .floating-player__header {
    gap: 6px;
    padding: 8px 10px;
  }

  .floating-player__header-dropdown :deep(.q-btn) {
    min-width: 30px;
    padding: 0 6px;
  }

  .floating-player__controls {
    opacity: 1;
  }

  .floating-player__controls-inner {
    gap: 8px;
    padding: 28px 12px 12px;
  }

  .floating-player__controls-row {
    gap: 10px;
  }

  .floating-player__time-group {
    gap: 4px;
    font-size: 0.75rem;
  }

  .floating-player__seek-row {
    padding: 0 1px;
  }

  .floating-player__resize-handle {
    display: none;
  }
}
</style>
