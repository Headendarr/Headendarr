<template>
  <div
    v-if="videoStore.isVisible"
    class="floating-player"
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
      <div class="floating-player__actions">
        <q-btn
          dense
          flat
          round
          icon="picture_in_picture_alt"
          @click.stop="togglePiP"
          :disable="!pipSupported"
          v-if="!isMobile"
        >
          <q-tooltip class="bg-white text-primary">Picture in Picture</q-tooltip>
        </q-btn>
        <q-btn
          dense
          flat
          round
          icon="fullscreen"
          @click.stop="toggleFullScreen"
          v-else
        >
          <q-tooltip class="bg-white text-primary">Full screen</q-tooltip>
        </q-btn>
        <q-btn
          dense
          flat
          round
          icon="close"
          @touchstart.stop
          @click.stop="closePlayer"
        >
          <q-tooltip class="bg-white text-primary">Close</q-tooltip>
        </q-btn>
      </div>
    </div>

    <div class="floating-player__body">
      <div v-if="isLoading" class="floating-player__overlay">
        <q-spinner size="32px" color="white" />
      </div>
      <div v-if="errorMessage" class="floating-player__error">
        {{ errorMessage }}
      </div>
      <video
        ref="videoEl"
        :class="['floating-player__video', { 'floating-player__video--loading': isLoading }]"
        controls
        playsinline
      />
    </div>

    <div
      class="floating-player__resize-handle floating-player__resize-handle--br"
      @mousedown.stop="startResize('br', $event)"
      @touchstart.prevent.stop="startResizeTouch('br', $event)"
    ></div>
    <div
      class="floating-player__resize-handle floating-player__resize-handle--bl"
      @mousedown.stop="startResize('bl', $event)"
      @touchstart.prevent.stop="startResizeTouch('bl', $event)"
    ></div>
    <div
      class="floating-player__resize-handle floating-player__resize-handle--tr"
      @mousedown.stop="startResize('tr', $event)"
      @touchstart.prevent.stop="startResizeTouch('tr', $event)"
    ></div>
    <div
      class="floating-player__resize-handle floating-player__resize-handle--tl"
      @mousedown.stop="startResize('tl', $event)"
      @touchstart.prevent.stop="startResizeTouch('tl', $event)"
    ></div>
  </div>
</template>

<script setup>
import axios from 'axios';
import {computed, nextTick, onBeforeUnmount, onMounted, onUnmounted, ref, watch} from 'vue';
import {useVideoStore} from 'stores/video';
import Hls from 'hls.js';
import mpegts from 'mpegts.js';

const videoStore = useVideoStore();
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
const pipSupported = computed(() => !!document.pictureInPictureEnabled);
const isMobile = ref(window.innerWidth < 600);
const volumeHandler = ref(null);
const videoErrorHandler = ref(null);
const videoPlayingHandler = ref(null);
const videoWaitingHandler = ref(null);
const metadataHandler = ref(null);
const resizeHandler = ref(null);
const loadTimeout = ref(null);
const loadingStartedAt = ref(0);
const streamDetails = ref({
  resolution: '',
  videoCodec: '',
  audioCodec: '',
  bitrate: null,
});
const directStartAuditLoggedForUrl = ref(null);
const pendingDirectStartAudit = ref(null);

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

async function safePlay(el) {
  try {
    await el.play();
    return true;
  } catch (error) {
    console.warn('[FloatingPlayer] play failed', error);
    errorMessage.value = 'Unable to start playback. The stream may be invalid or unsupported.';
    isLoading.value = false;
    return false;
  }
}

function getVideoElement() {
  return videoEl.value || document.querySelector('.floating-player__video');
}

function resetStreamDetails() {
  streamDetails.value = {
    resolution: '',
    videoCodec: '',
    audioCodec: '',
    bitrate: null,
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

function normalizeCodecLabel(codec) {
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
    if (!video && (lowered.startsWith('avc') || lowered.startsWith('hev') || lowered.startsWith('hvc') ||
      lowered.startsWith('av01') || lowered.startsWith('vp'))) {
      video = normalizeCodecLabel(part);
    } else if (!audio && (lowered.startsWith('mp4a') || lowered.startsWith('ac-3') || lowered.startsWith('ec-3') ||
      lowered.startsWith('opus') || lowered.startsWith('mp3'))) {
      audio = normalizeCodecLabel(part);
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

function cleanupPlayer() {
  console.info('[FloatingPlayer] cleanupPlayer start');
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
      el.removeEventListener('canplay', videoPlayingHandler.value);
      videoPlayingHandler.value = null;
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
  pendingDirectStartAudit.value = null;
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

function isTicManagedPlaybackUrl(url) {
  const lowered = String(url || '').toLowerCase();
  return (
    lowered.includes('/tic-hls-proxy/') ||
    lowered.includes('/tic-api/tvh_stream/') ||
    lowered.includes('/tic-api/recordings/')
  );
}

async function auditDirectPlaybackStart() {
  const pending = pendingDirectStartAudit.value;
  if (!pending?.url) {
    return;
  }
  if (directStartAuditLoggedForUrl.value === pending.url) {
    return;
  }
  try {
    await axios.post('/tic-api/audit/playback-start', {
      url: pending.url,
      title: pending.title || '',
    });
    directStartAuditLoggedForUrl.value = pending.url;
  } catch (error) {
    console.warn('[FloatingPlayer] direct start audit failed', error);
  } finally {
    pendingDirectStartAudit.value = null;
  }
}

async function initPlayer() {
  const token = ++initToken.value;
  cleanupPlayer();
  errorMessage.value = '';
  const url = videoStore.streamUrl;
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
  el.controls = true;
  el.volume = typeof videoStore.volume === 'number' ? videoStore.volume : 1;
  loadingStartedAt.value = Date.now();
  if (!volumeHandler.value) {
    volumeHandler.value = () => {
      videoStore.setVolume(el.volume);
    };
    el.addEventListener('volumechange', volumeHandler.value);
  }
  if (!metadataHandler.value) {
    metadataHandler.value = () => updateResolutionFromEl(el);
    el.addEventListener('loadedmetadata', metadataHandler.value);
  }
  if (!resizeHandler.value) {
    resizeHandler.value = () => updateResolutionFromEl(el);
    el.addEventListener('resize', resizeHandler.value);
  }
  if (!videoErrorHandler.value) {
    videoErrorHandler.value = () => {
      const mediaError = el.error;
      if (mediaError) {
        console.warn('[FloatingPlayer] media error', mediaError);
      }
      if (mediaError?.code === 1) {
        errorMessage.value = 'Stream loading was aborted.';
      } else if (mediaError?.code === 2) {
        errorMessage.value = 'Network error while loading the stream.';
      } else if (mediaError?.code === 3) {
        errorMessage.value = 'Stream could not be decoded. The format may be unsupported.';
      } else if (mediaError?.code === 4) {
        errorMessage.value = 'Stream is not supported or is invalid.';
      } else {
        errorMessage.value = 'Unable to load stream. Please try again.';
      }
      isLoading.value = false;
    };
    el.addEventListener('error', videoErrorHandler.value);
  }
  if (!videoPlayingHandler.value) {
    videoPlayingHandler.value = () => {
      errorMessage.value = '';
      isLoading.value = false;
      auditDirectPlaybackStart();
      if (loadTimeout.value) {
        clearTimeout(loadTimeout.value);
        loadTimeout.value = null;
      }
    };
    el.addEventListener('playing', videoPlayingHandler.value);
    el.addEventListener('canplay', videoPlayingHandler.value);
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
        errorMessage.value = 'No data received from the stream.';
        isLoading.value = false;
      }
    }, 12000);
  }
  const type = detectStreamType(url, videoStore.streamType);
  const shouldAuditDirectStart = !isTicManagedPlaybackUrl(url);
  pendingDirectStartAudit.value = shouldAuditDirectStart ? {url, title: videoStore.streamTitle || ''} : null;
  if (!shouldAuditDirectStart) {
    directStartAuditLoggedForUrl.value = null;
  }
  isLoading.value = true;
  console.info('[FloatingPlayer] initPlayer', {url, type});

  try {
    if (type === 'hls' && Hls.isSupported()) {
      const hls = new Hls({lowLatencyMode: true});
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
        const started = await safePlay(el);
        if (!started) {
          errorMessage.value = 'Playback blocked by browser. Press play to start.';
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
        console.warn('[FloatingPlayer] HLS error', data);
        const status = data?.response?.code;
        const details = data?.details;
        if (status === 401 || status === 403) {
          errorMessage.value = 'Stream rejected (unauthorized).';
        } else if (status === 404) {
          errorMessage.value = 'Stream not found.';
        } else if (details === Hls.ErrorDetails.MANIFEST_LOAD_ERROR) {
          errorMessage.value = 'Stream manifest failed to load.';
        } else if (details === Hls.ErrorDetails.LEVEL_LOAD_ERROR) {
          errorMessage.value = 'Stream level failed to load.';
        } else if (details === Hls.ErrorDetails.FRAG_LOAD_ERROR) {
          errorMessage.value = 'Stream data failed to load.';
        } else if (data?.fatal) {
          errorMessage.value = 'Unable to load stream. Please try again.';
        }
        if (data?.fatal || details === Hls.ErrorDetails.MANIFEST_LOAD_ERROR) {
          isLoading.value = false;
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
      mpegtsInstance.value = player;
      mpegtsInstances.add(player);
      player.attachMediaElement(el);
      player.load();
      await safePlay(el);
      isLoading.value = false;
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
            normalizeCodecLabel(info.videoCodec),
            normalizeCodecLabel(info.audioCodec),
          );
        }
        if (typeof info?.bitrate === 'number') {
          applyBitrate(info.bitrate);
        }
      });
      player.on?.(mpegts.Events.ERROR, (err) => {
        console.warn('[FloatingPlayer] MPEGTS error', err);
        errorMessage.value = 'Unable to load stream. Please try again.';
        isLoading.value = false;
      });
      return;
    }

    el.src = url;
    await safePlay(el);
    isLoading.value = false;
    snapToAspectRatio();
  } catch (error) {
    console.error('Failed to initialize player:', error);
    errorMessage.value = 'Unable to start playback.';
    isLoading.value = false;
  }
}

function closePlayer() {
  console.info('[FloatingPlayer] closePlayer');
  cleanupPlayer();
  directStartAuditLoggedForUrl.value = null;
  videoStore.hidePlayer();
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
  } catch (error) {
    console.warn('PiP failed:', error);
  }
}

function toggleFullScreen() {
  const el = getVideoElement();
  if (!el) return;
  try {
    if (document.fullscreenElement) {
      document.exitFullscreen();
    } else {
      el.requestFullscreen();
    }
  } catch (error) {
    console.warn('Fullscreen failed:', error);
  }
}

function startDrag(event) {
  if (event?.target?.closest?.('.floating-player__actions')) {
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
  if (event?.target?.closest?.('.floating-player__actions')) {
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

watch(
  () => videoStore.streamUrl,
  () => {
    if (videoStore.isVisible) {
      initPlayer();
    }
  },
);

watch(
  () => videoStore.isVisible,
  (visible) => {
    if (!visible) {
      cleanupPlayer();
      return;
    }
    initPlayer();
  },
);

onBeforeUnmount(() => {
  cleanupPlayer();
});

function handleResizeEvent() {
  isMobile.value = window.innerWidth < 600;
}

onMounted(() => {
  window.addEventListener('resize', handleResizeEvent);
});

onUnmounted(() => {
  window.removeEventListener('resize', handleResizeEvent);
});
</script>

<style scoped>
.floating-player {
  position: fixed;
  right: 24px;
  bottom: 24px;
  background: #0f1115;
  border: 1px solid rgba(255, 255, 255, 0.12);
  border-radius: 6px;
  overflow: visible;
  z-index: 10000;
  display: flex;
  flex-direction: column;
  box-shadow: 0 18px 40px rgba(0, 0, 0, 0.4);
  min-width: 320px;
  min-height: 260px;
}

.floating-player__header {
  cursor: move;
  background: #1d2330;
  color: #f0f2f4;
  padding: 6px 10px;
  display: flex;
  align-items: center;
  justify-content: space-between;
  user-select: none;
}

.floating-player__title {
  display: flex;
  flex-direction: column;
  gap: 2px;
  padding-right: 8px;
  min-width: 0;
}

.floating-player__title-text {
  font-size: 0.85rem;
  font-weight: 600;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.floating-player__meta {
  font-size: 0.72rem;
  color: rgba(240, 242, 244, 0.72);
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.floating-player__actions {
  display: flex;
  gap: 4px;
}

.floating-player__body {
  position: relative;
  flex: 1;
  background: #000;
  overflow: visible;
  min-height: 160px;
}

.floating-player__video {
  width: 100%;
  height: 100%;
  display: block;
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
  display: flex;
  align-items: center;
  justify-content: center;
  background: rgba(0, 0, 0, 0.35);
  z-index: 1;
}

.floating-player__error {
  position: absolute;
  inset: 0;
  display: flex;
  align-items: center;
  justify-content: center;
  padding: 12px;
  text-align: center;
  color: #f5f5f5;
  background: rgba(0, 0, 0, 0.6);
  z-index: 2;
}

.floating-player__resize-handle {
  position: absolute;
  width: 12px;
  height: 12px;
  cursor: nwse-resize;
  background: transparent;
}

.floating-player__resize-handle--br {
  right: 4px;
  bottom: 4px;
  cursor: nwse-resize;
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
  right: 4px;
  top: 4px;
  cursor: nesw-resize;
  border-right: 1px solid rgba(255, 255, 255, 0.7);
  border-top: 1px solid rgba(255, 255, 255, 0.7);
}

.floating-player__resize-handle--tl {
  left: 4px;
  top: 4px;
  cursor: nwse-resize;
  border-left: 1px solid rgba(255, 255, 255, 0.7);
  border-top: 1px solid rgba(255, 255, 255, 0.7);
}

@media (max-width: 600px) {
  .floating-player {
    right: 0;
    left: 0;
    bottom: 0;
    top: auto;
    width: 100% !important;
    border-radius: 12px 12px 0 0;
  }

  .floating-player__resize-handle {
    display: none;
  }
}
</style>
