<template>
  <div
    v-if="videoStore.isVisible"
    class="floating-player"
    :style="playerStyle"
  >
    <div
      class="floating-player__header"
      @mousedown="startDrag"
      @touchstart.prevent="startDragTouch"
    >
      <div class="floating-player__title">
        {{ videoStore.streamTitle || 'Stream Preview' }}
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
        :controls="!isLoading"
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

function getVideoElement() {
  return videoEl.value || document.querySelector('.floating-player__video');
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
    try {
      el.pause();
    } catch (error) {
      console.warn('Failed to pause video:', error);
    }
    el.removeAttribute('src');
    el.load();
  }
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
  return 'native';
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
  const type = detectStreamType(url, videoStore.streamType);
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
        try {
          await el.play();
        } catch (error) {
          errorMessage.value = 'Playback blocked by browser. Press play to start.';
        }
        isLoading.value = false;
      });
      hls.on(Hls.Events.ERROR, (_event, data) => {
        if (data.fatal) {
          errorMessage.value = 'Stream error. Please try again.';
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
      player.play();
      isLoading.value = false;
      return;
    }

    el.src = url;
    await el.play();
    isLoading.value = false;
  } catch (error) {
    console.error('Failed to initialize player:', error);
    errorMessage.value = 'Unable to start playback.';
    isLoading.value = false;
  }
}

function closePlayer() {
  console.info('[FloatingPlayer] closePlayer');
  cleanupPlayer();
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
  dragState.value = {
    startX: event.clientX,
    startY: event.clientY,
    initial: {...videoStore.position},
  };
  window.addEventListener('mousemove', handleDrag);
  window.addEventListener('mouseup', stopDrag);
}

function startDragTouch(event) {
  const touch = event.touches[0];
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
  const minWidth = 240;
  const minHeight = 160;
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
  const minWidth = 240;
  const minHeight = 160;
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

  const right = Math.max(0, initialPos.viewportWidth - left - width);
  const bottom = Math.max(0, initialPos.viewportHeight - top - height);

  videoStore.setSize({width, height});
  videoStore.setPosition({right, bottom});
}

function stopResize() {
  resizeState.value = null;
  window.removeEventListener('mousemove', handleResize);
  window.removeEventListener('mouseup', stopResize);
}

function stopResizeTouch() {
  resizeState.value = null;
  window.removeEventListener('touchmove', handleResizeTouch);
  window.removeEventListener('touchend', stopResizeTouch);
}

watch(
  () => videoStore.streamUrl,
  () => {
    if (videoStore.isVisible) {
      initPlayer();
    }
  }
);

watch(
  () => videoStore.isVisible,
  (visible) => {
    if (!visible) {
      cleanupPlayer();
      return;
    }
    initPlayer();
  }
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
  overflow: hidden;
  z-index: 3000;
  display: flex;
  flex-direction: column;
  box-shadow: 0 18px 40px rgba(0, 0, 0, 0.4);
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
  font-size: 0.85rem;
  font-weight: 600;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
  padding-right: 8px;
}

.floating-player__actions {
  display: flex;
  gap: 4px;
}

.floating-player__body {
  position: relative;
  flex: 1;
  background: #000;
}

.floating-player__video {
  width: 100%;
  height: 100%;
  display: block;
  background: #000;
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
