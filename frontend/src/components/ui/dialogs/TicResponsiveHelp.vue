<template>
  <div v-if="!$q.screen.lt.md && modelValue" class="col-sm-5 col-md-4 help-panel">
    <q-slide-transition>
      <q-card class="tic-help-card q-my-md">
        <slot />
      </q-card>
    </q-slide-transition>
  </div>

  <div
    v-else-if="$q.screen.lt.md && modelValue"
    class="tic-help-floating"
    :style="floatingStyle"
  >
    <q-card class="tic-help-card tic-help-card--floating">
      <q-card-section
        class="tic-help-header q-px-sm q-py-xs"
        @mousedown="startDragMouse"
        @touchstart="startDragTouch"
      >
        <div class="row items-start no-wrap">
          <q-icon name="drag_indicator" size="18px" class="q-mr-xs q-mt-xs" />
          <div class="tic-help-header__text">
            <div class="text-subtitle2 text-weight-medium">Setup Help</div>
            <div class="text-caption tic-help-header__hint">
              Drag this header to move. Resize from the bottom-right corner.
            </div>
          </div>
          <q-space />
          <q-btn flat dense round icon="close" size="sm" @click="closeHelp" />
        </div>
      </q-card-section>
      <q-separator />
      <q-card-section class="tic-help-floating-content">
        <slot />
      </q-card-section>
      <div
        class="tic-help-resize-handle"
        @mousedown.stop="startResizeMouse"
        @touchstart.prevent.stop="startResizeTouch"
      />
    </q-card>
  </div>
</template>

<script setup>
import {computed, onBeforeUnmount, ref, watch} from 'vue';
import {useQuasar} from 'quasar';
import {useUiStore} from 'stores/ui';

const props = defineProps({
  modelValue: {
    type: Boolean,
    default: false,
  },
});

const emit = defineEmits(['update:modelValue']);

const $q = useQuasar();
const uiStore = useUiStore();
const position = ref({left: 0, top: 0});
const dragState = ref(null);
const resizeState = ref(null);
const size = ref({width: 0, height: 0});

const defaultPanelWidth = computed(() => {
  if (!$q.screen.lt.sm) {
    return Math.min(460, Math.max(340, window.innerWidth - 24));
  }
  return Math.min(360, Math.max(260, window.innerWidth - 16));
});

const panelWidth = computed(() => size.value.width || defaultPanelWidth.value);
const panelHeight = computed(
  () => size.value.height || Math.min(560, Math.max(260, Math.round(window.innerHeight * 0.6))));

const floatingStyle = computed(() => ({
  width: `${panelWidth.value}px`,
  height: `${panelHeight.value}px`,
  left: `${position.value.left}px`,
  top: `${position.value.top}px`,
}));

function clampPosition(left, top) {
  const minLeft = 8;
  const minTop = 56;
  const maxLeft = Math.max(minLeft, window.innerWidth - panelWidth.value - 8);
  const maxTop = Math.max(minTop, window.innerHeight - 200);
  return {
    left: Math.min(Math.max(left, minLeft), maxLeft),
    top: Math.min(Math.max(top, minTop), maxTop),
  };
}

function resetPosition() {
  const helpBtn = document.getElementById('header-help-toggle');
  if (helpBtn) {
    const rect = helpBtn.getBoundingClientRect();
    const preferredLeft = rect.right - panelWidth.value;
    const preferredTop = rect.bottom + 8;
    position.value = clampPosition(preferredLeft, preferredTop);
    return;
  }
  position.value = clampPosition(window.innerWidth - panelWidth.value - 12, 82);
}

function startDrag(pointerX, pointerY) {
  stopResize();
  dragState.value = {
    offsetX: pointerX - position.value.left,
    offsetY: pointerY - position.value.top,
  };
  window.addEventListener('mousemove', onDragMouseMove);
  window.addEventListener('mouseup', stopDrag);
  window.addEventListener('touchmove', onDragTouchMove, {passive: false});
  window.addEventListener('touchend', stopDrag);
}

function startDragMouse(event) {
  startDrag(event.clientX, event.clientY);
}

function startDragTouch(event) {
  if (!event.touches || !event.touches.length) {
    return;
  }
  const touch = event.touches[0];
  startDrag(touch.clientX, touch.clientY);
}

function onDragMouseMove(event) {
  if (!dragState.value) {
    return;
  }
  position.value = clampPosition(
    event.clientX - dragState.value.offsetX,
    event.clientY - dragState.value.offsetY,
  );
}

function onDragTouchMove(event) {
  if (!dragState.value || !event.touches || !event.touches.length) {
    return;
  }
  event.preventDefault();
  const touch = event.touches[0];
  position.value = clampPosition(
    touch.clientX - dragState.value.offsetX,
    touch.clientY - dragState.value.offsetY,
  );
}

function clampWidth(width, left) {
  const minWidth = 260;
  const maxWidth = Math.max(minWidth, Math.min(680, window.innerWidth - left - 8));
  return Math.min(Math.max(width, minWidth), maxWidth);
}

function clampHeight(height, top) {
  const minHeight = 180;
  const maxHeight = Math.max(minHeight, window.innerHeight - top - 8);
  return Math.min(Math.max(height, minHeight), maxHeight);
}

function startResize(pointerX, pointerY) {
  stopDrag();
  resizeState.value = {
    startX: pointerX,
    startY: pointerY,
    startWidth: panelWidth.value,
    startHeight: panelHeight.value,
  };
  window.addEventListener('mousemove', onResizeMouseMove);
  window.addEventListener('mouseup', stopResize);
  window.addEventListener('touchmove', onResizeTouchMove, {passive: false});
  window.addEventListener('touchend', stopResize);
}

function startResizeMouse(event) {
  startResize(event.clientX, event.clientY);
}

function startResizeTouch(event) {
  if (!event.touches || !event.touches.length) {
    return;
  }
  const touch = event.touches[0];
  startResize(touch.clientX, touch.clientY);
}

function onResizeMouseMove(event) {
  if (!resizeState.value) {
    return;
  }
  const width = resizeState.value.startWidth + (event.clientX - resizeState.value.startX);
  const height = resizeState.value.startHeight + (event.clientY - resizeState.value.startY);
  size.value = {
    width: clampWidth(width, position.value.left),
    height: clampHeight(height, position.value.top),
  };
}

function onResizeTouchMove(event) {
  if (!resizeState.value || !event.touches || !event.touches.length) {
    return;
  }
  event.preventDefault();
  const touch = event.touches[0];
  const width = resizeState.value.startWidth + (touch.clientX - resizeState.value.startX);
  const height = resizeState.value.startHeight + (touch.clientY - resizeState.value.startY);
  size.value = {
    width: clampWidth(width, position.value.left),
    height: clampHeight(height, position.value.top),
  };
}

function stopResize() {
  resizeState.value = null;
  window.removeEventListener('mousemove', onResizeMouseMove);
  window.removeEventListener('mouseup', stopResize);
  window.removeEventListener('touchmove', onResizeTouchMove);
  window.removeEventListener('touchend', stopResize);
}

function stopDrag() {
  dragState.value = null;
  window.removeEventListener('mousemove', onDragMouseMove);
  window.removeEventListener('mouseup', stopDrag);
  window.removeEventListener('touchmove', onDragTouchMove);
  window.removeEventListener('touchend', stopDrag);
}

function closeHelp() {
  // Persist hide state the same way as header toggle.
  uiStore.setShowHelp(false);
  emit('update:modelValue', false);
}

watch(
  () => props.modelValue,
  (visible) => {
    if (visible && $q.screen.lt.md) {
      resetPosition();
      if (!size.value.width || !size.value.height) {
        size.value = {
          width: defaultPanelWidth.value,
          height: Math.min(560, Math.max(260, Math.round(window.innerHeight * 0.6))),
        };
      }
    }
  },
);

onBeforeUnmount(() => {
  stopDrag();
  stopResize();
});
</script>

<style scoped>
.tic-help-card {
  border-radius: 8px;
  border: 1px solid var(--tic-help-border);
  border-left: 4px solid var(--tic-help-accent);
  background: var(--tic-help-bg);
  color: var(--tic-help-text);
  box-shadow: var(--tic-dropdown-shadow);
}

.tic-help-floating {
  position: fixed;
  z-index: 2900;
  max-height: calc(100vh - 72px);
}

.tic-help-card--floating {
  height: 100%;
  overflow: hidden;
}

.tic-help-header {
  cursor: move;
  user-select: none;
}

.tic-help-header__text {
  min-width: 0;
}

.tic-help-header__hint {
  opacity: 0.85;
}

.tic-help-floating-content {
  height: calc(100% - 44px);
  overflow-y: auto;
  padding: 10px 12px 12px;
}

.tic-help-resize-handle {
  position: absolute;
  right: 0;
  bottom: 0;
  width: 16px;
  height: 16px;
  cursor: nwse-resize;
}

.tic-help-resize-handle::before {
  content: '';
  position: absolute;
  right: 4px;
  bottom: 4px;
  width: 7px;
  height: 7px;
  border-right: 2px solid var(--tic-help-accent);
  border-bottom: 2px solid var(--tic-help-accent);
}

@media (max-width: 599px) {
  .tic-help-floating-content {
    padding: 8px 10px 10px;
    font-size: 0.92rem;
  }
}
</style>
