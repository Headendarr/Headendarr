<template>
  <div
    class="tic-channel-icon"
    :class="{
      'tic-channel-icon--clickable': clickable,
      'tic-channel-icon--overlay-active': showOverlay && overlayActive,
    }"
    :style="iconStyle"
    @click="onClick"
  >
    <img
      v-if="src"
      class="tic-channel-icon__image"
      :src="src"
      alt=""
    />
    <q-icon
      v-else
      :name="fallbackIcon"
      size="20px"
      class="tic-channel-icon__fallback"
    />

    <div v-if="showOverlay" class="tic-channel-icon__overlay">
      <q-icon :name="overlayIcon" size="22px" />
    </div>
  </div>
</template>

<script setup>
import {computed} from 'vue';

const props = defineProps({
  src: {
    type: String,
    default: '',
  },
  size: {
    type: [Number, String],
    default: 46,
  },
  clickable: {
    type: Boolean,
    default: false,
  },
  showOverlay: {
    type: Boolean,
    default: false,
  },
  overlayActive: {
    type: Boolean,
    default: false,
  },
  overlayIcon: {
    type: String,
    default: 'play_arrow',
  },
  fallbackIcon: {
    type: String,
    default: 'play_arrow',
  },
});

const emit = defineEmits(['click']);

const iconStyle = computed(() => {
  const sizeValue = typeof props.size === 'number' ? `${props.size}px` : `${props.size}`;
  return {
    '--tic-channel-icon-size': sizeValue,
  };
});

const onClick = (event) => {
  if (!props.clickable) {
    return;
  }
  emit('click', event);
};
</script>

<style scoped>
.tic-channel-icon {
  position: relative;
  width: var(--tic-channel-icon-size, 46px);
  height: var(--tic-channel-icon-size, 46px);
  border-radius: 6px;
  background: var(--guide-logo-bg, #fff);
  border: 1px solid var(--guide-logo-border, #e0e0e0);
  display: flex;
  align-items: center;
  justify-content: center;
  overflow: hidden;
  padding: 1px;
  box-sizing: border-box;
  flex: 0 0 auto;
}

.tic-channel-icon--clickable {
  cursor: pointer;
}

.tic-channel-icon__image {
  width: 100%;
  height: 100%;
  max-width: 100%;
  max-height: 100%;
  object-fit: contain;
  display: block;
}

.tic-channel-icon__fallback {
  color: var(--guide-play-icon, var(--q-primary));
}

.tic-channel-icon__overlay {
  position: absolute;
  inset: 0;
  display: flex;
  align-items: center;
  justify-content: center;
  background: var(--guide-play-overlay, rgba(25, 118, 210, 0.08));
  opacity: 0;
  transition: opacity 0.2s ease;
}

.tic-channel-icon--overlay-active .tic-channel-icon__overlay {
  opacity: 1;
}
</style>
