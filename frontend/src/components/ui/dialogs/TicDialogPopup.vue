<template>
  <q-dialog
    class="tic-dialog-popup"
    backdrop-filter="grayscale(80%) blur(0.7px)"
    :model-value="modelValue"
    :maximized="isMobile && mobileFullscreen"
    :persistent="persistent"
    :transition-show="isMobile ? 'jump-up' : 'scale'"
    :transition-hide="isMobile ? 'jump-down' : 'scale'"
    @update:model-value="$emit('update:modelValue', $event)"
    @show="$emit('show')"
    @hide="$emit('hide')">
    <q-card :style="cardStyle" :class="['dialog-popup-card', {'dialog-card-mobile': isMobile}]">
      <q-card-section class="dialog-sticky-header bg-card-head" :class="{'dialog-header-mobile': isMobile}">
        <div class="row items-center no-wrap">
          <div class="col">
            <div class="text-h6 text-primary">
              <slot name="title">{{ title }}</slot>
            </div>
          </div>
          <div class="col-auto row items-center q-gutter-xs no-wrap">
            <slot name="header-actions" />
            <q-btn
              flat
              dense
              round
              color="grey-7"
              icon="close"
              @click="onCloseClick">
              <q-tooltip>{{ closeTooltip }}</q-tooltip>
            </q-btn>
          </div>
        </div>
      </q-card-section>

      <q-separator />

      <q-card-section class="dialog-content" :class="{'dialog-content-mobile': isMobile}">
        <slot />
      </q-card-section>

      <q-separator v-if="$slots.actions" />

      <q-card-actions
        v-if="$slots.actions"
        align="right"
        class="q-pa-md"
        :class="{'dialog-actions-mobile': isMobile}">
        <slot name="actions" />
      </q-card-actions>
    </q-card>
  </q-dialog>
</template>

<script setup>
import {computed} from 'vue';
import {useQuasar} from 'quasar';

const props = defineProps({
  modelValue: {
    type: Boolean,
    default: false,
  },
  title: {
    type: String,
    default: '',
  },
  width: {
    type: String,
    default: '640px',
  },
  maxWidth: {
    type: String,
    default: '95vw',
  },
  persistent: {
    type: Boolean,
    default: false,
  },
  mobileFullscreen: {
    type: Boolean,
    default: false,
  },
  closeTooltip: {
    type: String,
    default: 'Close',
  },
});

const emit = defineEmits(['update:modelValue', 'show', 'hide', 'close']);
const $q = useQuasar();

const isMobile = computed(() => $q.screen.lt.md);
const cardStyle = computed(() => {
  if (isMobile.value && props.mobileFullscreen) {
    return 'width:100vw;max-width:100vw;';
  }
  return `width:${props.width};max-width:${props.maxWidth};`;
});

const onCloseClick = () => {
  emit('close');
  emit('update:modelValue', false);
};
</script>

<style scoped>
.dialog-sticky-header {
  position: sticky;
  top: 0;
  z-index: 100;
}

.dialog-content {
  max-height: calc(100vh - 170px);
  overflow-y: auto;
}

.dialog-card-mobile {
  width: min(96vw, 640px) !important;
  max-width: min(96vw, 640px) !important;
}

.dialog-popup-card {
  border: var(--tic-elevated-border);
  border-radius: var(--tic-radius-sm);
  box-shadow: var(--tic-elevated-shadow);
}

.dialog-header-mobile {
  padding: 10px 12px;
}

.dialog-content-mobile {
  padding: 10px 12px;
  max-height: calc(100vh - 150px);
}

.dialog-actions-mobile {
  padding: 8px 12px;
}
</style>
