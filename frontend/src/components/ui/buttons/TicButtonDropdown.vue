<template>
  <q-btn-dropdown
    v-bind="forwardedAttrs"
    :outline="isOutline"
    :flat="isFlat"
    :color="color"
    :content-class="mergedContentClass"
    :content-style="mergedContentStyle"
    :icon="icon || undefined"
    :icon-right="iconRight || undefined"
    :label="label"
    :dense="dense"
    :auto-close="autoClose"
  >
    <slot />
  </q-btn-dropdown>
</template>

<script setup>
import {computed, useAttrs} from 'vue';

const attrs = useAttrs();

const props = defineProps({
  label: {
    type: String,
    default: '',
  },
  icon: {
    type: String,
    default: '',
  },
  iconRight: {
    type: String,
    default: '',
  },
  variant: {
    type: String,
    default: 'filled',
    validator: (value) => ['filled', 'outline', 'flat'].includes(value),
  },
  color: {
    type: String,
    default: 'primary',
  },
  dense: {
    type: Boolean,
    default: false,
  },
  autoClose: {
    type: Boolean,
    default: false,
  },
  contentClass: {
    type: [String, Array, Object],
    default: '',
  },
  contentStyle: {
    type: [String, Array, Object],
    default: '',
  },
});

const isOutline = computed(() => props.variant === 'outline');
const isFlat = computed(() => props.variant === 'flat');
const mergedContentClass = computed(() => ['tic-dropdown-menu', attrs['content-class'], attrs.contentClass, props.contentClass]);
const mergedContentStyle = computed(() => [attrs['content-style'], attrs.contentStyle, props.contentStyle]);
const forwardedAttrs = computed(() => {
  const nextAttrs = {...attrs};
  delete nextAttrs['content-class'];
  delete nextAttrs.contentClass;
  delete nextAttrs['content-style'];
  delete nextAttrs.contentStyle;
  return nextAttrs;
});
</script>

<style scoped>
</style>
