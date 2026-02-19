<template>
  <q-btn-dropdown
    v-bind="$attrs"
    :outline="isOutline"
    :flat="isFlat"
    :color="color"
    content-class="tic-dropdown-menu"
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
import {computed} from 'vue';

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
});

const isOutline = computed(() => props.variant === 'outline');
const isFlat = computed(() => props.variant === 'flat');
</script>

<style scoped>
</style>
