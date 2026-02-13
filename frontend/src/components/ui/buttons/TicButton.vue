<template>
  <q-btn
    :label="label"
    :icon="icon"
    :color="color"
    :type="type"
    :size="size"
    :disable="disable"
    :loading="loading"
    :dense="dense"
    :round="round"
    :flat="isFlat"
    :outline="isOutline"
    :unelevated="isFilled"
    @click="$emit('click', $event)">
    <slot />
    <q-tooltip v-if="tooltip">{{ tooltip }}</q-tooltip>
  </q-btn>
</template>

<script setup>
import {computed} from 'vue';

const props = defineProps({
  label: {
    type: String,
    default: undefined,
  },
  icon: {
    type: String,
    default: undefined,
  },
  color: {
    type: String,
    default: 'primary',
  },
  variant: {
    type: String,
    default: 'filled',
    validator: (value) => ['filled', 'outline', 'flat', 'text'].includes(value),
  },
  type: {
    type: String,
    default: 'button',
  },
  size: {
    type: String,
    default: undefined,
  },
  dense: {
    type: Boolean,
    default: false,
  },
  round: {
    type: Boolean,
    default: false,
  },
  disable: {
    type: Boolean,
    default: false,
  },
  loading: {
    type: Boolean,
    default: false,
  },
  tooltip: {
    type: String,
    default: '',
  },
});

defineEmits(['click']);

const isFilled = computed(() => props.variant === 'filled');
const isOutline = computed(() => props.variant === 'outline');
const isFlat = computed(() => props.variant === 'flat' || props.variant === 'text');
</script>
