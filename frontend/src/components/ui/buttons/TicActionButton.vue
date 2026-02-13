<template>
  <q-btn
    flat
    dense
    round
    size="12px"
    :icon="icon"
    :color="color"
    :disable="disable"
    :loading="loading"
    @click="$emit('click', $event)">
    <q-tooltip v-if="tooltip">{{ tooltip }}</q-tooltip>
  </q-btn>
</template>

<script setup>
defineProps({
  icon: {
    type: String,
    required: true,
  },
  color: {
    type: String,
    default: 'grey-8',
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
</script>
