<template>
  <q-input
    outlined
    :model-value="modelValue"
    :label="label"
    :placeholder="placeholder"
    :debounce="debounce"
    :dense="dense"
    :color="color"
    :clearable="clearable"
    :disable="disable"
    :loading="loading"
    @update:model-value="$emit('update:modelValue', $event)"
    @clear="$emit('clear')">
    <template #prepend>
      <q-icon name="search" />
    </template>
  </q-input>
</template>

<script setup>
defineProps({
  modelValue: {
    type: String,
    default: '',
  },
  label: {
    type: String,
    default: 'Search',
  },
  placeholder: {
    type: String,
    default: '',
  },
  debounce: {
    type: Number,
    default: 200,
  },
  dense: {
    type: Boolean,
    default: true,
  },
  clearable: {
    type: Boolean,
    default: true,
  },
  color: {
    type: String,
    default: 'primary',
  },
  disable: {
    type: Boolean,
    default: false,
  },
  loading: {
    type: Boolean,
    default: false,
  },
});

defineEmits(['update:modelValue', 'clear']);
</script>
