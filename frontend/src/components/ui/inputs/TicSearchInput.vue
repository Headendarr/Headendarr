<template>
  <q-input
    ref="inputRef"
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
    @keydown.enter="onEnter"
    @clear="$emit('clear')">
    <template #prepend>
      <q-icon name="search" />
    </template>
  </q-input>
</template>

<script setup>
import {ref} from 'vue';

const props = defineProps({
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

const emit = defineEmits(['update:modelValue', 'clear', 'search']);

const inputRef = ref(null);

const onEnter = () => {
  const inputEl = inputRef.value?.$el?.querySelector('input');
  if (inputEl) {
    emit('update:modelValue', inputEl.value);
  }
  emit('search');
};
</script>
