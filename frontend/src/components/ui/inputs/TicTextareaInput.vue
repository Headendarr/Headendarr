<template>
  <div>
    <q-input
      class="tic-textarea-input-field"
      outlined
      :model-value="modelValue"
      type="textarea"
      :label="label"
      :hint="hint"
      :placeholder="placeholder"
      :dense="dense"
      :autogrow="autogrow"
      :rows="rows"
      :input-style="textareaInputStyle"
      :clearable="clearable"
      :readonly="readonly"
      :disable="disable"
      :loading="loading"
      :maxlength="maxlength"
      :counter="counter"
      :rules="rules"
      @update:model-value="$emit('update:modelValue', $event)"
    />
    <div v-if="description" class="tic-input-description text-caption text-grey-7">
      {{ description }}
    </div>
  </div>
</template>

<script setup>
import {computed} from 'vue';

const props = defineProps({
  modelValue: {
    type: String,
    default: '',
  },
  label: {
    type: String,
    default: '',
  },
  hint: {
    type: String,
    default: '',
  },
  description: {
    type: String,
    default: '',
  },
  placeholder: {
    type: String,
    default: '',
  },
  dense: {
    type: Boolean,
    default: false,
  },
  autogrow: {
    type: Boolean,
    default: true,
  },
  rows: {
    type: [String, Number],
    default: 3,
  },
  minHeight: {
    type: String,
    default: '92px',
  },
  resize: {
    type: String,
    default: 'vertical',
  },
  clearable: {
    type: Boolean,
    default: false,
  },
  readonly: {
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
  maxlength: {
    type: [String, Number],
    default: undefined,
  },
  counter: {
    type: Boolean,
    default: false,
  },
  rules: {
    type: Array,
    default: () => [],
  },
});

const textareaInputStyle = computed(() => {
  return {
    minHeight: props.minHeight,
    resize: props.resize,
  };
});

defineEmits(['update:modelValue']);
</script>

<style scoped>
.tic-textarea-input-field {
  padding-bottom: 8px !important;
}

.tic-input-description {
  margin-top: 0;
  margin-left: 8px;
}
</style>
