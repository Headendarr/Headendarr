<template>
  <div>
    <q-input
      class="tic-number-input-field"
      outlined
      :model-value="modelValue"
      type="number"
      :label="label"
      :hint="hint"
      :placeholder="placeholder"
      :dense="dense"
      :clearable="clearable"
      :readonly="readonly"
      :disable="disable"
      :loading="loading"
      :rules="rules"
      :min="min"
      :max="max"
      :step="step"
      @update:model-value="$emit('update:modelValue', $event)"
    />
    <div v-if="description" class="tic-input-description text-caption text-grey-7">
      {{ description }}
    </div>
  </div>
</template>

<script setup>
defineProps({
  modelValue: {
    type: [String, Number],
    default: null,
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
  rules: {
    type: Array,
    default: () => [],
  },
  min: {
    type: [String, Number],
    default: undefined,
  },
  max: {
    type: [String, Number],
    default: undefined,
  },
  step: {
    type: [String, Number],
    default: 1,
  },
});

defineEmits(['update:modelValue']);
</script>

<style scoped>
.tic-number-input-field {
  padding-bottom: 8px !important;
}

.tic-input-description {
  margin-top: 0;
  margin-left: 8px;
}
</style>
