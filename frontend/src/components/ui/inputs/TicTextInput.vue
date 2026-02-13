<template>
  <div>
    <q-input
      class="tic-text-input-field"
      outlined
      :model-value="modelValue"
      :label="label"
      :type="type"
      :hint="hint"
      :placeholder="placeholder"
      :dense="dense"
      :clearable="clearable"
      :readonly="readonly"
      :disable="disable"
      :loading="loading"
      :maxlength="maxlength"
      :counter="counter"
      :autofocus="autofocus"
      :autocomplete="autocomplete"
      :rules="rules"
      :debounce="debounce"
      @update:model-value="$emit('update:modelValue', $event)"
    >
      <template v-if="$slots.prepend" #prepend>
        <slot name="prepend" />
      </template>
      <template v-if="$slots.append" #append>
        <slot name="append" />
      </template>
    </q-input>
    <div v-if="description" class="tic-input-description text-caption text-grey-7">
      {{ description }}
    </div>
  </div>
</template>

<script setup>
defineProps({
  modelValue: {
    type: [String, Number],
    default: '',
  },
  label: {
    type: String,
    default: '',
  },
  type: {
    type: String,
    default: 'text',
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
  maxlength: {
    type: [String, Number],
    default: undefined,
  },
  counter: {
    type: Boolean,
    default: false,
  },
  autofocus: {
    type: Boolean,
    default: false,
  },
  autocomplete: {
    type: String,
    default: undefined,
  },
  rules: {
    type: Array,
    default: () => [],
  },
  debounce: {
    type: Number,
    default: 0,
  },
});

defineEmits(['update:modelValue']);
</script>

<style scoped>
.tic-text-input-field {
  padding-bottom: 8px !important;
}

.tic-input-description {
  margin-top: 0;
  margin-left: 8px;
}
</style>
