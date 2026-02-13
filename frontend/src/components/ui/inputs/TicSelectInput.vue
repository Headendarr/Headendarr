<template>
  <div>
    <q-select
      class="tic-select-input-field"
      outlined
      :model-value="modelValue"
      :options="displayOptions"
      :label="label"
      :option-label="optionLabel"
      :option-value="optionValue"
      :emit-value="emitValue"
      :map-options="mapOptions"
      :multiple="multiple"
      :use-input="searchable"
      :use-chips="multiple"
      :hide-dropdown-icon="hideDropdownIcon"
      :input-debounce="inputDebounce"
      :clearable="clearable"
      :dense="dense"
      :disable="disable"
      :loading="loading"
      :behavior="behavior"
      @update:model-value="$emit('update:modelValue', $event)"
      @filter="onFilter"
    >
      <template #no-option>
        <q-item>
          <q-item-section class="text-grey"> No matching options</q-item-section>
        </q-item>
      </template>
    </q-select>
    <div v-if="description" class="tic-input-description text-caption text-grey-7">
      {{ description }}
    </div>
  </div>
</template>

<script setup>
import {computed, ref, watch} from 'vue';

const props = defineProps({
  modelValue: {
    type: [String, Number, Boolean, Object, Array],
    default: null,
  },
  options: {
    type: Array,
    default: () => [],
  },
  label: {
    type: String,
    default: '',
  },
  description: {
    type: String,
    default: '',
  },
  optionLabel: {
    type: [String, Function],
    default: 'label',
  },
  optionValue: {
    type: [String, Function],
    default: 'value',
  },
  emitValue: {
    type: Boolean,
    default: false,
  },
  mapOptions: {
    type: Boolean,
    default: false,
  },
  multiple: {
    type: Boolean,
    default: false,
  },
  searchable: {
    type: Boolean,
    default: true,
  },
  clearable: {
    type: Boolean,
    default: false,
  },
  dense: {
    type: Boolean,
    default: false,
  },
  hideDropdownIcon: {
    type: Boolean,
    default: false,
  },
  inputDebounce: {
    type: Number,
    default: 0,
  },
  disable: {
    type: Boolean,
    default: false,
  },
  loading: {
    type: Boolean,
    default: false,
  },
  behavior: {
    type: String,
    default: 'menu',
  },
});

const emit = defineEmits(['update:modelValue', 'filter']);
const filteredOptions = ref([...props.options]);

watch(
  () => props.options,
  (nextValue) => {
    filteredOptions.value = [...nextValue];
  },
  {deep: true},
);

const displayOptions = computed(() => {
  return props.searchable ? filteredOptions.value : props.options;
});

const resolveOptionLabel = (option) => {
  if (typeof props.optionLabel === 'function') {
    return props.optionLabel(option);
  }
  if (typeof option === 'string') {
    return option;
  }
  return option?.[props.optionLabel] ?? '';
};

const onFilter = (value, update) => {
  emit('filter', value, update);
  if (!props.searchable) {
    return;
  }

  update(() => {
    const needle = String(value || '').trim().toLowerCase();
    if (!needle) {
      filteredOptions.value = [...props.options];
      return;
    }

    filteredOptions.value = props.options.filter((option) => {
      return String(resolveOptionLabel(option)).toLowerCase().includes(needle);
    });
  });
};
</script>

<style scoped>
.tic-select-input-field {
  padding-bottom: 8px !important;
}

.tic-input-description {
  margin-top: 0;
  margin-left: 8px;
}
</style>
