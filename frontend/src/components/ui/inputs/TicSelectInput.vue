<template>
  <div :class="{'tic-select-input--with-description': !!description}">
    <q-select
      class="tic-select-input-field"
      outlined
      :model-value="modelValue"
      :options="displayOptions"
      :display-value="selectionDisplayValue"
      :input-class="multiple && collapseSelections ? 'tic-select-input-collapsed-value' : ''"
      :label="label"
      :option-label="optionLabel"
      :option-value="optionValue"
      :emit-value="emitValue"
      :map-options="mapOptions"
      :multiple="multiple"
      :use-input="searchable"
      :use-chips="multiple && !collapseSelections"
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
      <template #option="scope">
        <q-item
          v-bind="scope.itemProps"
          :class="{'tic-select-option--selected': scope.selected}"
        >
          <q-item-section>
            <q-item-label>{{ resolveOptionLabel(scope.opt) }}</q-item-label>
            <q-item-label v-if="resolveOptionDescription(scope.opt)" caption>
              {{ resolveOptionDescription(scope.opt) }}
            </q-item-label>
          </q-item-section>
          <q-item-section
            v-if="scope.selected"
            side
            class="tic-select-option-check"
          >
            <q-icon name="check" color="primary" size="18px" />
          </q-item-section>
        </q-item>
      </template>
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
  optionDescription: {
    type: [String, Function],
    default: null,
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
  collapseSelections: {
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

const resolveOptionValue = (option) => {
  if (typeof props.optionValue === 'function') {
    return props.optionValue(option);
  }
  if (typeof option === 'string') {
    return option;
  }
  return option?.[props.optionValue];
};

const findOptionByValue = (value) => {
  return props.options.find((option) => resolveOptionValue(option) === value);
};

const resolveOptionLabel = (option) => {
  if (typeof props.optionLabel === 'function') {
    return props.optionLabel(option);
  }
  if (typeof option === 'string') {
    return option;
  }
  return option?.[props.optionLabel] ?? '';
};

const resolveOptionDescription = (option) => {
  if (!props.optionDescription) {
    return '';
  }
  if (typeof props.optionDescription === 'function') {
    return props.optionDescription(option) || '';
  }
  if (typeof option === 'string') {
    return '';
  }
  return option?.[props.optionDescription] ?? '';
};

const selectedLabels = computed(() => {
  const rawValues = Array.isArray(props.modelValue) ? props.modelValue : [];
  return rawValues.map((value) => {
    const option = findOptionByValue(value);
    if (option !== undefined) {
      return String(resolveOptionLabel(option) || '').trim();
    }
    return String(value || '').trim();
  }).filter(Boolean);
});

const selectionDisplayValue = computed(() => {
  if (!props.multiple || !props.collapseSelections) {
    return undefined;
  }
  const labels = selectedLabels.value;
  if (!labels.length) {
    return '';
  }
  if (labels.length === 1) {
    return labels[0];
  }
  return `${labels.length} selected`;
});

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
  padding-bottom: 0 !important;
}

.tic-select-option--selected {
  background: color-mix(in srgb, var(--q-primary) 10%, transparent);
}

.tic-select-option-check {
  min-width: 24px;
}

.tic-select-input--with-description .tic-select-input-field {
  padding-bottom: 8px !important;
}

.tic-input-description {
  margin-top: 0;
  margin-left: 8px;
}

:deep(.tic-select-input-field .q-field__native),
:deep(.tic-select-input-field .q-field__input) {
  flex-wrap: nowrap !important;
}

:deep(.tic-select-input-field .q-field__native > .ellipsis) {
  flex: 1 1 auto;
  min-width: 0;
}

:deep(.tic-select-input-field .q-field__input.tic-select-input-collapsed-value) {
  flex: 0 0 1px !important;
  width: 1px !important;
  min-width: 1px !important;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

:deep(.tic-select-input-field .q-field__control) {
  min-height: 40px;
}

:deep(.tic-select-input-field .q-field__marginal) {
  height: 40px;
}
</style>
