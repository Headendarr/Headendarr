<template>
  <div>
    <q-field
      class="tic-toggle-input-field"
      :model-value="modelValue"
      :disable="disable"
      outlined
      hide-bottom-space
    >
      <template #control>
        <div class="tic-toggle-control-row row items-center no-wrap full-width" @click="onFieldClick">
          <div v-if="label" class="q-field__label tic-toggle-label">
            {{ label }}
          </div>
          <q-space />
          <q-toggle
            class="tic-toggle-control"
            :model-value="modelValue"
            :disable="disable"
            :color="color"
            @click.stop
            @update:model-value="emit('update:modelValue', $event)"
          />
        </div>
      </template>
    </q-field>

    <div v-if="description" class="tic-input-description text-caption text-grey-7">
      {{ description }}
    </div>
  </div>
</template>

<script setup>
const props = defineProps({
  modelValue: {
    type: Boolean,
    default: false,
  },
  label: {
    type: String,
    default: '',
  },
  description: {
    type: String,
    default: '',
  },
  color: {
    type: String,
    default: 'primary',
  },
  disable: {
    type: Boolean,
    default: false,
  },
});

const emit = defineEmits(['update:modelValue']);

const onFieldClick = () => {
  if (props.disable) {
    return;
  }
  emit('update:modelValue', !props.modelValue);
};
</script>

<style scoped>
.tic-toggle-input-field {
  padding-bottom: 8px !important;
}

.tic-toggle-control-row {
  min-height: 40px;
  cursor: pointer;
}

.tic-toggle-label {
  position: static;
  transform: none;
}

.tic-toggle-control {
  margin-right: -8px;
}

.tic-input-description {
  margin-top: 0;
  margin-left: 8px;
}
</style>
