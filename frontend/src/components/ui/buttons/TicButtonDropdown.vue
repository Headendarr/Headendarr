<template>
  <q-btn-dropdown
    v-bind="$attrs"
    outline
    color="secondary"
    content-class="tic-dropdown-menu"
    :icon="icon || undefined"
    :icon-right="iconRight || undefined"
    :label="label"
    :dense="dense"
    :auto-close="autoClose"
  >
    <slot />
  </q-btn-dropdown>
</template>

<script setup>
import {computed} from 'vue';
import {useQuasar} from 'quasar';

const $q = useQuasar();

defineProps({
  label: {
    type: String,
    default: '',
  },
  icon: {
    type: String,
    default: '',
  },
  iconRight: {
    type: String,
    default: '',
  },
  dense: {
    type: Boolean,
    default: false,
  },
  autoClose: {
    type: Boolean,
    default: false,
  },
});
</script>

<style scoped>
</style>
