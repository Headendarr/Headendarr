<template>
  <div v-if="!$q.screen.lt.sm" class="row items-center q-gutter-xs">
    <TicActionButton
      v-for="action in actions"
      :key="action.id || action.icon || action.label"
      :icon="action.icon"
      :color="action.color || 'grey-8'"
      :disable="Boolean(action.disable)"
      :loading="Boolean(action.loading)"
      :tooltip="action.tooltip || action.label || ''"
      @click="emitAction(action)"
    />
  </div>

  <q-btn
    v-else
    flat
    dense
    round
    icon="more_vert"
    color="grey-8">
    <q-menu class="tic-dropdown-menu">
      <q-list dense class="tic-list-actions-menu" style="min-width: 260px">
        <q-item
          v-for="action in nonDestructiveActions"
          :key="action.id || action.icon || action.label"
          clickable
          :disable="Boolean(action.disable)"
          v-close-popup
          class="tic-list-actions-item"
          @click="emitAction(action)">
          <q-item-section avatar>
            <q-icon :name="action.icon" :color="action.color || 'grey-8'" />
          </q-item-section>
          <q-item-section>
            {{ action.label }}
          </q-item-section>
        </q-item>

        <q-separator v-if="nonDestructiveActions.length && destructiveActions.length" />

        <q-item
          v-for="action in destructiveActions"
          :key="action.id || action.icon || action.label"
          clickable
          :disable="Boolean(action.disable)"
          v-close-popup
          class="tic-list-actions-item"
          @click="emitAction(action)">
          <q-item-section avatar>
            <q-icon :name="action.icon" :color="action.color || 'grey-8'" />
          </q-item-section>
          <q-item-section>
            {{ action.label }}
          </q-item-section>
        </q-item>
      </q-list>
    </q-menu>
  </q-btn>
</template>

<script setup>
import {computed} from 'vue';
import TicActionButton from './TicActionButton.vue';

const props = defineProps({
  actions: {
    type: Array,
    default: () => [],
  },
});

const emit = defineEmits(['action']);

const isDestructiveAction = (action) => {
  const id = String(action?.id || '').toLowerCase();
  const color = String(action?.color || '').toLowerCase();
  return color === 'negative' || id.includes('delete') || id.includes('remove') || id.includes('destroy');
};

const nonDestructiveActions = computed(
  () => (Array.isArray(props.actions) ? props.actions : []).filter((action) => !isDestructiveAction(action)));
const destructiveActions = computed(
  () => (Array.isArray(props.actions) ? props.actions : []).filter((action) => isDestructiveAction(action)));

const emitAction = (action) => {
  emit('action', action);
};
</script>

<style scoped>
.tic-list-actions-item {
  padding-top: 8px;
  padding-bottom: 8px;
}
</style>
