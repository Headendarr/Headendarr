<template>
  <div
    class="section-toolbar"
    :class="{'section-toolbar--sticky bg-card-head': sticky}"
    :style="stickyToolbarStyle"
  >
    <div class="section-toolbar-row">
      <div class="section-toolbar-left">
        <TicButton
          v-for="action in toolbarActions"
          :key="action.key"
          :label="action.label || 'Action'"
          :icon="action.icon || ''"
          :color="action.color || 'primary'"
          class="section-toolbar-btn"
          :class="$q.screen.lt.sm && action.fullWidthOnMobile !== false ? 'full-width' : ''"
          :dense="Boolean(action.dense)"
          :disable="Boolean(action.disable || action.disabled)"
          :tooltip="action.tooltip || ''"
          @click="handleActionClick(action)"
        />

        <div v-if="search" class="section-toolbar-search-wrap">
          <TicSearchInput
            :model-value="searchValue"
            class="section-toolbar-field"
            :label="search.label || 'Search'"
            :placeholder="search.placeholder || ''"
            :debounce="search.debounce ?? 300"
            :clearable="search.clearable !== false"
            @update:model-value="$emit('update:searchValue', $event)"
            @search="$emit('search')"
          />
        </div>

        <slot name="left-extra" />
      </div>

      <div v-if="hasRightControls || $slots['right-extra']" class="section-toolbar-right">
        <template v-if="showInlineFilters">
          <div
            v-for="filter in filters"
            :key="filter.key"
            class="section-toolbar-filter-wrap"
          >
            <TicSelectInput
              :model-value="filter.modelValue"
              class="section-toolbar-field"
              :label="filter.label || 'Filter'"
              :options="filter.options || []"
              :option-label="filter.optionLabel || 'label'"
              :option-value="filter.optionValue || 'value'"
              :emit-value="filter.emitValue !== false"
              :map-options="filter.mapOptions !== false"
              :multiple="Boolean(filter.multiple)"
              :collapse-selections="Boolean(filter.collapseSelections)"
              :clearable="Boolean(filter.clearable)"
              :dense="filter.dense !== false"
              :disable="Boolean(filter.disable)"
              :loading="Boolean(filter.loading)"
              :behavior="filter.behavior || ($q.screen.lt.md ? 'dialog' : 'menu')"
              @update:model-value="$emit('filterChange', {key: filter.key, value: $event})"
            />
          </div>
        </template>
        <div v-else-if="showFilterButton" class="section-toolbar-sort-wrap">
          <TicButton
            :label="filterButtonLabel"
            :icon="filterButtonIcon"
            color="secondary"
            :dense="$q.screen.lt.sm"
            class="section-toolbar-btn section-toolbar-btn--compact"
            @click="$emit('filters')"
          />
        </div>

        <div v-if="sortAction" class="section-toolbar-sort-wrap">
          <TicButton
            :label="$q.screen.lt.md ? (sortAction.mobileLabel || 'Sort') : (sortAction.label || 'Sort')"
            :icon="sortAction.icon || 'sort'"
            :color="sortAction.color || 'secondary'"
            :dense="$q.screen.lt.sm"
            class="section-toolbar-btn section-toolbar-btn--compact"
            @click="$emit('sort')"
          />
        </div>

        <slot name="right-extra" />
      </div>
    </div>

    <div v-if="$slots.below" class="section-toolbar-below">
      <slot name="below" />
    </div>
  </div>
</template>

<script setup>
import {computed, onBeforeUnmount, onMounted, ref, watch} from 'vue';
import {useQuasar} from 'quasar';
import TicButton from '../buttons/TicButton.vue';
import TicSearchInput from '../inputs/TicSearchInput.vue';
import TicSelectInput from '../inputs/TicSelectInput.vue';

const props = defineProps({
  addAction: {
    type: Object,
    default: null,
  },
  actions: {
    type: Array,
    default: () => [],
  },
  search: {
    type: Object,
    default: null,
  },
  searchValue: {
    type: String,
    default: '',
  },
  filters: {
    type: Array,
    default: () => [],
  },
  sortAction: {
    type: Object,
    default: null,
  },
  collapseFiltersOnMobile: {
    type: Boolean,
    default: true,
  },
  filterButtonLabel: {
    type: String,
    default: 'Filters',
  },
  filterButtonIcon: {
    type: String,
    default: 'filter_list',
  },
  sticky: {
    type: Boolean,
    default: false,
  },
  stickyTop: {
    type: Number,
    default: null,
  },
  stickyZIndex: {
    type: Number,
    default: 6,
  },
});

const $q = useQuasar();
const stickyTopOffset = ref(0);

const hasFilters = computed(() => Array.isArray(props.filters) && props.filters.length > 0);
const showInlineFilters = computed(() => hasFilters.value && (!props.collapseFiltersOnMobile || !$q.screen.lt.md));
const showFilterButton = computed(() => hasFilters.value && !showInlineFilters.value);
const hasRightControls = computed(() => hasFilters.value || Boolean(props.sortAction));
const stickyToolbarStyle = computed(() => {
  if (!props.sticky) {
    return undefined;
  }
  return {
    '--tic-toolbar-sticky-top': `${stickyTopOffset.value}px`,
    '--tic-toolbar-sticky-z': String(props.stickyZIndex),
  };
});
const toolbarActions = computed(() => {
  const entries = [];
  if (props.addAction) {
    entries.push({
      ...props.addAction,
      id: props.addAction.id || 'add',
      key: props.addAction.key || props.addAction.id || 'add',
      __addAction: true,
    });
  }
  for (const action of props.actions || []) {
    if (!action) {
      continue;
    }
    entries.push({
      ...action,
      key: action.key || action.id || action.label || `action-${entries.length}`,
    });
  }
  return entries;
});

const emit = defineEmits([
  'add',
  'action',
  'update:searchValue',
  'filterChange',
  'filters',
  'sort',
  'search',
]);

const updateStickyOffset = () => {
  if (!props.sticky) {
    stickyTopOffset.value = 0;
    return;
  }
  if (Number.isFinite(props.stickyTop)) {
    stickyTopOffset.value = props.stickyTop;
    return;
  }
  const headerEl = document.querySelector('header.q-header') || document.querySelector('header');
  stickyTopOffset.value = headerEl ? Math.round(headerEl.getBoundingClientRect().height) : 0;
};

onMounted(() => {
  updateStickyOffset();
  window.addEventListener('resize', updateStickyOffset, {passive: true});
});

onBeforeUnmount(() => {
  window.removeEventListener('resize', updateStickyOffset);
});

watch(
  () => [props.sticky, props.stickyTop],
  () => {
    updateStickyOffset();
  },
);

const handleActionClick = (action) => {
  if (action.__addAction) {
    emit('add');
    return;
  }
  emit('action', action);
};
</script>

<style scoped>
.section-toolbar {
  display: flex;
  flex-direction: column;
  gap: 8px;
  padding: 8px 0;
}

.section-toolbar--sticky {
  position: sticky;
  top: var(--tic-toolbar-sticky-top, 0px);
  z-index: var(--tic-toolbar-sticky-z, 6);
}

.section-toolbar-below {
  width: 100%;
}
</style>
