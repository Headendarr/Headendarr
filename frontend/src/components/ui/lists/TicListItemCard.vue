<template>
  <div class="tic-list-item-card" :style="cardStyle">
    <div
      v-if="!hideHeader"
      class="tic-list-item-card__header row items-start justify-between no-wrap"
    >
      <div class="tic-list-item-card__header-left">
        <slot name="header-left" />
      </div>
      <div class="tic-list-item-card__header-right row items-center no-wrap q-gutter-xs">
        <slot name="header-actions" />
      </div>
    </div>
    <div class="tic-list-item-card__body">
      <slot />
    </div>
  </div>
</template>

<script>
import {defineComponent} from 'vue';

export default defineComponent({
  name: 'TicListItemCard',
  props: {
    accentColor: {
      type: String,
      default: '',
    },
    surfaceColor: {
      type: String,
      default: '',
    },
    headerColor: {
      type: String,
      default: '',
    },
    textColor: {
      type: String,
      default: '',
    },
    hideHeader: {
      type: Boolean,
      default: false,
    },
  },
  computed: {
    cardStyle() {
      const style = {};
      if (this.accentColor) {
        style['--tic-list-card-accent'] = this.accentColor;
      }
      if (this.surfaceColor) {
        style['--tic-list-card-surface'] = this.surfaceColor;
      }
      if (this.headerColor) {
        style['--tic-list-card-header'] = this.headerColor;
      }
      if (this.textColor) {
        style['--tic-list-card-text'] = this.textColor;
      }
      return style;
    },
  },
});
</script>

<style scoped>
.tic-list-item-card {
  background: var(--tic-list-card-surface, var(--tic-list-card-default-bg));
  padding: 0;
  border-radius: var(--tic-radius-md);
  overflow: hidden;
  border-left: 4px solid var(--tic-list-card-accent, transparent);
  color: var(--tic-list-card-text, inherit);
}

.tic-list-item-card__header {
  margin: 0;
  padding: 8px;
  background: var(--tic-list-card-header, var(--tic-list-card-default-header-bg));
  border-bottom: 1px solid var(--q-separator-color);
}

.tic-list-item-card__header-left {
  min-width: 0;
  flex: 1;
}

.tic-list-item-card__header-right {
  flex-shrink: 0;
}

.tic-list-item-card__body {
  margin: 8px;
}

@media (max-width: 599px) {
  .tic-list-item-card__body {
    margin: 8px 8px 10px;
  }
}
</style>
