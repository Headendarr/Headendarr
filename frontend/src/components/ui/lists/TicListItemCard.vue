<template>
  <div class="tic-list-item-card">
    <div class="tic-list-item-card__header row items-start justify-between no-wrap">
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
});
</script>

<style scoped>
.tic-list-item-card {
  background: rgba(127, 127, 127, 0.08);
  padding: 0;
}

.tic-list-item-card__header {
  margin: 0;
  padding: 8px;
  background: rgba(127, 127, 127, 0.12);
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
