<template>
  <div :class="['admonition-banner', `admonition-banner--${type}`]" :style="toneVars">
    <div class="admonition-banner__heading">
      <q-icon :name="iconName" size="20px" />
      <span>{{ displayTitle }}</span>
    </div>
    <div class="admonition-banner__content">
      <slot />
    </div>
  </div>
</template>

<script>
import { defineComponent } from 'vue';

const TYPE_FALLBACK_TITLE = {
  note: 'Note',
  tip: 'Tip',
  warning: 'Warning',
  caution: 'Caution',
  important: 'Important',
};

const TYPE_ICON = {
  note: 'info',
  tip: 'lightbulb',
  warning: 'warning',
  caution: 'error',
  important: 'campaign',
};

const TYPE_COLOR = {
  note: { color: 'var(--q-info)', background: 'rgba(3, 169, 244, 0.10)' },
  tip: { color: 'var(--q-positive)', background: 'rgba(33, 186, 69, 0.10)' },
  warning: { color: 'var(--q-warning)', background: 'rgba(242, 192, 55, 0.16)' },
  caution: { color: 'var(--q-negative)', background: 'rgba(193, 0, 21, 0.10)' },
  important: { color: 'var(--q-accent)', background: 'rgba(156, 39, 176, 0.10)' },
};

export default defineComponent({
  name: 'AdmonitionBanner',
  props: {
    type: {
      type: String,
      default: 'note',
      validator: (value) => ['note', 'tip', 'warning', 'caution', 'important'].includes(value),
    },
    title: {
      type: String,
      default: null,
    },
  },
  computed: {
    displayTitle() {
      return this.title || TYPE_FALLBACK_TITLE[this.type] || TYPE_FALLBACK_TITLE.note;
    },
    iconName() {
      return TYPE_ICON[this.type] || TYPE_ICON.note;
    },
    toneVars() {
      const tone = TYPE_COLOR[this.type] || TYPE_COLOR.note;
      return {
        '--admonition-color': tone.color,
        '--admonition-bg': tone.background,
      };
    },
  },
});
</script>

<style scoped>
.admonition-banner {
  border-left: 6px solid var(--admonition-color);
  border-radius: var(--tic-radius-md);
  background: var(--admonition-bg);
  padding: 10px 12px;
}

.admonition-banner__heading {
  display: flex;
  align-items: center;
  gap: 8px;
  color: var(--admonition-color);
  font-size: 0.95rem;
  font-weight: 700;
  letter-spacing: 0.02em;
  text-transform: uppercase;
  line-height: 1.2;
  margin-bottom: 6px;
}

.admonition-banner__content {
  color: var(--admonition-color);
  line-height: 1.4;
}

.admonition-banner__content :deep(:last-child) {
  margin-bottom: 0;
}
</style>
