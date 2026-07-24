<template>
  <q-card
    class="vod-content-card cursor-pointer"
    :class="{ 'vod-content-card--failed': failed }"
    flat
    bordered
    @click="$emit('select')"
  >
    <div class="vod-content-card__poster-wrap">
      <img
        v-if="posterUrl"
        :src="posterUrl"
        :alt="title"
        class="vod-content-card__poster"
      />
      <div v-else class="vod-content-card__poster vod-content-card__poster--placeholder">
        <q-icon :name="contentType === 'movie' ? 'movie' : 'tv'" size="40px" color="grey-6" />
      </div>

      <div v-if="typeLabel" class="vod-content-card__chip vod-content-card__chip--type">
        {{ typeLabel }}
      </div>

      <div v-if="yearLabel" class="vod-content-card__chip vod-content-card__chip--year">
        {{ yearLabel }}
      </div>

      <div v-if="failed" class="vod-content-card__failed-banner">
        <q-icon name="warning" size="14px" class="q-mr-xs" />
        Upstream Issue
      </div>
    </div>

    <q-card-section class="vod-content-card__body">
      <div class="text-weight-medium vod-content-card__title ellipsis-2-lines">{{ title }}</div>
      <div class="text-caption text-grey-7 vod-content-card__meta ellipsis">
        {{ categoryLabel }}
      </div>
      <div v-if="secondaryLabel" class="text-caption text-grey-7 vod-content-card__meta ellipsis">
        {{ secondaryLabel }}
      </div>
      <div v-if="plot" class="text-caption text-grey-8 vod-content-card__plot ellipsis-2-lines">
        {{ plot }}
      </div>
    </q-card-section>
  </q-card>
</template>

<script>
import {defineComponent} from 'vue';

export default defineComponent({
  name: 'TicVodContentCard',
  props: {
    title: {
      type: String,
      default: '',
    },
    posterUrl: {
      type: String,
      default: '',
    },
    plot: {
      type: String,
      default: '',
    },
    categoryLabel: {
      type: String,
      default: '',
    },
    secondaryLabel: {
      type: String,
      default: '',
    },
    typeLabel: {
      type: String,
      default: '',
    },
    yearLabel: {
      type: [String, Number],
      default: '',
    },
    contentType: {
      type: String,
      default: 'movie',
    },
    failed: {
      type: Boolean,
      default: false,
    },
  },
  emits: ['select'],
});
</script>

<style scoped>
.vod-content-card {
  display: flex;
  flex-direction: column;
  background: var(--tic-list-card-default-bg);
  border: var(--tic-elevated-border);
  min-height: 100%;
  border-radius: 0;
  overflow: hidden;
  transition: transform 0.16s ease, border-color 0.16s ease, box-shadow 0.16s ease;
}

.vod-content-card--failed {
  border-color: color-mix(in srgb, var(--q-negative) 40%, var(--q-separator-color));
}

.vod-content-card--failed:hover {
  border-color: var(--q-negative);
}

.vod-content-card__failed-banner {
  position: absolute;
  bottom: 0;
  left: 0;
  right: 0;
  background: color-mix(in srgb, var(--q-negative) 85%, transparent);
  color: white;
  text-align: center;
  font-size: 0.72rem;
  font-weight: 500;
  padding: 4px 8px;
  display: flex;
  align-items: center;
  justify-content: center;
  z-index: 2;
  border-top: 1px solid var(--q-negative);
}

.vod-content-card:hover {
  transform: translateY(-2px);
  border-color: color-mix(in srgb, var(--q-primary) 24%, var(--q-separator-color));
  box-shadow: var(--tic-elevated-shadow);
}

.vod-content-card__poster-wrap {
  aspect-ratio: 2 / 3;
  background: var(--guide-channel-bg);
  overflow: hidden;
  position: relative;
}

.vod-content-card__poster {
  width: 100%;
  height: 100%;
  object-fit: cover;
  display: block;
}

.vod-content-card__poster--placeholder {
  display: flex;
  align-items: center;
  justify-content: center;
  background: var(--guide-channel-bg);
}

.vod-content-card__chip {
  position: absolute;
  top: 0;
  padding: 4px 8px;
  min-height: 26px;
  display: inline-flex;
  align-items: center;
  font-size: 0.7rem;
  line-height: 1;
  letter-spacing: 0.02em;
  background: color-mix(in srgb, var(--guide-channel-bg) 68%, var(--q-primary) 32%);
  color: var(--q-grey-1);
  border-bottom: 1px solid color-mix(in srgb, var(--q-primary) 26%, var(--q-separator-color));
  box-shadow: inset 0 0 0 1px color-mix(in srgb, var(--q-primary) 22%, transparent);
}

.vod-content-card__chip--type {
  left: 0;
  border-right: 1px solid var(--q-separator-color);
}

.vod-content-card__chip--year {
  right: 0;
  border-left: 1px solid var(--q-separator-color);
  color: var(--q-grey-1);
}

.vod-content-card__body {
  display: flex;
  flex-direction: column;
  gap: 7px;
  padding: 12px;
}

.vod-content-card__title {
  line-height: 1.3;
}

.vod-content-card__meta,
.vod-content-card__plot {
  line-height: 1.35;
}

.ellipsis-2-lines {
  display: -webkit-box;
  -webkit-line-clamp: 2;
  -webkit-box-orient: vertical;
  overflow: hidden;
}

@media (max-width: 600px) {
  .vod-content-card__body {
    gap: 6px;
    padding: 8px;
  }

  .vod-content-card__chip {
    min-height: 22px;
    padding: 3px 6px;
    font-size: 0.64rem;
  }

  .vod-content-card__title {
    font-size: 0.86rem;
    line-height: 1.25;
  }

  .vod-content-card__meta,
  .vod-content-card__plot {
    font-size: 0.72rem;
    line-height: 1.3;
  }
}
</style>
