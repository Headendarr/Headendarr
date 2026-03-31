<template>
  <TicDialogWindow
    v-model="isOpen"
    :title="dialogTitle"
    width="960px"
    close-tooltip="Close"
    @hide="$emit('hide')"
  >
    <div class="vod-content-details">
      <q-inner-loading :showing="loading">
        <q-spinner-dots size="42px" color="primary" />
      </q-inner-loading>

      <template v-if="detailItem && !loading">
        <div class="row q-col-gutter-lg items-start vod-content-details__top">
          <div class="col-auto vod-content-details__poster-col">
            <div class="vod-content-details__poster-wrap">
              <img
                v-if="detailItem.poster_url"
                :src="detailItem.poster_url"
                :alt="detailItem.title"
                class="vod-content-details__poster"
              />
              <div v-else class="vod-content-details__poster vod-content-details__poster--placeholder">
                <q-icon :name="activeTab === 'movie' ? 'movie' : 'tv'" size="56px" color="grey-6" />
              </div>
            </div>
          </div>

          <div class="col vod-content-details__summary">
            <div class="vod-content-details__title-row">
              <div class="text-h6 text-weight-medium vod-content-details__title">{{ displayTitle }}</div>
            </div>

            <div v-if="displayMetaItems.length" class="vod-content-details__meta-stack q-mt-sm">
              <div
                v-for="item in displayMetaItems"
                :key="`${item.label}-${item.value}`"
                class="vod-content-details__meta-item"
              >
                <span class="vod-content-details__meta-label">{{ item.label }}</span>
                <span class="vod-content-details__meta-value">{{ item.value }}</span>
              </div>
            </div>

            <div v-if="showInlinePlot" class="vod-content-details__inline-plot q-mt-md">
              <div class="vod-content-details__plot-divider" />
              <div class="vod-content-details__plot vod-content-details__plot--plain q-mt-md">
                {{ detailItem.plot }}
              </div>
            </div>

            <div v-if="showInlineActions" class="vod-content-details__inline-actions">
              <template v-if="useCompactInlineActions">
                <TicActionButton
                  v-for="action in detailActions"
                  :key="action.id"
                  :icon="action.icon"
                  :color="action.color || 'primary'"
                  :tooltip="action.label || ''"
                  @click="$emit('detail-action', action)"
                />
              </template>
              <template v-else>
                <TicButton
                  v-for="action in detailActions"
                  :key="action.id"
                  :label="action.label"
                  :icon="action.icon"
                  :color="action.color || 'primary'"
                  :variant="action.color === 'primary' ? 'filled' : 'outline'"
                  @click="$emit('detail-action', action)"
                />
              </template>
            </div>

            <div v-if="showFooterActions" class="vod-content-details__footer-actions">
              <TicButton
                v-for="action in detailActions"
                :key="action.id"
                :label="action.label"
                :icon="action.icon"
                :color="action.color || 'primary'"
                :variant="action.color === 'primary' ? 'filled' : 'outline'"
                class="vod-content-details__footer-button"
                @click="$emit('detail-action', action)"
              />
            </div>
          </div>
        </div>

        <div class="vod-content-details__lower q-mt-xs">
          <div v-if="castLine" class="vod-content-details__pills">
            <div v-if="castLine" class="vod-content-details__pill">
              {{ castLine }}
            </div>
          </div>

          <div v-if="showLowerPlot" class="vod-content-details__plot q-mt-xs">
            {{ detailItem.plot }}
          </div>
        </div>

        <div v-if="activeTab === 'series' && seasonOptions.length" class="q-mt-sm">
          <div class="vod-content-details__season-tabs">
            <q-tabs
              :model-value="selectedSeason"
              align="left"
              narrow-indicator
              inline-label
              outside-arrows
              mobile-arrows
              class="vod-content-details__season-tabs-control text-primary"
              active-color="primary"
              indicator-color="primary"
              @update:model-value="$emit('update:selected-season', $event)"
            >
              <q-tab
                v-for="season in seasonOptions"
                :key="season.value"
                :name="season.value"
                :label="season.label"
              />
            </q-tabs>
          </div>

          <q-list bordered separator class="vod-content-details__episode-list">
            <q-item
              v-for="episode in visibleEpisodes"
              :key="`${detailItem.id}-${episode.id || `${episode.season_number}-${episode.episode_number}-${episode.title}`}`"
              class="vod-content-details__episode-item"
            >
              <q-item-section>
                <q-item-label class="text-weight-medium">
                  {{ episodeLabel(episode) }}
                </q-item-label>
                <q-item-label caption lines="2">
                  {{ episode.plot || 'No episode description available.' }}
                </q-item-label>
              </q-item-section>

              <q-item-section v-if="episodeActions(episode).length" side top>
                <TicListActions
                  :actions="episodeActions(episode)"
                  @action="$emit('episode-action', {action: $event, episode})"
                />
              </q-item-section>
            </q-item>
          </q-list>
        </div>
      </template>
    </div>
  </TicDialogWindow>
</template>

<script>
import {defineComponent} from 'vue';
import {TicActionButton, TicButton, TicDialogWindow, TicListActions} from 'components/ui';

export default defineComponent({
  name: 'VodContentDetails',
  components: {
    TicActionButton,
    TicButton,
    TicDialogWindow,
    TicListActions,
  },
  props: {
    modelValue: {
      type: Boolean,
      default: false,
    },
    dialogTitle: {
      type: String,
      default: 'Details',
    },
    loading: {
      type: Boolean,
      default: false,
    },
    detailItem: {
      type: Object,
      default: null,
    },
    activeTab: {
      type: String,
      default: 'movie',
    },
    displayTitle: {
      type: String,
      default: '',
    },
    metaItems: {
      type: Array,
      default: () => [],
    },
    genreLine: {
      type: String,
      default: '',
    },
    castLine: {
      type: String,
      default: '',
    },
    detailActions: {
      type: Array,
      default: () => [],
    },
    seasonOptions: {
      type: Array,
      default: () => [],
    },
    selectedSeason: {
      type: String,
      default: 'season-1',
    },
    visibleEpisodes: {
      type: Array,
      default: () => [],
    },
    episodeLabel: {
      type: Function,
      required: true,
    },
    episodeActions: {
      type: Function,
      required: true,
    },
  },
  emits: ['update:modelValue', 'hide', 'detail-action', 'episode-action', 'update:selected-season'],
  computed: {
    isDesktopWide() {
      return this.$q.screen.width > 1024;
    },
    showFooterActions() {
      return this.isDesktopWide && this.detailActions.length > 0;
    },
    showInlineActions() {
      return !this.isDesktopWide && this.detailActions.length > 0;
    },
    showInlinePlot() {
      return this.$q.screen.width > 600 && !!this.detailItem?.plot;
    },
    showLowerPlot() {
      return this.$q.screen.width <= 600 && !!this.detailItem?.plot;
    },
    useCompactInlineActions() {
      return this.$q.screen.width < 600;
    },
    displayMetaItems() {
      const items = Array.isArray(this.metaItems) ? [...this.metaItems] : [];
      if (this.genreLine) {
        items.push({
          label: 'Genres',
          value: this.genreLine.replace(/^Genres:\s*/i, ''),
        });
      }
      return items;
    },
    isOpen: {
      get() {
        return this.modelValue;
      },
      set(value) {
        this.$emit('update:modelValue', value);
      },
    },
  },
});
</script>

<style scoped>
.vod-content-details {
  position: relative;
  min-height: 240px;
  padding: 12px 16px 18px;
}

.vod-content-details__top {
  border-bottom: 1px solid var(--q-separator-color);
  align-items: stretch;
}

.vod-content-details__poster-col {
  flex: 0 0 auto;
}

.vod-content-details__poster-wrap {
  aspect-ratio: 2 / 3;
  width: 260px;
  background: var(--guide-channel-bg);
  overflow: hidden;
  border: var(--tic-elevated-border);
}

.vod-content-details__poster {
  width: 100%;
  height: 100%;
  object-fit: cover;
  display: block;
}

.vod-content-details__poster--placeholder {
  display: flex;
  align-items: center;
  justify-content: center;
}

.vod-content-details__pills {
  display: flex;
  flex-wrap: wrap;
  gap: 8px;
}

.vod-content-details__pill {
  padding: 8px 10px;
  border-radius: var(--tic-radius-md);
  background: var(--guide-channel-bg);
  color: var(--q-grey-8);
  font-size: 0.78rem;
  line-height: 1.4;
  border: var(--tic-elevated-border);
}

.vod-content-details__summary {
  display: flex;
  flex-direction: column;
  min-width: 0;
}

.vod-content-details__title-row {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 12px;
}

.vod-content-details__title {
  min-width: 0;
}

.vod-content-details__inline-actions {
  display: flex;
  flex-shrink: 0;
  gap: 8px;
  margin-top: auto;
  padding-top: 12px;
  flex-wrap: wrap;
}

.vod-content-details__meta-stack {
  display: flex;
  flex-direction: column;
  gap: 6px;
}

.vod-content-details__meta-item {
  display: flex;
  gap: 8px;
  line-height: 1.35;
}

.vod-content-details__meta-label {
  min-width: 66px;
  color: var(--q-grey-7);
  font-weight: 600;
}

.vod-content-details__meta-value {
  min-width: 0;
}

.vod-content-details__plot {
  white-space: pre-line;
  line-height: 1.5;
  padding: 12px 14px;
  border-radius: var(--tic-radius-md);
  background: var(--tic-list-card-default-bg);
  border: var(--tic-elevated-border);
}

.vod-content-details__plot-divider {
  display: block;
  width: 100%;
  height: 1px;
  border-top: 1px solid var(--q-separator-color);
}

.vod-content-details__plot--plain {
  padding: 0;
  border: 0;
  border-radius: 0;
  background: transparent;
}

.vod-content-details__season-tabs {
  margin-bottom: 10px;
  border-bottom: 1px solid var(--q-separator-color);
}

.vod-content-details__season-tabs-control {
  min-height: 40px;
}

.vod-content-details__episode-list {
  border-radius: 0;
  overflow: hidden;
  border: var(--tic-elevated-border);
}

.vod-content-details__episode-item {
  min-height: 76px;
}

.vod-content-details__footer-actions {
  display: grid;
  grid-template-columns: repeat(2, minmax(0, 1fr));
  gap: 12px;
  margin-top: auto;
  padding-top: 18px;
}

.vod-content-details__footer-button {
  width: 100%;
}

@media (max-width: 1024px) {
  .vod-content-details {
    padding: 12px 12px 16px;
  }

  .vod-content-details__poster-wrap {
    width: 320px;
  }

  .vod-content-details__inline-actions {
    gap: 8px;
    align-self: flex-start;
    width: 100%;
  }

  .vod-content-details__inline-actions :deep(.tic-button) {
    flex: 0 0 auto;
  }
}

@media (max-width: 600px) {
  .vod-content-details__top {
    align-items: stretch;
  }

  .vod-content-details__poster-wrap {
    width: 132px;
  }

  .vod-content-details__summary {
    justify-content: flex-start;
    min-height: calc(132px * 1.5);
  }

  .vod-content-details__title-row {
    gap: 8px;
    align-items: flex-start;
  }

  .vod-content-details__title {
    font-size: 1.35rem;
    line-height: 1.2;
  }

  .vod-content-details__inline-actions {
    gap: 2px;
    align-self: flex-start;
    padding-top: 10px;
    flex-direction: row;
    flex-wrap: nowrap;
  }

  .vod-content-details__inline-plot {
    margin-top: 12px !important;
  }

  .vod-content-details__inline-actions :deep(.tic-action-button) {
    flex: 0 0 auto;
  }

  .vod-content-details__lower {
    margin-top: 14px !important;
  }

  .vod-content-details__pills {
    gap: 6px;
  }

  .vod-content-details__pill {
    width: 100%;
  }

  .vod-content-details__season-tabs {
    margin-left: -2px;
    margin-right: -2px;
  }

  .vod-content-details__episode-item :deep(.q-item__section--side) {
    padding-left: 0;
  }
}
</style>
