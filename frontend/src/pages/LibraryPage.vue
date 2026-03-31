<template>
  <q-page>
    <div :class="$q.screen.lt.sm ? 'q-pa-none' : 'q-pa-md'">
      <div class="row">
        <div
          :class="uiStore.showHelp ? 'col-12 col-md-8 help-main' : 'col-12 help-main help-main--full'"
        >
          <div class="library-page__surface">
            <VodContentBrowser mode="curated" scroll-height="calc(100vh - 230px)" />
          </div>
        </div>

        <TicResponsiveHelp v-model="uiStore.showHelp">
          <q-card-section>
            <div class="text-h5 q-mb-none">How To Use This Page:</div>
            <q-list>
              <q-separator inset spaced />
              <q-item>
                <q-item-section>
                  <q-item-label>
                    <q-icon name="video_library" class="q-mr-xs" />
                    Browse TIC's curated <b>Movies</b> and <b>TV Series</b> library using the tabs at the top.
                  </q-item-label>
                </q-item-section>
              </q-item>
              <q-item>
                <q-item-section>
                  <q-item-label>
                    <q-icon name="search" class="q-mr-xs" />
                    Use search and category filters to narrow the library quickly.
                  </q-item-label>
                </q-item-section>
              </q-item>
              <q-item>
                <q-item-section>
                  <q-item-label>
                    <q-icon name="play_arrow" class="q-mr-xs" />
                    Open an item to read details, then watch the movie or an individual episode directly.
                  </q-item-label>
                </q-item-section>
              </q-item>
            </q-list>
          </q-card-section>
        </TicResponsiveHelp>
      </div>
    </div>
  </q-page>
</template>

<script>
import {defineComponent} from 'vue';
import {useUiStore} from 'stores/ui';
import VodContentBrowser from 'components/VodContentBrowser.vue';
import {TicResponsiveHelp} from 'components/ui';

export default defineComponent({
  name: 'LibraryPage',
  components: {
    TicResponsiveHelp,
    VodContentBrowser,
  },
  setup() {
    return {
      uiStore: useUiStore(),
    };
  },
});
</script>

<style scoped>
.library-page__surface {
  background: transparent;
  min-width: 0;
}

.help-main {
  transition: flex-basis 0.25s ease, max-width 0.25s ease;
  padding-right: 8px;
}

.help-main--full {
  flex: 0 0 100%;
  max-width: 100%;
  padding-right: 0;
}

@media (max-width: 1023px) {
  .help-main {
    padding-right: 0;
  }
}
</style>
