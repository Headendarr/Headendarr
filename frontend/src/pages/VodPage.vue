<template>
  <q-page>
    <div :class="$q.screen.lt.sm ? 'q-pa-none' : 'q-pa-md'">
      <div class="row">
        <div
          :class="uiStore.showHelp ? 'col-12 col-md-8 help-main' : 'col-12 help-main help-main--full'"
        >
          <q-card flat>
            <div class="dvr-tabs-bar row items-center justify-between q-col-gutter-sm">
              <div class="col">
                <q-tabs
                  v-model="activeTab"
                  align="left"
                  class="dvr-tabs text-primary"
                  content-class="dvr-tabs-content"
                >
                  <q-tab name="movie" :label="`Movies (${movieCategoryCount})`" />
                  <q-tab name="series" :label="`TV Series (${seriesCategoryCount})`" />
                </q-tabs>
              </div>
              <div class="col-auto q-pr-xs">
                <TicButton
                  v-if="!$q.screen.lt.sm"
                  label="Browse Upstream"
                  icon="travel_explore"
                  color="primary"
                  class="section-toolbar-btn"
                  @click="openUpstreamBrowser"
                />
                <TicButton
                  v-else
                  icon="travel_explore"
                  color="primary"
                  round
                  tooltip="Browse Upstream"
                  class="section-toolbar-icon-btn"
                  @click="openUpstreamBrowser"
                />
              </div>
            </div>

            <q-separator />

            <q-tab-panels v-model="activeTab" animated class="dvr-tab-panels">
              <q-tab-panel name="movie">
                <q-banner v-if="!status.show_page" rounded class="vod-status-banner q-mb-sm">
                  VOD management is hidden until at least one XC source with imported VOD or series content is
                  available.
                </q-banner>
                <template v-else>
                  <TicListToolbar
                    class="q-mb-sm dvr-toolbar"
                    :add-action="{label: 'Add Category', icon: 'add', disable: !availableCategories.length}"
                    :search="{label: 'Search categories', placeholder: 'Category name or source category...'}"
                    :search-value="categorySearch"
                    :filters="toolbarFilters"
                    @add="openCreateDialog"
                    @update:search-value="categorySearch = $event"
                    @filter-change="onToolbarFilterChange"
                  />

                  <q-list bordered separator class="channels-list rounded-borders q-pl-none">
                    <draggable
                      v-model="renderedCategories"
                      group="vod-groups"
                      item-key="id"
                      handle=".handle"
                      :component-data="{tag: 'ul', name: 'flip-list', type: 'transition'}"
                      v-bind="dragOptions"
                      @change="setCategoryOrder"
                    >
                      <template #item="{element}">
                        <q-item
                          :key="element.id"
                          class="channel-list-item q-px-none rounded-borders"
                          :class="!$q.screen.lt.md ? categoryRowClass(element) : ''"
                        >
                          <template v-if="!$q.screen.lt.md">
                            <q-item-section avatar class="q-px-sm q-mx-sm handle">
                              <q-avatar rounded>
                                <q-icon name="format_line_spacing" style="max-width: 30px; cursor: grab">
                                  <q-tooltip class="bg-white text-primary">
                                    {{ canDragCategories ?
                                    'Drag to move and re-order' :
                                    'Clear search and filters to re-order' }}
                                  </q-tooltip>
                                </q-icon>
                              </q-avatar>
                            </q-item-section>

                            <q-separator inset vertical class="gt-xs" />

                            <q-item-section top class="q-mx-md">
                              <q-item-label lines="1" class="text-left">
                                <span
                                  class="text-weight-medium channel-name-label"
                                  :class="categoryTitleTextClass(element)"
                                >
                                  {{ element.name }}
                                </span>
                              </q-item-label>
                              <q-item-label caption lines="1" class="text-left q-ml-none">
                                Items: {{ element.item_count }} | Profile: {{ profileLabel(element.profile_id) }}
                              </q-item-label>
                            </q-item-section>

                            <q-separator inset vertical class="gt-xs gt-sm" />

                            <q-item-section class="q-px-sm q-mx-sm gt-sm">
                              <q-item-label lines="1" class="text-left">
                                <span class="q-ml-sm">Source Categories</span>
                              </q-item-label>
                              <q-item-label caption lines="2" class="text-left q-ml-sm channel-wrap-text">
                                {{ sourceCategoryNames(element) || 'None configured' }}
                              </q-item-label>
                            </q-item-section>

                            <q-separator inset vertical class="gt-xs" />

                            <q-item-section side class="q-mr-md">
                              <TicListActions
                                :actions="categoryActions"
                                @action="(action) => onCategoryAction(action, element)"
                              />
                            </q-item-section>
                          </template>

                          <template v-else>
                            <q-item-section>
                              <TicListItemCard v-bind="categoryCardProps(element)">
                                <template #header-left>
                                  <div class="row items-center no-wrap q-pr-sm">
                                    <div class="handle channel-drag-handle q-mr-sm">
                                      <q-icon name="format_line_spacing" size="22px" style="cursor: grab" />
                                    </div>
                                    <div>
                                      <div class="text-caption text-grey-7">Reorder</div>
                                      <div class="text-caption text-grey-6 lt-sm">Tap and hold to drag</div>
                                    </div>
                                  </div>
                                </template>
                                <template #header-actions>
                                  <TicActionButton
                                    v-for="action in categoryActions"
                                    :key="`compact-movie-${element.id}-${action.id}`"
                                    :icon="action.icon"
                                    :color="action.color || 'grey-8'"
                                    :tooltip="action.label || ''"
                                    @click="onCategoryAction(action, element)"
                                  />
                                </template>
                                <div class="row items-start q-col-gutter-md">
                                  <div class="col-12 col-sm">
                                    <div class="text-weight-medium" :class="categoryTitleTextClass(element)">
                                      {{ element.name }}
                                    </div>
                                    <div class="text-caption text-grey-7">
                                      Items: {{ element.item_count }} | Profile: {{ profileLabel(element.profile_id) }}
                                    </div>
                                  </div>
                                  <div class="col-12 col-sm-5">
                                    <div class="text-caption text-grey-7">Source Categories</div>
                                    <div class="text-caption channel-wrap-text">
                                      {{ sourceCategoryNames(element) || 'None configured' }}
                                    </div>
                                  </div>
                                </div>
                              </TicListItemCard>
                            </q-item-section>
                          </template>
                        </q-item>
                      </template>
                    </draggable>
                  </q-list>
                  <div v-if="!loading && !visibleCategories.length" class="q-pa-md text-caption text-grey-7">
                    No curated categories found.
                  </div>
                </template>
              </q-tab-panel>

              <q-tab-panel name="series">
                <q-banner v-if="!status.show_page" rounded class="vod-status-banner q-mb-sm">
                  VOD management is hidden until at least one XC source with imported VOD or series content is
                  available.
                </q-banner>
                <template v-else>
                  <TicListToolbar
                    class="q-mb-sm dvr-toolbar"
                    :add-action="{label: 'Add Category', icon: 'add', disable: !availableCategories.length}"
                    :search="{label: 'Search categories', placeholder: 'Category name or source category...'}"
                    :search-value="categorySearch"
                    :filters="toolbarFilters"
                    @add="openCreateDialog"
                    @update:search-value="categorySearch = $event"
                    @filter-change="onToolbarFilterChange"
                  />

                  <q-list bordered separator class="channels-list rounded-borders q-pl-none">
                    <draggable
                      v-model="renderedCategories"
                      group="vod-groups"
                      item-key="id"
                      handle=".handle"
                      :component-data="{tag: 'ul', name: 'flip-list', type: 'transition'}"
                      v-bind="dragOptions"
                      @change="setCategoryOrder"
                    >
                      <template #item="{element}">
                        <q-item
                          :key="element.id"
                          class="channel-list-item q-px-none rounded-borders"
                          :class="!$q.screen.lt.md ? categoryRowClass(element) : ''"
                        >
                          <template v-if="!$q.screen.lt.md">
                            <q-item-section avatar class="q-px-sm q-mx-sm handle">
                              <q-avatar rounded>
                                <q-icon name="format_line_spacing" style="max-width: 30px; cursor: grab">
                                  <q-tooltip class="bg-white text-primary">
                                    {{ canDragCategories ?
                                    'Drag to move and re-order' :
                                    'Clear search and filters to re-order' }}
                                  </q-tooltip>
                                </q-icon>
                              </q-avatar>
                            </q-item-section>

                            <q-separator inset vertical class="gt-xs" />

                            <q-item-section top class="q-mx-md">
                              <q-item-label lines="1" class="text-left">
                                <span
                                  class="text-weight-medium channel-name-label"
                                  :class="categoryTitleTextClass(element)"
                                >
                                  {{ element.name }}
                                </span>
                              </q-item-label>
                              <q-item-label caption lines="1" class="text-left q-ml-none">
                                Items: {{ element.item_count }} | Profile: {{ profileLabel(element.profile_id) }}
                              </q-item-label>
                            </q-item-section>

                            <q-separator inset vertical class="gt-xs gt-sm" />

                            <q-item-section class="q-px-sm q-mx-sm gt-sm">
                              <q-item-label lines="1" class="text-left">
                                <span class="q-ml-sm">Source Categories</span>
                              </q-item-label>
                              <q-item-label caption lines="2" class="text-left q-ml-sm channel-wrap-text">
                                {{ sourceCategoryNames(element) || 'None configured' }}
                              </q-item-label>
                            </q-item-section>

                            <q-separator inset vertical class="gt-xs" />

                            <q-item-section side class="q-mr-md">
                              <TicListActions
                                :actions="categoryActions"
                                @action="(action) => onCategoryAction(action, element)"
                              />
                            </q-item-section>
                          </template>

                          <template v-else>
                            <q-item-section>
                              <TicListItemCard v-bind="categoryCardProps(element)">
                                <template #header-left>
                                  <div class="row items-center no-wrap q-pr-sm">
                                    <div class="handle channel-drag-handle q-mr-sm">
                                      <q-icon name="format_line_spacing" size="22px" style="cursor: grab" />
                                    </div>
                                    <div>
                                      <div class="text-caption text-grey-7">Reorder</div>
                                      <div class="text-caption text-grey-6 lt-sm">Tap and hold to drag</div>
                                    </div>
                                  </div>
                                </template>
                                <template #header-actions>
                                  <TicActionButton
                                    v-for="action in categoryActions"
                                    :key="`compact-series-${element.id}-${action.id}`"
                                    :icon="action.icon"
                                    :color="action.color || 'grey-8'"
                                    :tooltip="action.label || ''"
                                    @click="onCategoryAction(action, element)"
                                  />
                                </template>
                                <div class="row items-start q-col-gutter-md">
                                  <div class="col-12 col-sm">
                                    <div class="text-weight-medium" :class="categoryTitleTextClass(element)">
                                      {{ element.name }}
                                    </div>
                                    <div class="text-caption text-grey-7">
                                      Items: {{ element.item_count }} | Profile: {{ profileLabel(element.profile_id) }}
                                    </div>
                                  </div>
                                  <div class="col-12 col-sm-5">
                                    <div class="text-caption text-grey-7">Source Categories</div>
                                    <div class="text-caption channel-wrap-text">
                                      {{ sourceCategoryNames(element) || 'None configured' }}
                                    </div>
                                  </div>
                                </div>
                              </TicListItemCard>
                            </q-item-section>
                          </template>
                        </q-item>
                      </template>
                    </draggable>
                  </q-list>
                  <div v-if="!loading && !visibleCategories.length" class="q-pa-md text-caption text-grey-7">
                    No curated categories found.
                  </div>
                </template>
              </q-tab-panel>
            </q-tab-panels>

            <q-inner-loading :showing="loading">
              <q-spinner-dots size="40px" color="primary" />
            </q-inner-loading>
          </q-card>
        </div>

        <TicResponsiveHelp v-model="uiStore.showHelp">
          <q-card-section>
            <div class="text-h5 q-mb-none">How To Use This Page:</div>
            <q-list>
              <q-separator inset spaced />
              <q-item>
                <q-item-section>
                  <q-item-label>
                    <q-icon name="add" class="q-mr-xs" />
                    Create curated VOD categories for <b>Movies</b> and <b>TV Series</b> using the tabs at the top of
                    the page.
                  </q-item-label>
                </q-item-section>
              </q-item>
              <q-item>
                <q-item-section>
                  <q-item-label>
                    <q-icon name="source" class="q-mr-xs" />
                    You can combine upstream categories from multiple XC sources into one curated VOD category. Only
                    linked upstream categories in that curated category take part in deduplication.
                  </q-item-label>
                </q-item-section>
              </q-item>
              <q-item>
                <q-item-section>
                  <q-item-label>
                    <q-icon name="tune" class="q-mr-xs" />
                    Configure prefix and suffix stripping on each linked upstream category if a provider adds language
                    markers, stars, year suffixes, or other title decorations.
                  </q-item-label>
                </q-item-section>
              </q-item>
              <q-item>
                <q-item-section>
                  <q-item-label>
                    <q-icon name="merge_type" class="q-mr-xs" />
                    Movies and series dedupe by cleaned title and year. Episodes prefer TMDB ID when available,
                    otherwise season and episode number, and only fall back to cleaned episode titles if required.
                  </q-item-label>
                </q-item-section>
              </q-item>
              <q-item>
                <q-item-section>
                  <q-item-label>
                    <q-icon name="movie_filter" class="q-mr-xs" />
                    Use the profile setting on each curated VOD category to choose whether playback should pass the
                    source through or force a specific remux or transcode profile.
                  </q-item-label>
                </q-item-section>
              </q-item>
            </q-list>
          </q-card-section>
        </TicResponsiveHelp>
      </div>
    </div>

    <VodGroupInfoDialog ref="vodCategoryInfoDialogRef" @ok="onDialogSaved" />
    <UpstreamVodBrowserDialog ref="upstreamVodBrowserDialogRef" />
  </q-page>
</template>

<script>
import {defineComponent} from 'vue';
import axios from 'axios';
import draggable from 'vuedraggable';
import {useUiStore} from 'stores/ui';
import UpstreamVodBrowserDialog from 'components/UpstreamVodBrowserDialog.vue';
import VodGroupInfoDialog from 'components/VodGroupInfoDialog.vue';
import {
  TicActionButton,
  TicButton,
  TicListActions,
  TicListItemCard,
  TicListToolbar,
  TicResponsiveHelp,
} from 'components/ui';

export default defineComponent({
  name: 'VodPage',
  components: {
    draggable,
    TicActionButton,
    TicButton,
    TicListActions,
    TicListItemCard,
    TicListToolbar,
    TicResponsiveHelp,
    UpstreamVodBrowserDialog,
    VodGroupInfoDialog,
  },
  setup() {
    return {
      uiStore: useUiStore(),
    };
  },
  data() {
    return {
      loading: false,
      activeTab: 'movie',
      categorySearch: '',
      sourceFilter: null,
      status: {
        show_page: false,
      },
      categoriesByType: {
        movie: [],
        series: [],
      },
      curatedCategoriesByType: {
        movie: [],
        series: [],
      },
      users: [],
      profileDefinitions: [],
      profileSettings: {},
      categoryActions: [
        {id: 'edit', icon: 'tune', label: 'Configure category', color: 'primary'},
      ],
    };
  },
  computed: {
    dragOptions() {
      return {
        animation: 100,
        group: 'vodCategories',
        disabled: !this.canDragCategories,
        ghostClass: 'ghost',
        direction: 'vertical',
        delay: 200,
        delayOnTouchOnly: true,
      };
    },
    curatedCategories() {
      return this.curatedCategoriesByType[this.activeTab] || [];
    },
    canDragCategories() {
      return !String(this.categorySearch || '').trim() && !this.sourceFilter;
    },
    filteredCategories() {
      const search = String(this.categorySearch || '').trim().toLowerCase();
      return (this.curatedCategories || []).filter((category) => {
        if (this.sourceFilter &&
          !(category.categories || []).some((item) => Number(item.playlist_id) === Number(this.sourceFilter))) {
          return false;
        }
        if (!search) {
          return true;
        }
        const values = [
          category.name,
          ...(category.categories || []).map((item) => `${item.playlist_name} ${item.name}`),
        ];
        return values.some((value) => String(value || '').toLowerCase().includes(search));
      });
    },
    visibleCategories() {
      return this.filteredCategories;
    },
    renderedCategories: {
      get() {
        return this.canDragCategories ? (this.curatedCategoriesByType[this.activeTab] || []) : this.filteredCategories;
      },
      set(value) {
        this.curatedCategoriesByType = {
          ...this.curatedCategoriesByType,
          [this.activeTab]: value,
        };
      },
    },
    availableCategories() {
      return this.categoriesByType[this.activeTab] || [];
    },
    movieCategoryCount() {
      return (this.curatedCategoriesByType.movie || []).length;
    },
    seriesCategoryCount() {
      return (this.curatedCategoriesByType.series || []).length;
    },
    sourceFilterOptions() {
      const options = [{label: 'All sources', value: null}];
      const seen = new Map();
      (this.categoriesByType[this.activeTab] || []).forEach((category) => {
        const playlistId = Number(category.playlist_id);
        if (seen.has(playlistId)) {
          return;
        }
        seen.set(playlistId, category.playlist_name);
        options.push({label: category.playlist_name, value: playlistId});
      });
      return options;
    },
    toolbarFilters() {
      return [
        {
          key: 'source',
          modelValue: this.sourceFilter,
          label: 'Source',
          options: this.sourceFilterOptions,
          clearable: false,
          dense: true,
          behavior: this.$q.screen.lt.md ? 'dialog' : 'menu',
        },
      ];
    },
  },
  watch: {
    activeTab() {
      this.sourceFilter = null;
      this.categorySearch = '';
      this.loadTabData();
    },
  },
  methods: {
    async loadPageState() {
      this.loading = true;
      try {
        const [statusResponse, settingsResponse, usersResponse] = await Promise.all([
          axios.get('/tic-api/vod/status'),
          axios.get('/tic-api/get-settings'),
          axios.get('/tic-api/users'),
        ]);
        this.status = statusResponse?.data?.data || {show_page: false};
        const settings = settingsResponse?.data?.data || {};
        this.users = usersResponse?.data?.data || [];
        this.profileDefinitions = settings.stream_profile_definitions || [];
        this.profileSettings = settings.stream_profiles || {};
        if (this.status.show_page) {
          await this.loadAllTabData();
        }
      } finally {
        this.loading = false;
      }
    },
    async loadAllTabData() {
      await Promise.all([this.loadContentTypeData('movie'), this.loadContentTypeData('series')]);
    },
    async loadTabData() {
      await this.loadContentTypeData(this.activeTab);
    },
    async loadContentTypeData(contentType) {
      await Promise.all([this.loadCategories(contentType), this.loadGroups(contentType)]);
    },
    async loadCategories(contentType = this.activeTab) {
      const response = await axios.get('/tic-api/vod/categories', {
        params: {content_type: contentType},
      });
      this.categoriesByType = {
        ...this.categoriesByType,
        [contentType]: response?.data?.data || [],
      };
    },
    async loadGroups(contentType = this.activeTab) {
      const response = await axios.get('/tic-api/vod/groups', {
        params: {content_type: contentType},
      });
      this.curatedCategoriesByType = {
        ...this.curatedCategoriesByType,
        [contentType]: response?.data?.data || [],
      };
    },
    onToolbarFilterChange({key, value}) {
      if (key === 'source') {
        this.sourceFilter = value;
      }
    },
    async setCategoryOrder() {
      if (!this.canDragCategories) {
        return;
      }
      const nextCategories = (this.curatedCategoriesByType[this.activeTab] || []).map((category, index) => ({
        ...category,
        sort_order: (index + 1) * 10,
      }));
      this.curatedCategoriesByType = {
        ...this.curatedCategoriesByType,
        [this.activeTab]: nextCategories,
      };
      try {
        await Promise.all(
          nextCategories.map((category) =>
            axios.put(`/tic-api/vod/groups/${category.id}`, {
              content_type: this.activeTab,
              enabled: !!category.enabled,
              name: category.name,
              sort_order: category.sort_order,
              profile_id: category.profile_id,
              category_ids: (category.categories || []).map((item) => item.id),
              category_configs: (category.categories || []).map((item) => ({
                category_id: item.id,
                strip_title_prefixes: item.strip_title_prefixes || [],
                strip_title_suffixes: item.strip_title_suffixes || [],
              })),
            }),
          ),
        );
      } catch {
        this.$q.notify({color: 'negative', message: 'Failed to save VOD category order'});
        await this.loadGroups();
      }
    },
    openCreateDialog() {
      this.$refs.vodCategoryInfoDialogRef.show({
        contentType: this.activeTab,
        availableCategories: this.availableCategories,
        existingCategoryNames: this.curatedCategories,
        eligibleStrmUserCount: this.eligibleStrmUserCount(this.activeTab),
        profileDefinitions: this.profileDefinitions,
        profileSettings: this.profileSettings,
        nextSortOrder: (this.curatedCategories[this.curatedCategories.length - 1]?.sort_order || 0) + 10,
      });
    },
    editCategory(category) {
      this.$refs.vodCategoryInfoDialogRef.show({
        contentType: this.activeTab,
        category,
        availableCategories: this.availableCategories,
        existingCategoryNames: this.curatedCategories,
        eligibleStrmUserCount: this.eligibleStrmUserCount(this.activeTab),
        profileDefinitions: this.profileDefinitions,
        profileSettings: this.profileSettings,
        nextSortOrder: category.sort_order,
      });
    },
    onCategoryAction(action, category) {
      if (action.id === 'edit') {
        this.editCategory(category);
      }
    },
    openUpstreamBrowser() {
      this.$refs.upstreamVodBrowserDialogRef?.show();
    },
    async onDialogSaved() {
      await this.loadGroups();
    },
    categoryTitleTextClass(category) {
      return category?.enabled ? 'text-primary' : 'text-grey-8';
    },
    categoryCardProps(category) {
      if (category?.enabled) {
        return {
          accentColor: '',
          surfaceColor: 'var(--tic-list-card-default-bg)',
          headerColor: 'var(--tic-list-card-default-header-bg)',
        };
      }
      return {
        accentColor: 'var(--tic-list-card-disabled-border)',
        surfaceColor: 'var(--tic-list-card-disabled-bg)',
        headerColor: 'var(--tic-list-card-disabled-header)',
        textColor: 'var(--q-grey-8, #616161)',
      };
    },
    profileLabel(profileId) {
      if (!profileId) {
        return 'Pass';
      }
      const found = (this.profileDefinitions || []).find((item) => item.key === profileId);
      return found ? found.label : profileId;
    },
    formatCategoryStripRules(category) {
      const parts = [];
      if ((category.strip_title_prefixes || []).length) {
        parts.push(`prefixes: ${(category.strip_title_prefixes || []).join(', ')}`);
      }
      if ((category.strip_title_suffixes || []).length) {
        parts.push(`suffixes: ${(category.strip_title_suffixes || []).join(', ')}`);
      }
      return parts.join(' | ');
    },
    categoryRowClass(category) {
      return category.enabled ? '' : 'channel-disabled';
    },
    sourceCategoryNames(category) {
      return (category.categories || []).map((item) => `${item.playlist_name}: ${item.name}`).join(', ');
    },
    userCanAccessVodKind(user, contentType) {
      if ((user?.roles || []).includes('admin')) {
        return true;
      }
      const mode = String(user?.vod_access_mode || 'none').trim().toLowerCase();
      if (contentType === 'movie') {
        return mode === 'movies' || mode === 'movies_series';
      }
      if (contentType === 'series') {
        return mode === 'series' || mode === 'movies_series';
      }
      return false;
    },
    eligibleStrmUserCount(contentType) {
      return (this.users || []).filter((user) =>
        Boolean(user?.is_active) &&
        Boolean(String(user?.streaming_key || '').trim()) &&
        Boolean(user?.vod_generate_strm_files) &&
        this.userCanAccessVodKind(user, contentType),
      ).length;
    },
  },
  mounted() {
    this.loadPageState();
  },
});
</script>

<style scoped>
.dvr-tabs-bar {
  background: transparent;
}

.dvr-tabs {
  background: transparent !important;
}

.dvr-tabs :deep(.dvr-tabs-content) {
  background: var(--guide-channel-bg);
}

.dvr-tab-panels {
  background: transparent !important;
}

.dvr-tab-panels :deep(.q-tab-panel) {
  background: transparent !important;
}

.vod-status-banner {
  background: var(--tic-list-card-default-header-bg);
  color: var(--q-grey-2);
  border: 1px solid var(--q-separator-color);
}

.section-toolbar-btn {
  margin-right: 4px;
}

.section-toolbar-icon-btn {
  margin-right: 4px;
}

.channel-list-item {
  margin-bottom: 8px;
  padding: 8px 0;
  border-radius: var(--tic-radius-md);
  overflow: hidden;
}

.channels-list .channel-list-item:last-child {
  margin-bottom: 0;
}

.channel-disabled {
  background: var(--tic-list-card-disabled-bg);
  border-left: 4px solid var(--tic-list-card-disabled-border);
  color: var(--q-grey-8, #616161);
}

.channel-meta-list {
  display: flex;
  flex-direction: column;
  gap: 2px;
}

.channel-meta-row {
  display: flex;
  gap: 6px;
  align-items: baseline;
}

.channel-meta-label {
  min-width: 52px;
  font-weight: 600;
  color: var(--q-grey-7);
}

.channel-meta-value {
  min-width: 0;
}

.channel-wrap-text {
  white-space: normal;
  word-break: break-word;
}

.channel-name-label {
  line-height: 1.3;
}

.channel-drag-handle {
  min-width: 24px;
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

.help-panel {
  padding-left: 8px;
}

@media (max-width: 1023px) {
  .channels-list {
    border: none;
  }

  .channel-list-item {
    margin-top: 4px;
    margin-bottom: 4px;
    padding-left: 0;
    padding-right: 0;
    padding-top: 0;
    padding-bottom: 0;
  }

  .help-main {
    padding-right: 0;
  }

  .help-panel {
    padding-left: 0;
  }
}
</style>
