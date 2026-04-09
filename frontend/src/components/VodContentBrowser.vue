<template>
  <div class="vod-browser">
    <div class="vod-browser__tabs-bar">
      <q-tabs
        v-model="activeTab"
        align="left"
        class="vod-browser__tabs text-primary"
        content-class="vod-browser__tabs-content"
      >
        <q-tab name="movie" label="Movies" />
        <q-tab name="series" label="TV Series" />
      </q-tabs>
    </div>

    <q-separator />

    <TicListToolbar
      class="vod-browser__toolbar"
      :search="searchConfig"
      :search-value="searchValue"
      :filters="toolbarFilters"
      @update:search-value="searchValue = $event"
      @search="resetAndReload"
      @filter-change="onToolbarFilterChange"
      @filters="filtersDialogOpen = true"
    />

    <div
      :id="scrollTargetId"
      ref="scrollTargetRef"
      class="vod-browser__scroll"
      :style="scrollAreaStyle"
    >
      <q-infinite-scroll
        ref="infiniteScrollRef"
        :disable="allLoaded || loadingMore || loadingInitial"
        :offset="180"
        :scroll-target="`#${scrollTargetId}`"
        @load="loadMore"
      >
        <div v-if="rows.length" class="vod-browser__grid">
          <TicVodContentCard
            v-for="item in rows"
            :key="`${activeTab}-${mode}-${item.id}`"
            :title="displayTitle(item)"
            :poster-url="item.poster_url"
            :plot="item.plot"
            :category-label="categoryMeta(item)"
            :secondary-label="secondaryMeta(item)"
            :type-label="activeTab === 'movie' ? 'Movie' : 'Series'"
            :year-label="displayYear(item)"
            :content-type="activeTab"
            @select="openItemDetails(item)"
          />
        </div>

        <template #loading>
          <div class="row flex-center q-my-md">
            <q-spinner-dots size="30px" color="primary" />
          </div>
        </template>
      </q-infinite-scroll>

      <div v-if="!loadingInitial && !rows.length" class="vod-browser__empty">
        <q-icon name="video_library" size="2em" color="grey-6" />
        <div>{{ emptyStateLabel }}</div>
      </div>

      <q-inner-loading :showing="loadingInitial">
        <q-spinner-dots size="42px" color="primary" />
      </q-inner-loading>
    </div>

    <VodContentDetails
      v-model="detailDialogOpen"
      :dialog-title="detailDialogTitle"
      :loading="detailLoading"
      :detail-item="detailItem"
      :active-tab="activeTab"
      :display-title="detailDisplayTitle"
      :meta-items="detailMetaItems"
      :genre-line="detailGenreLine"
      :cast-line="detailCastLine"
      :external-links="detailExternalLinks"
      :detail-actions="detailActions"
      :season-options="detailSeasonOptions"
      :selected-season="selectedDetailSeason"
      :visible-episodes="visibleDetailEpisodes"
      :episode-label="episodeLabel"
      :episode-actions="episodeActions"
      @detail-action="handleDetailAction"
      @episode-action="handleEpisodeAction($event.action, $event.episode)"
      @update:selected-season="selectedDetailSeason = $event"
    />

    <TicDialogPopup
      v-model="filtersDialogOpen"
      title="Filters"
      width="560px" max-width="95vw"
      @hide="resetFilterDrafts"
    >
      <div class="tic-form-layout">
        <TicSelectInput
          v-if="mode === 'upstream'"
          v-model="draftSourceId"
          label="Source"
          description="Filter items by source playlist."
          :options="sourceOptions"
          option-label="label"
          option-value="value"
          :emit-value="true"
          :map-options="true"
          :clearable="false"
          :dense="true"
          :behavior="$q.screen.lt.md ? 'dialog' : 'menu'"
        />

        <TicSelectInput
          v-model="draftCategoryId"
          label="Category"
          :description="mode === 'upstream' ? 'Filter items by upstream category.' : 'Filter items by curated category.'"
          :options="draftCategoryOptions"
          option-label="label"
          option-value="value"
          :emit-value="true"
          :map-options="true"
          :clearable="false"
          :dense="true"
          :disable="mode === 'upstream' && !draftSourceId"
          :behavior="$q.screen.lt.md ? 'dialog' : 'menu'"
        />
      </div>

      <template #actions>
        <TicButton label="Clear" variant="flat" color="grey-7" @click="clearFilterDrafts" />
        <TicButton label="Apply" icon="check" color="positive" @click="applyFilterDrafts" />
      </template>
    </TicDialogPopup>
  </div>
</template>

<script>
import axios from 'axios';
import {copyToClipboard} from 'quasar';
import {useVideoStore} from 'stores/video';
import {TicButton, TicDialogPopup, TicListToolbar, TicSelectInput, TicVodContentCard} from 'components/ui';
import VodContentDetails from 'components/VodContentDetails.vue';
import {
  buildVodPlaybackProfiles,
  isBrowserSafeVodSourceContainer,
  resolveVodPlayerStreamType,
} from 'src/utils/vodPlaybackProfiles';
import {normalisePreviewCandidates, primaryPreviewCandidate} from 'src/utils/previewCandidates';

const BROWSER_PAGE_SIZE = 40;

export default {
  name: 'VodContentBrowser',
  components: {
    TicDialogPopup,
    TicListToolbar,
    TicButton,
    TicSelectInput,
    TicVodContentCard,
    VodContentDetails,
  },
  props: {
    mode: {
      type: String,
      default: 'curated',
    },
    scrollHeight: {
      type: String,
      default: '',
    },
  },
  setup() {
    return {
      videoStore: useVideoStore(),
    };
  },
  data() {
    return {
      activeTab: 'movie',
      searchValue: '',
      selectedSourceId: null,
      selectedCategoryId: null,
      rows: [],
      loadOffset: 0,
      loadingInitial: false,
      loadingMore: false,
      allLoaded: false,
      searchReloadTimer: null,
      upstreamCategoriesByType: {
        movie: [],
        series: [],
      },
      curatedCategoriesByType: {
        movie: [],
        series: [],
      },
      detailDialogOpen: false,
      detailLoading: false,
      detailItem: null,
      selectedDetailSeason: 'season-1',
      filtersDialogOpen: false,
      draftSourceId: null,
      draftCategoryId: null,
      scrollTargetId: `vod-browser-scroll-${Math.random().toString(36).slice(2, 10)}`,
    };
  },
  computed: {
    scrollAreaStyle() {
      if (!this.scrollHeight) {
        return null;
      }
      return {height: this.scrollHeight};
    },
    searchConfig() {
      return {
        label: this.mode === 'upstream' ? 'Search upstream content' : 'Search library',
        placeholder: this.mode === 'upstream'
          ? 'Title, source, category, year...'
          : 'Title, category, year...',
        debounce: 300,
        clearable: true,
      };
    },
    sourceOptions() {
      if (this.mode !== 'upstream') {
        return [];
      }
      const options = [{label: 'All sources', value: null}];
      const seen = new Map();
      (this.upstreamCategoriesByType[this.activeTab] || []).forEach((category) => {
        const playlistId = Number(category.playlist_id);
        if (!playlistId || seen.has(playlistId)) {
          return;
        }
        seen.set(playlistId, category.playlist_name);
        options.push({
          label: category.playlist_name,
          value: playlistId,
        });
      });
      return options;
    },
    categoryOptions() {
      return this.buildCategoryOptions(this.selectedSourceId);
    },
    toolbarFilters() {
      const filters = [];
      if (this.mode === 'upstream') {
        filters.push({
          key: 'source',
          modelValue: this.selectedSourceId,
          label: 'Source',
          options: this.sourceOptions,
          optionLabel: 'label',
          optionValue: 'value',
          emitValue: true,
          mapOptions: true,
          clearable: false,
          dense: true,
          behavior: this.$q.screen.lt.md ? 'dialog' : 'menu',
        });
      }
      filters.push({
        key: 'category',
        modelValue: this.selectedCategoryId,
        label: 'Category',
        options: this.categoryOptions,
        optionLabel: 'label',
        optionValue: 'value',
        emitValue: true,
        mapOptions: true,
        clearable: false,
        dense: true,
        disable: this.mode === 'upstream' && !this.selectedSourceId,
        behavior: this.$q.screen.lt.md ? 'dialog' : 'menu',
      });
      return filters;
    },
    emptyStateLabel() {
      return this.mode === 'upstream'
        ? 'No upstream VOD items match the current search or filters.'
        : 'No curated VOD items match the current search or filters.';
    },
    detailDialogTitle() {
      if (!this.detailItem) {
        return this.activeTab === 'movie' ? 'Movie Details' : 'Series Details';
      }
      return this.mode === 'upstream' ? 'Upstream Details' : 'Library Details';
    },
    detailMetaItems() {
      if (!this.detailItem) {
        return [];
      }
      const items = [];
      const year = this.displayYear(this.detailItem);
      if (year) {
        items.push({label: 'Year', value: year});
      }
      if (this.detailItem.rating) {
        items.push({label: 'Rating', value: String(this.detailItem.rating)});
      }
      if (this.detailItem.category_name) {
        items.push({label: 'Category', value: this.detailItem.category_name});
      }
      if (this.detailItem.playlist_name) {
        items.push({label: 'Source', value: this.detailItem.playlist_name});
      }
      return items;
    },
    detailDisplayTitle() {
      return this.displayTitle(this.detailItem);
    },
    detailGenreLine() {
      if (!this.detailItem || !Array.isArray(this.detailItem.genre) || !this.detailItem.genre.length) {
        return '';
      }
      return `Genres: ${this.detailItem.genre.join(', ')}`;
    },
    detailCastLine() {
      if (!this.detailItem || !Array.isArray(this.detailItem.cast) || !this.detailItem.cast.length) {
        return '';
      }
      return `Cast: ${this.detailItem.cast.slice(0, 8).join(', ')}`;
    },
    detailExternalLinks() {
      if (!this.detailItem) {
        return [];
      }

      const links = [];
      const imdbId = String(this.detailItem.imdb_id || this.detailItem.imdb || '').trim();
      const tmdbId = String(this.detailItem.tmdb_id || this.detailItem.tmdb || '').trim();
      const trailerValue = String(this.detailItem.trailer || '').trim();

      if (tmdbId) {
        links.push({
          key: 'tmdb',
          label: 'TMDb',
          icon: 'movie',
          href: `https://www.themoviedb.org/${this.activeTab === 'series' ? 'tv' : 'movie'}/${encodeURIComponent(
            tmdbId)}`,
        });
      }
      if (imdbId) {
        links.push({
          key: 'imdb',
          label: 'IMDb',
          icon: 'open_in_new',
          href: `https://www.imdb.com/title/${encodeURIComponent(imdbId)}/`,
        });
      }
      if (trailerValue) {
        links.push({
          key: 'trailer',
          label: 'Trailer',
          icon: 'smart_display',
          href: this.resolveTrailerUrl(trailerValue),
        });
      }

      return links.filter((link) => link.href);
    },
    detailSeasonOptions() {
      if (this.activeTab !== 'series' || !Array.isArray(this.detailItem?.episodes)) {
        return [];
      }
      const seen = new Set();
      const options = [];
      this.detailItem.episodes.forEach((episode) => {
        const seasonNumber = Number(episode.season_number || 1);
        const key = `season-${seasonNumber}`;
        if (seen.has(key)) {
          return;
        }
        seen.add(key);
        options.push({
          value: key,
          label: `Season ${seasonNumber}`,
          seasonNumber,
        });
      });
      return options;
    },
    visibleDetailEpisodes() {
      if (!Array.isArray(this.detailItem?.episodes)) {
        return [];
      }
      const selectedSeasonNumber = Number(String(this.selectedDetailSeason || 'season-1').replace('season-', '')) || 1;
      return this.detailItem.episodes.filter((episode) => {
        return Number(episode.season_number || 1) === selectedSeasonNumber;
      });
    },
    draftCategoryOptions() {
      return this.buildCategoryOptions(this.draftSourceId);
    },
    detailActions() {
      if (!this.detailItem) {
        return [];
      }
      if (this.mode === 'curated' && this.activeTab === 'movie') {
        return [
          {id: 'watch-movie', icon: 'play_arrow', label: 'Watch Now', color: 'primary', tooltip: 'Watch Now'},
          {
            id: 'copy-movie-url',
            icon: 'content_copy',
            label: 'Copy Stream URL',
            color: 'grey-8',
            tooltip: 'Copy Stream URL',
          },
        ];
      }
      if (this.mode === 'upstream' && this.activeTab === 'movie') {
        return [
          {
            id: 'preview-upstream-movie',
            icon: 'play_arrow',
            label: 'Preview Stream',
            color: 'primary',
            tooltip: 'Preview Stream',
          },
          {
            id: 'copy-upstream-movie-url',
            icon: 'content_copy',
            label: 'Copy Stream URL',
            color: 'grey-8',
            tooltip: 'Copy Stream URL',
          },
        ];
      }
      return [];
    },
  },
  watch: {
    activeTab() {
      this.selectedSourceId = null;
      this.selectedCategoryId = null;
      this.searchValue = '';
      this.detailDialogOpen = false;
      this.filtersDialogOpen = false;
      this.preloadFilterData(this.activeTab).then(() => {
        this.resetAndReload();
      });
    },
    searchValue() {
      if (this.searchReloadTimer) {
        clearTimeout(this.searchReloadTimer);
      }
      this.searchReloadTimer = setTimeout(() => {
        this.resetAndReload();
      }, 350);
    },
    detailDialogOpen(nextValue) {
      if (!nextValue) {
        this.selectedDetailSeason = 'season-1';
      }
    },
    draftSourceId() {
      this.draftCategoryId = null;
    },
    filtersDialogOpen(nextValue) {
      if (nextValue) {
        this.resetFilterDrafts();
      }
    },
  },
  methods: {
    buildCategoryOptions(sourceId) {
      if (this.mode === 'upstream') {
        const options = [{label: 'All categories', value: null}];
        (this.upstreamCategoriesByType[this.activeTab] || []).forEach((category) => {
          if (sourceId && Number(category.playlist_id) !== Number(sourceId)) {
            return;
          }
          options.push({
            label: `${category.playlist_name}: ${category.name}`,
            value: Number(category.id),
          });
        });
        return options;
      }

      return [
        {label: 'All categories', value: null},
        ...((this.curatedCategoriesByType[this.activeTab] || []).map((category) => ({
          label: `${category.name} (${category.item_count || 0})`,
          value: Number(category.id),
        }))),
      ];
    },
    async preloadFilterData(contentType) {
      if (this.mode === 'upstream') {
        const response = await axios.get('/tic-api/vod/categories', {
          params: {content_type: contentType},
        });
        this.upstreamCategoriesByType = {
          ...this.upstreamCategoriesByType,
          [contentType]: response?.data?.data || [],
        };
        return;
      }

      const response = await axios.get('/tic-api/library/categories', {
        params: {content_type: contentType},
      });
      this.curatedCategoriesByType = {
        ...this.curatedCategoriesByType,
        [contentType]: response?.data?.data || [],
      };
    },
    onToolbarFilterChange({key, value}) {
      if (key === 'source') {
        this.selectedSourceId = value || null;
        this.selectedCategoryId = null;
      }
      if (key === 'category') {
        this.selectedCategoryId = value || null;
      }
      this.resetAndReload();
    },
    resetFilterDrafts() {
      this.draftSourceId = this.selectedSourceId;
      this.draftCategoryId = this.selectedCategoryId;
    },
    clearFilterDrafts() {
      this.draftSourceId = null;
      this.draftCategoryId = null;
    },
    applyFilterDrafts() {
      this.selectedSourceId = this.mode === 'upstream' ? this.draftSourceId : null;
      this.selectedCategoryId = this.draftCategoryId;
      this.filtersDialogOpen = false;
      this.resetAndReload();
    },
    async loadMore(index, done) {
      await this.loadNextChunk();
      done();
    },
    async resetAndReload() {
      this.rows = [];
      this.loadOffset = 0;
      this.allLoaded = false;
      this.loadingInitial = true;
      if (this.$refs.scrollTargetRef?.scrollTo) {
        this.$refs.scrollTargetRef.scrollTo({top: 0});
      }
      await this.loadNextChunk();
      this.loadingInitial = false;
    },
    async loadNextChunk() {
      if (this.loadingMore || this.allLoaded) {
        return;
      }

      this.loadingMore = true;
      try {
        const endpoint = this.mode === 'upstream' ? '/tic-api/vod/browser/items' : '/tic-api/library/items';
        const response = await axios.get(endpoint, {
          params: {
            content_type: this.activeTab,
            playlist_id: this.mode === 'upstream' ? this.selectedSourceId : undefined,
            category_id: this.selectedCategoryId,
            search: this.searchValue || undefined,
            offset: this.loadOffset,
            limit: BROWSER_PAGE_SIZE,
          },
        });
        const payload = response?.data?.data || {};
        const items = Array.isArray(payload.items) ? payload.items : [];
        this.rows = [...this.rows, ...items];
        this.loadOffset += items.length;
        this.allLoaded = !payload.has_more;
      } catch {
        this.$q.notify({
          color: 'negative',
          message: this.mode === 'upstream'
            ? 'Failed to load upstream VOD items'
            : 'Failed to load VOD library items',
        });
      } finally {
        this.loadingMore = false;
      }
    },
    categoryMeta(item) {
      if (item.category_name) {
        return item.category_name;
      }
      return this.activeTab === 'movie' ? 'Movie' : 'TV Series';
    },
    displayTitle(item) {
      const title = String(item?.title || '');
      const matched = title.match(/^(.*?)(?:\s[-(]\s*(\d{4})\)?)$/);
      if (matched?.[1] && !item?.year) {
        return matched[1].trim();
      }
      return title;
    },
    displayYear(item) {
      if (item?.year) {
        return String(item.year);
      }
      const title = String(item?.title || '');
      const matched = title.match(/(?:\s[-(]\s*)(\d{4})\)?$/);
      return matched?.[1] || '';
    },
    secondaryMeta(item) {
      if (this.mode === 'upstream') {
        return item.playlist_name || 'Upstream source';
      }
      return item.rating ? `Rating: ${item.rating}` : item.release_date || 'Curated library';
    },
    async openItemDetails(item) {
      this.detailDialogOpen = true;
      this.detailLoading = true;
      this.detailItem = null;
      try {
        const endpoint = this.mode === 'upstream'
          ? `/tic-api/vod/browser/details/${item.id}`
          : `/tic-api/library/details/${item.id}`;
        const response = await axios.get(endpoint, {
          params: {content_type: this.activeTab},
        });
        this.detailItem = response?.data?.data || item;
        this.selectedDetailSeason = this.detailSeasonOptions[0]?.value || 'season-1';
      } catch {
        this.detailItem = item;
        this.selectedDetailSeason = 'season-1';
        this.$q.notify({
          color: 'negative',
          message: 'Failed to load item details',
        });
      } finally {
        this.detailLoading = false;
      }
    },
    episodeLabel(episode) {
      const parts = [];
      if (episode.season_number || episode.episode_number) {
        parts.push(
          `S${String(episode.season_number || 0).padStart(2, '0')}E${String(episode.episode_number || 0).
            padStart(2, '0')}`,
        );
      }
      if (episode.title) {
        parts.push(episode.title);
      }
      return parts.join(' • ') || 'Episode';
    },
    resolveTrailerUrl(value) {
      const trailerValue = String(value || '').trim();
      if (!trailerValue) {
        return '';
      }
      if (/^https?:\/\//i.test(trailerValue)) {
        return trailerValue;
      }
      return `https://www.youtube.com/watch?v=${encodeURIComponent(trailerValue)}`;
    },
    startBrowserPlayback(payload, title) {
      const candidates = normalisePreviewCandidates(payload);
      const primaryCandidate = candidates[0];
      const previewUrl = primaryCandidate?.url || '';
      if (!previewUrl) {
        throw new Error('Playback URL unavailable');
      }
      const sourceResolution = primaryCandidate?.sourceResolution || payload?.source_resolution || null;
      const durationSeconds = Number(primaryCandidate?.durationSeconds || payload?.duration_seconds || 0) || null;
      const streamType = resolveVodPlayerStreamType(primaryCandidate?.streamType || payload?.stream_type);
      const sourceContainer = String(
        primaryCandidate?.containerExtension || payload?.container_extension || '',
      ).trim().toLowerCase();
      const forceBrowserTranscode = sourceContainer && !isBrowserSafeVodSourceContainer(sourceContainer);
      const playbackProfiles = buildVodPlaybackProfiles(sourceResolution, streamType, forceBrowserTranscode);
      const initialProfile = playbackProfiles[0] || null;
      const playbackUrl = this.appendPlaybackProfile(previewUrl, initialProfile?.profile || '');
      this.videoStore.showPlayer({
        url: playbackUrl,
        candidates,
        title: title || 'VOD Playback',
        type: initialProfile?.streamType || streamType,
        seekMode: initialProfile?.seekMode || 'native',
        playbackProfiles,
        selectedPlaybackProfile: initialProfile?.id || ORIGINAL_BROWSER_VOD_PROFILE,
        previewMetadataUrl: previewUrl,
        sourceResolution,
        durationSeconds,
      });
    },
    appendPlaybackProfile(url, profileId) {
      if (!url) {
        return '';
      }
      try {
        const parsed = new URL(url, window.location.origin);
        if (profileId) {
          parsed.searchParams.set('profile', profileId);
        } else {
          parsed.searchParams.delete('profile');
        }
        return parsed.toString();
      } catch (error) {
        console.warn('Failed to append playback profile', error);
        return url;
      }
    },
    async resolveCuratedMoviePreview(item, options = {}) {
      const response = await axios.get(`/tic-api/vod/movie/${Number(item.id)}/preview`, {
        params: options,
      });
      if (response?.data?.success && normalisePreviewCandidates(response.data).length) {
        return response.data;
      }
      throw new Error(response?.data?.message || 'Failed to load preview');
    },
    async resolveCuratedEpisodePreview(episode, options = {}) {
      const response = await axios.get(`/tic-api/vod/series/${Number(episode.id)}/preview`, {
        params: options,
      });
      if (response?.data?.success && normalisePreviewCandidates(response.data).length) {
        return response.data;
      }
      throw new Error(response?.data?.message || 'Failed to load preview');
    },
    async resolveUpstreamMoviePreview(item, options = {}) {
      const response = await axios.get(`/tic-api/vod/upstream/movie/${Number(item.id)}/preview`, {
        params: options,
      });
      if (response?.data?.success && normalisePreviewCandidates(response.data).length) {
        return response.data;
      }
      throw new Error(response?.data?.message || 'Failed to load preview');
    },
    async resolveUpstreamEpisodePreview(item, episode, options = {}) {
      const response = await axios.get(
        `/tic-api/vod/upstream/series/${Number(item.id)}/${Number(episode?.id || 0)}/preview`,
        {params: options},
      );
      if (response?.data?.success && normalisePreviewCandidates(response.data).length) {
        return response.data;
      }
      throw new Error(response?.data?.message || 'Failed to load preview');
    },
    async watchMovie(item) {
      try {
        const playback = this.mode === 'upstream'
          ? await this.resolveUpstreamMoviePreview(item)
          : await this.resolveCuratedMoviePreview(item);
        this.startBrowserPlayback(playback, item.title || 'Movie');
      } catch (error) {
        this.$q.notify({color: 'negative', message: error?.message || 'Failed to start playback'});
      }
    },
    async previewUpstreamMovie(item) {
      try {
        const playback = await this.resolveUpstreamMoviePreview(item);
        this.startBrowserPlayback(playback, item.title || 'Movie');
      } catch (error) {
        this.$q.notify({color: 'negative', message: error?.message || 'Failed to load preview'});
      }
    },
    async copyMovieUrl(item) {
      try {
        const preview = this.mode === 'upstream'
          ? await this.resolveUpstreamMoviePreview(item)
          : await this.resolveCuratedMoviePreview(item);
        const primaryCandidate = primaryPreviewCandidate(preview);
        if (!primaryCandidate?.url) {
          throw new Error('Playback URL unavailable');
        }
        await copyToClipboard(primaryCandidate.url);
        this.$q.notify({color: 'positive', message: 'Stream URL copied'});
      } catch (error) {
        this.$q.notify({color: 'negative', message: error?.message || 'Failed to copy stream URL'});
      }
    },
    async watchEpisode(episode) {
      try {
        const playback = this.mode === 'upstream'
          ? await this.resolveUpstreamEpisodePreview(this.detailItem, episode)
          : await this.resolveCuratedEpisodePreview(episode);
        this.startBrowserPlayback(playback, this.episodeLabel(episode));
      } catch (error) {
        this.$q.notify({color: 'negative', message: error?.message || 'Failed to start playback'});
      }
    },
    async previewUpstreamEpisode(episode) {
      try {
        const playback = await this.resolveUpstreamEpisodePreview(this.detailItem, episode);
        this.startBrowserPlayback(playback, this.episodeLabel(episode));
      } catch (error) {
        this.$q.notify({color: 'negative', message: error?.message || 'Failed to load preview'});
      }
    },
    async copyEpisodeUrl(episode) {
      try {
        const preview = this.mode === 'upstream'
          ? await this.resolveUpstreamEpisodePreview(this.detailItem, episode)
          : await this.resolveCuratedEpisodePreview(episode);
        const primaryCandidate = primaryPreviewCandidate(preview);
        if (!primaryCandidate?.url) {
          throw new Error('Playback URL unavailable');
        }
        await copyToClipboard(primaryCandidate.url);
        this.$q.notify({color: 'positive', message: 'Stream URL copied'});
      } catch (error) {
        this.$q.notify({color: 'negative', message: error?.message || 'Failed to copy stream URL'});
      }
    },
    handleDetailAction(action) {
      if (!this.detailItem || !action?.id) {
        return;
      }
      if (action.id === 'watch-movie') {
        this.watchMovie(this.detailItem);
        return;
      }
      if (action.id === 'copy-movie-url') {
        this.copyMovieUrl(this.detailItem);
        return;
      }
      if (action.id === 'preview-upstream-movie') {
        this.previewUpstreamMovie(this.detailItem);
        return;
      }
      if (action.id === 'copy-upstream-movie-url') {
        this.copyMovieUrl(this.detailItem);
      }
    },
    episodeActions(episode) {
      if (this.mode === 'curated' && episode?.id) {
        return [
          {id: 'watch-episode', icon: 'play_arrow', label: 'Watch Episode', color: 'primary', tooltip: 'Watch Episode'},
          {
            id: 'copy-episode-url',
            icon: 'content_copy',
            label: 'Copy Stream URL',
            color: 'grey-8',
            tooltip: 'Copy Stream URL',
          },
        ];
      }
      if (this.mode === 'upstream' && episode?.id) {
        return [
          {
            id: 'preview-upstream-episode',
            icon: 'play_arrow',
            label: 'Preview Stream',
            color: 'primary',
            tooltip: 'Preview Stream',
          },
          {
            id: 'copy-upstream-episode-url',
            icon: 'content_copy',
            label: 'Copy Stream URL',
            color: 'grey-8',
            tooltip: 'Copy Stream URL',
          },
        ];
      }
      return [];
    },
    handleEpisodeAction(action, episode) {
      if (!action?.id || !episode) {
        return;
      }
      if (action.id === 'watch-episode') {
        this.watchEpisode(episode);
        return;
      }
      if (action.id === 'copy-episode-url') {
        this.copyEpisodeUrl(episode);
        return;
      }
      if (action.id === 'preview-upstream-episode') {
        this.previewUpstreamEpisode(episode);
        return;
      }
      if (action.id === 'copy-upstream-episode-url') {
        this.copyEpisodeUrl(episode);
      }
    },
  },
  async mounted() {
    await this.preloadFilterData('movie');
    await this.preloadFilterData('series');
    await this.resetAndReload();
  },
  beforeUnmount() {
    if (this.searchReloadTimer) {
      clearTimeout(this.searchReloadTimer);
      this.searchReloadTimer = null;
    }
  },
};
</script>

<style scoped>
.vod-browser {
  display: flex;
  flex-direction: column;
  flex: 1 1 auto;
  gap: 0;
  min-height: 0;
}

.vod-browser__tabs-bar {
  background: transparent;
}

.vod-browser__tabs {
  background: transparent !important;
}

.vod-browser__tabs :deep(.vod-browser__tabs-content) {
  background: var(--guide-channel-bg);
}

.vod-browser__toolbar {
  padding: 12px 0 8px;
}

.vod-browser__scroll {
  flex: 1 1 auto;
  min-height: 0;
  overflow: auto;
  padding: 6px 12px 8px;
}

.vod-browser__grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(180px, 1fr));
  gap: 14px;
}

@media (max-width: 1023px) {
  .vod-browser__grid {
    grid-template-columns: repeat(auto-fill, minmax(148px, 1fr));
    gap: 10px;
  }
}

.vod-browser__empty {
  min-height: 220px;
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  gap: 8px;
  color: var(--q-grey-7);
  border: 1px dashed var(--q-separator-color);
  border-radius: var(--tic-radius-lg);
  background: var(--tic-list-card-default-bg);
}

@media (max-width: 600px) {
  .vod-browser {
    gap: 0;
  }

  .vod-browser__tabs-bar {
    margin: 0 -12px;
  }

  .vod-browser__tabs {
    width: 100%;
  }

  .vod-browser__tabs :deep(.q-tabs__content) {
    width: 100%;
  }

  .vod-browser__tabs :deep(.q-tab) {
    flex: 1 1 0;
    min-width: 0;
  }

  .vod-browser__toolbar {
    padding: 10px 12px 6px;
  }

  .vod-browser__grid {
    grid-template-columns: repeat(3, minmax(0, 1fr));
    gap: 8px;
  }

  .vod-browser__scroll {
    padding: 6px 12px 8px;
  }

}

.tic-form-layout > *:not(:last-child) {
  margin-bottom: 24px;
}
</style>
