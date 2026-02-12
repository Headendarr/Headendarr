<template>
  <q-dialog ref="channelSuggestionsDialogRef" @hide="onDialogHide">
    <q-card style="min-width: 520px; max-width: 720px;">
      <q-card-section class="bg-card-head">
        <div class="row items-center no-wrap">
          <div class="col">
            <div class="text-h6 text-blue-10">Stream Suggestions</div>
            <div class="text-caption text-grey-7">
              Potential matches were found for this channel. Review and add them below, or open the channel to edit
              streams manually.
            </div>
          </div>
          <div class="col-auto">
            <q-btn dense round flat icon="close" v-close-popup />
          </div>
        </div>
      </q-card-section>

      <q-separator />

      <q-card-section>
        <div class="q-gutter-sm">
          <q-btn
            color="primary"
            outline
            label="Open Channel Settings"
            @click="openChannelSettings"
          />
        </div>
      </q-card-section>

      <q-separator />

      <q-card-section>
        <div v-if="loading" class="row items-center q-gutter-sm">
          <q-spinner size="24px" color="primary" />
          <div>Loading suggestions...</div>
        </div>
        <div v-else-if="!suggestions.length" class="text-grey-7">
          No suggestions available for this channel.
        </div>
        <q-list v-else bordered separator class="rounded-borders">
          <q-item v-for="suggestion in suggestions" :key="suggestion.id">
            <q-item-section>
              <q-item-label lines="1">
                <span class="text-weight-medium">{{ suggestion.stream_name }}</span>
              </q-item-label>
              <q-item-label caption lines="1">
                <span class="text-weight-medium text-primary">Group:</span>
                {{ suggestion.group_title || 'Unknown group' }}
                <span class="q-mx-xs">•</span>
                <span class="text-weight-medium text-primary">Source:</span>
                {{ suggestion.playlist_name }}
              </q-item-label>
            </q-item-section>
            <q-item-section side>
              <div v-if="$q.screen.lt.sm" class="text-grey-8">
                <q-btn size="12px" flat dense round color="primary" icon="more_vert">
                  <q-tooltip class="bg-white text-primary">Suggestion actions</q-tooltip>
                  <q-menu anchor="bottom right" self="top right">
                    <q-list dense>
                      <q-item clickable v-close-popup @click="previewChannelStream(suggestion, {usePlaylistStream: true})">
                        <q-item-section avatar>
                          <q-icon name="play_arrow" color="primary" />
                        </q-item-section>
                        <q-item-section>Preview stream</q-item-section>
                      </q-item>
                      <q-item clickable v-close-popup @click="copyChannelStreamUrl(suggestion, {usePlaylistStream: true})">
                        <q-item-section avatar>
                          <q-icon name="link" color="primary" />
                        </q-item-section>
                        <q-item-section>Copy stream URL</q-item-section>
                      </q-item>
                      <q-item clickable v-close-popup @click="addSuggestedStream(suggestion)">
                        <q-item-section avatar>
                          <q-icon name="add" color="primary" />
                        </q-item-section>
                        <q-item-section>Add to channel</q-item-section>
                      </q-item>
                      <q-separator />
                      <q-item clickable v-close-popup @click="dismissSuggestedStream(suggestion)">
                        <q-item-section avatar>
                          <q-icon name="close" color="grey-7" />
                        </q-item-section>
                        <q-item-section>Dismiss suggestion</q-item-section>
                      </q-item>
                    </q-list>
                  </q-menu>
                </q-btn>
              </div>
              <div v-else class="text-grey-8 q-gutter-xs">
                <q-btn size="12px" flat dense round color="primary" icon="play_arrow"
                       @click.stop="previewChannelStream(suggestion, {usePlaylistStream: true})">
                  <q-tooltip class="bg-white text-primary">Preview stream</q-tooltip>
                </q-btn>
                <q-btn size="12px" flat dense round color="primary" icon="link"
                       @click.stop="copyChannelStreamUrl(suggestion, {usePlaylistStream: true})">
                  <q-tooltip class="bg-white text-primary">Copy stream URL</q-tooltip>
                </q-btn>
                <q-btn size="12px" flat dense round color="primary" icon="add"
                       @click="addSuggestedStream(suggestion)">
                  <q-tooltip class="bg-white text-primary">Add to channel</q-tooltip>
                </q-btn>
                <q-btn size="12px" flat dense round color="grey-7" icon="close"
                       @click="dismissSuggestedStream(suggestion)">
                  <q-tooltip class="bg-white text-primary">Dismiss suggestion</q-tooltip>
                </q-btn>
              </div>
            </q-item-section>
          </q-item>
        </q-list>
      </q-card-section>
    </q-card>
  </q-dialog>
</template>

<script>
import {ref} from 'vue';
import axios from 'axios';
import {copyToClipboard} from 'quasar';
import {useVideoStore} from 'stores/video';

export default {
  name: 'ChannelSuggestionsDialog',
  props: {
    channelId: {
      type: Number,
      required: true,
    },
  },
  emits: ['ok', 'hide'],
  data() {
    return {
      loading: ref(false),
      channelConfig: ref(null),
      suggestions: ref([]),
      didEmitOk: false,
    };
  },
  methods: {
    show() {
      this.didEmitOk = false;
      this.$refs.channelSuggestionsDialogRef.show();
      this.fetchData();
    },
    hide() {
      this.$refs.channelSuggestionsDialogRef.hide();
    },
    onDialogHide() {
      if (!this.didEmitOk) {
        this.$emit('ok', {refresh: true});
      }
      this.didEmitOk = false;
      this.$emit('hide');
    },
    fetchData() {
      if (!this.channelId) return;
      this.loading = true;
      axios({
        method: 'GET',
        url: `/tic-api/channels/settings/${this.channelId}`,
      }).then((response) => {
        this.channelConfig = response.data.data;
        return axios({
          method: 'GET',
          url: `/tic-api/channels/${this.channelId}/stream-suggestions`,
        });
      }).then((response) => {
        this.suggestions = response.data.data || [];
      }).catch(() => {
        this.$q.notify({color: 'negative', message: 'Failed to load suggestions'});
      }).finally(() => {
        this.loading = false;
      });
    },
    openChannelSettings() {
      this.didEmitOk = true;
      this.$emit('ok', {openSettings: true, channelId: this.channelId});
      this.hide();
    },
    normalizeStreamUrl(streamUrl) {
      if (!streamUrl) {
        return streamUrl;
      }
      if (streamUrl.includes('__TIC_HOST__')) {
        return streamUrl.replace('__TIC_HOST__', window.location.origin);
      }
      return streamUrl;
    },
    async previewChannelStream(stream, options = {}) {
      if (options.usePlaylistStream && stream?.stream_id) {
        try {
          const response = await axios.get(`/tic-api/playlists/streams/${stream.stream_id}/preview`);
          if (response.data.success) {
            this.videoStore.showPlayer({
              url: response.data.preview_url,
              title: stream?.stream_name || 'Stream',
              type: response.data.stream_type || 'auto',
            });
            return;
          }
          this.$q.notify({color: 'negative', message: response.data.message || 'Failed to load preview'});
          return;
        } catch (error) {
          console.error('Preview stream error:', error);
          this.$q.notify({color: 'negative', message: 'Failed to load preview'});
          return;
        }
      }
      const url = this.normalizeStreamUrl(stream?.stream_url);
      if (!url) {
        this.$q.notify({color: 'negative', message: 'Stream URL missing'});
        return;
      }
      this.videoStore.showPlayer({
        url,
        title: stream?.stream_name || 'Stream',
        type: url.toLowerCase().includes('.m3u8') ? 'hls' : 'mpegts',
      });
    },
    async copyChannelStreamUrl(stream, options = {}) {
      if (options.usePlaylistStream && stream?.stream_id) {
        try {
          const response = await axios.get(`/tic-api/playlists/streams/${stream.stream_id}/preview`);
          if (response.data.success) {
            await copyToClipboard(response.data.preview_url);
            this.$q.notify({color: 'positive', message: 'Stream URL copied'});
            return;
          }
          this.$q.notify({color: 'negative', message: response.data.message || 'Failed to copy stream URL'});
          return;
        } catch (error) {
          console.error('Copy stream URL error:', error);
          this.$q.notify({color: 'negative', message: 'Failed to copy stream URL'});
          return;
        }
      }
      const url = this.normalizeStreamUrl(stream?.stream_url);
      if (!url) {
        this.$q.notify({color: 'negative', message: 'Stream URL missing'});
        return;
      }
      await copyToClipboard(url);
      this.$q.notify({color: 'positive', message: 'Stream URL copied'});
    },
    addSuggestedStream(suggestion) {
      if (!this.channelConfig) return;
      const sources = this.channelConfig.sources || [];
      const exists = sources.some((source) => {
        if (source.playlist_id !== suggestion.playlist_id) {
          return false;
        }
        if (suggestion.stream_url && source.stream_url) {
          return source.stream_url === suggestion.stream_url;
        }
        return source.stream_name === suggestion.stream_name;
      });
      if (exists) {
        this.$q.notify({color: 'warning', message: 'Stream already added'});
        return;
      }
      sources.push({
        stream_id: suggestion.stream_id,
        playlist_id: suggestion.playlist_id,
        playlist_name: suggestion.playlist_name || 'Playlist',
        priority: 0,
        stream_name: suggestion.stream_name,
        stream_url: suggestion.stream_url,
        use_hls_proxy: false,
        source_type: 'playlist',
        xc_account_id: null,
      });
      const payload = {
        enabled: this.channelConfig.enabled,
        name: this.channelConfig.name,
        logo_url: this.channelConfig.logo_url,
        number: this.channelConfig.number,
        tags: this.channelConfig.tags || [],
        guide: this.channelConfig.guide || {},
        sources,
        refresh_sources: [],
      };
      axios({
        method: 'POST',
        url: `/tic-api/channels/settings/${this.channelId}/save`,
        data: payload,
      }).then(() => {
        this.channelConfig.sources = sources;
        this.suggestions = this.suggestions.filter(item => item.id !== suggestion.id);
        return this.dismissSuggestedStream(suggestion, {silent: true, skipLocalRemove: true});
      }).then(() => {
        this.$q.notify({color: 'positive', message: 'Stream added'});
      }).catch(() => {
        this.$q.notify({color: 'negative', message: 'Failed to add stream'});
      });
    },
    dismissSuggestedStream(suggestion, options = {}) {
      if (!suggestion) return Promise.resolve();
      const dismiss = () => axios({
        method: 'POST',
        url: `/tic-api/channels/${this.channelId}/stream-suggestions/${suggestion.id}/dismiss`,
      }).then(() => {
        if (!options.skipLocalRemove) {
          this.suggestions = this.suggestions.filter(item => item.id !== suggestion.id);
        }
        if (!options.silent) {
          this.$q.notify({color: 'positive', message: 'Suggestion dismissed'});
        }
      }).catch(() => {
        if (!options.silent) {
          this.$q.notify({color: 'negative', message: 'Failed to dismiss suggestion'});
        }
      });

      if (options.silent) {
        return dismiss();
      }

      return this.$q.dialog({
        title: 'Dismiss Stream Suggestion?',
        message: 'This will permanently hide this suggestion for this channel. This cannot be undone.',
        cancel: true,
        persistent: true,
      }).onOk(() => dismiss());
    },
  },
  setup() {
    const videoStore = useVideoStore();
    return {videoStore};
  },
};
</script>
