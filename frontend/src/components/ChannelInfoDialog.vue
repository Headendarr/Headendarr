<template>
  <!--
    TODO:
      - Configure mobile view such that the form elements on the settings tab are not padded
      - Fix header wrapping on mobile view
    -->

  <!-- START DIALOG CONFIG
  Right fullscreen pop-up
  Note: Update template q-dialog ref="" value

  All Platforms:
   - Swipe right to dismiss
  Desktop:
   - Width 700px
   - Minimise button top-right
  Mobile:
   - Full screen
   - Back button top-left
  -->
  <q-dialog
    ref="channelInfoDialogRef"
    :maximized="$q.platform.is.mobile"
    :transition-show="$q.platform.is.mobile ? 'jump-left' : 'slide-left'"
    :transition-hide="$q.platform.is.mobile ? 'jump-right' : 'slide-right'"
    full-height
    position="right"
    @before-hide="beforeDialogHide"
    @hide="onDialogHide">

    <q-card
      v-touch-swipe.touch.right="hide"
      :style="$q.platform.is.mobile ? 'max-width: 100vw;' : 'max-width: 95vw;'"
      style="width:700px;">

      <q-card-section class="bg-card-head">
        <div class="row items-center no-wrap">
          <div
            v-if="$q.platform.is.mobile"
            class="col">
            <q-btn
              color="grey-7"
              dense
              round
              flat
              icon="arrow_back"
              v-close-popup>
            </q-btn>
          </div>

          <div class="col">
            <div class="text-h6 text-blue-10">
              Channel Settings
            </div>
          </div>

          <div
            v-if="!$q.platform.is.mobile"
            class="col-auto">
            <q-btn
              color="grey-7"
              dense
              round
              flat
              icon="arrow_forward"
              v-close-popup>
              <q-tooltip class="bg-white text-primary">Close</q-tooltip>
            </q-btn>
          </div>
        </div>
      </q-card-section>
      <!-- END DIALOG CONFIG -->

      <q-separator />

      <div class="row">
        <div class="col col-12 q-pa-lg">
          <div>
            <q-form
              @submit="save"
              class="q-gutter-md"
            >
              <div class="q-gutter-sm">
                <q-skeleton
                  v-if="enabled === null"
                  type="QCheckbox" />
                <q-toggle v-model="enabled" label="Enabled" />
              </div>

              <q-separator class="q-my-lg" />

              <!--START DETAILS CONFIG-->
              <h5 class="q-mb-none">Channel Details:</h5>
              <div class="q-gutter-sm">
                <q-skeleton
                  v-if="number === null"
                  type="QInput" />
                <q-input
                  v-else
                  v-model="number"
                  readonly
                  label="Channel Number (edit from channel list)"
                />
              </div>
              <div class="q-gutter-sm">
                <q-skeleton
                  v-if="name === null"
                  type="QInput" />
                <q-input
                  v-else
                  v-model="name"
                  label="Channel Name"
                />
              </div>
              <div class="q-gutter-sm">
                <q-skeleton
                  v-if="logoUrl === null"
                  type="QInput" />
                <q-input
                  v-else
                  v-model="logoUrl"
                  type="textarea"
                  label="Logo URL"
                />
              </div>
              <div class="q-gutter-sm">
                <q-skeleton
                  v-if="tags === null"
                  type="QInput" />
                <q-select
                  v-else
                  use-input
                  use-chips
                  multiple
                  hide-dropdown-icon
                  input-debounce="0"
                  new-value-mode="add-unique"
                  v-model="tags"
                  label="Categories"
                  @keyup.tab="addTag"
                />
              </div>
              <!--END DETAILS CONFIG-->

              <q-separator class="q-my-lg" />

              <!--START EPG CONFIG-->
              <h5 class="q-mb-none">Programme Guide:</h5>
              <div class="q-gutter-sm">
                <q-skeleton
                  v-if="epgSourceOptions === null"
                  type="QInput" />
                <q-select
                  v-else
                  v-model="epgSourceId"
                  :options="epgSourceOptions"
                  use-input
                  input-debounce="0"
                  emit-value
                  map-options
                  label="EPG Source"
                  @filter="filterEpgSource"
                  @input="updateCurrentEpgChannelOptions"
                />
              </div>
              <div class="q-gutter-sm">
                <q-skeleton
                  v-if="epgSourceId === null || epgChannelOptions === null"
                  type="QInput" />
                <q-select
                  v-else
                  label="EPG Channel"
                  v-model="epgChannel"
                  :options="epgChannelOptions"
                  emit-value
                  map-options
                  use-input
                  input-debounce="0"
                  @filter="filterEpg"
                  behavior="menu"
                  @input="updateCurrentEpgChannelOptions"
                />
              </div>
              <!--END EPG CONFIG-->

              <q-separator class="q-my-lg" />

              <!--START SOURCES CONFIG-->
              <h5 class="q-mb-none">Channel Streams:</h5>
              <div class="q-gutter-sm">
                <q-list
                  bordered
                  separator
                  class="rounded-borders q-pl-none"
                  style="">

                  <draggable
                    group="channels"
                    item-key="number"
                    handle=".handle"
                    :component-data="{ tag: 'ul', name: 'flip-list', type: 'transition' }"
                    v-model="listOfChannelSources"
                    v-bind="dragOptions"
                  >
                    <template #item="{ element, index }">
                      <q-item
                        :key="index"
                        class="q-px-none rounded-borders"
                        :class="{'q-py-xs': $q.screen.lt.sm}"
                        active-class="">

                        <!--START DRAGGABLE HANDLE-->
                        <q-item-section
                          avatar
                          class="handle"
                          :class="{
                            'q-px-xs q-mx-xs': $q.screen.lt.sm,
                            'q-px-sm q-mx-sm': !$q.screen.lt.sm
                          }">
                          <q-avatar rounded>
                            <q-icon name="drag_handle" class="" style="max-width: 30px;">
                              <q-tooltip class="bg-white text-primary">Drag to move and set priority</q-tooltip>
                            </q-icon>
                          </q-avatar>
                        </q-item-section>
                        <!--END DRAGGABLE HANDLE-->

                        <q-separator inset vertical class="gt-xs" />

                        <!--START CHANNEL NUMBER-->
                        <q-item-section
                          side
                          :class="{
                            'q-px-xs q-mx-xs': $q.screen.lt.sm,
                            'q-px-sm q-mx-sm': !$q.screen.lt.sm
                          }"
                          style="max-width: 60px;">
                          <q-item-label lines="1" class="text-left">
                            <span class="text-weight-medium">{{ index + 1 }}</span>
                            <q-tooltip
                              anchor="bottom middle" self="center middle"
                              class="bg-white text-primary">Channel Number (Click to edit)
                            </q-tooltip>
                          </q-item-label>
                        </q-item-section>
                        <!--END CHANNEL NUMBER-->

                        <q-separator inset vertical class="gt-xs" />

                        <!--START NAME / DESCRIPTION-->
                        <q-item-section
                          top
                          :class="{
                            'q-mx-xs': $q.screen.lt.sm,
                            'q-mx-md': !$q.screen.lt.sm
                          }">
                          <q-item-label lines="1" class="text-left">
                            <span class="text-weight-medium q-ml-sm">{{ element.stream_name }}</span>
                          </q-item-label>
                          <q-item-label caption lines="1" class="text-left q-ml-sm">
                            <!--TODO: Limit length of description-->
                            {{ element.playlist_name }}
                          </q-item-label>
                          <q-input
                            v-if="element.source_type === 'manual' || !element.playlist_id"
                            v-model="element.stream_url"
                            dense
                            outlined
                            class="q-mt-sm"
                            label="Stream URL"
                          />
                          <q-toggle
                            v-if="element.source_type === 'manual' || !element.playlist_id"
                            v-model="element.use_hls_proxy"
                            class="q-mt-xs q-ml-sm"
                            label="Use HLS proxy"
                            dense
                          />
                          <div v-if="refreshHint && element.source_type !== 'manual' && element.playlist_id"
                               class="text-italic text-caption text-primary q-mt-xs">
                            {{ refreshHint }}
                          </div>
                        </q-item-section>
                        <!--END NAME / DESCRIPTION-->

                        <q-separator inset vertical class="gt-xs" />

                        <q-item-section side class="q-mr-md">
                          <div v-if="$q.screen.lt.sm" class="text-grey-8">
                            <q-btn size="12px" flat dense round color="primary" icon="more_vert">
                              <q-tooltip class="bg-white text-primary">Stream actions</q-tooltip>
                              <q-menu anchor="bottom right" self="top right">
                                <q-list dense>
                                    <q-item clickable v-close-popup @click="previewChannelStream(element, {useChannelSource: true})">
                                    <q-item-section avatar>
                                      <q-icon name="play_arrow" color="primary" />
                                    </q-item-section>
                                    <q-item-section>Preview stream</q-item-section>
                                  </q-item>
                                  <q-item clickable v-close-popup @click="copyChannelStreamUrl(element, {useChannelSource: true})">
                                    <q-item-section avatar>
                                      <q-icon name="link" color="primary" />
                                    </q-item-section>
                                    <q-item-section>Copy stream URL</q-item-section>
                                  </q-item>
                                  <q-item
                                    v-if="element.source_type !== 'manual' && element.playlist_id"
                                    clickable
                                    v-close-popup
                                    @click="refreshChannelSourceFromPlaylist(index)">
                                    <q-item-section avatar>
                                      <q-icon name="refresh" color="primary" />
                                    </q-item-section>
                                    <q-item-section>Refresh stream</q-item-section>
                                  </q-item>
                                  <q-separator />
                                  <q-item clickable v-close-popup @click="removeChannelSourceFromList(index)">
                                    <q-item-section avatar>
                                      <q-icon name="delete" color="negative" />
                                    </q-item-section>
                                    <q-item-section>Remove stream</q-item-section>
                                  </q-item>
                                </q-list>
                              </q-menu>
                            </q-btn>
                          </div>
                          <div v-else class="text-grey-8 q-gutter-xs">
                            <q-btn size="12px" flat dense round color="primary" icon="play_arrow"
                                   @click.stop="previewChannelStream(element, {useChannelSource: true})">
                              <q-tooltip class="bg-white text-primary">Preview stream</q-tooltip>
                            </q-btn>
                            <q-btn size="12px" flat dense round color="primary" icon="link"
                                   @click.stop="copyChannelStreamUrl(element, {useChannelSource: true})">
                              <q-tooltip class="bg-white text-primary">Copy stream URL</q-tooltip>
                            </q-btn>
                            <q-btn size="12px" flat dense round color="primary" icon="refresh"
                                   v-if="element.source_type !== 'manual' && element.playlist_id"
                                   @click="refreshChannelSourceFromPlaylist(index)">
                              <q-tooltip class="bg-white text-primary">Refresh stream</q-tooltip>
                            </q-btn>
                            <q-btn size="12px" flat dense round color="negative" icon="delete"
                                   @click="removeChannelSourceFromList(index)">
                              <q-tooltip class="bg-white text-primary">Remove this stream</q-tooltip>
                            </q-btn>
                          </div>
                        </q-item-section>

                      </q-item>
                    </template>
                  </draggable>

                </q-list>

                <q-bar class="bg-transparent q-mb-sm">
                  <q-space />
                  <q-btn
                    round
                    flat
                    color="primary"
                    icon="add"
                    @click="selectChannelSourceFromList">
                    <q-tooltip class="bg-white text-primary">Add stream</q-tooltip>
                  </q-btn>
                  <q-btn
                    round
                    flat
                    color="primary"
                    icon="add_link"
                    @click="addManualChannelSource">
                    <q-tooltip class="bg-white text-primary">Add manual stream URL</q-tooltip>
                  </q-btn>
                </q-bar>
              </div>
              <!--END SOURCES CONFIG-->

              <q-separator v-if="suggestedStreams && suggestedStreams.length" class="q-my-lg" />

              <template v-if="suggestedStreams && suggestedStreams.length">
                <h5 class="q-mb-none">Suggested Streams:</h5>
                <div class="q-gutter-sm">
                  <q-list bordered separator class="rounded-borders q-pl-none">
                    <q-item v-for="suggestion in suggestedStreams" :key="suggestion.id">
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
                </div>
              </template>

              <div>
                <q-btn label="Save" type="submit" color="primary" />
                <q-btn
                  @click="deleteChannel()"
                  class="q-ml-md"
                  color="red"
                  label="Delete" />
              </div>

            </q-form>

          </div>
        </div>
      </div>

    </q-card>

  </q-dialog>
</template>

<script>
/*
tab          - The tab to display first ['info', 'settings']
*/

import axios from 'axios';
import {ref} from 'vue';
import draggable from 'vuedraggable';
import {copyToClipboard} from 'quasar';
import {useVideoStore} from 'stores/video';
import ChannelStreamSelectorDialog from 'components/ChannelStreamSelectorDialog.vue';

export default {
  name: 'ChannelInfoDialog',
  components: {draggable},
  props: {
    channelId: {
      type: String,
    },
    newChannelNumber: {
      type: Number,
    },
  },
  emits: [
    // REQUIRED
    'ok', 'hide', 'path',
  ],
  data() {
    return {
      canSave: ref(false),
      enabled: ref(null),
      number: ref(null),
      name: ref(null),
      logoUrl: ref(null),
      tags: ref(null),
      newTag: ref(''),
      epgSourceOptions: ref(null),
      epgSourceDefaultOptions: ref(null),
      epgSourceId: ref(null),
      epgChannelAllOptions: ref(null),
      epgChannelDefaultOptions: ref(null),
      epgChannelOptions: ref(null),
      epgChannel: ref(null),

      listOfChannelSources: ref(null),
      refreshHint: ref(''),
      suggestedStreams: ref([]),

      channelSourceOptions: ref(null),
      channelSourceOptionsFiltered: ref(null),
      channelSources: ref(null),
    };
  },
  methods: {
    // following method is REQUIRED
    // (don't change its name --> "show")
    show() {
      this.$refs.channelInfoDialogRef.show();
      this.fetchEpgData();
      this.fetchPlaylistData();
      if (this.channelId) {
        this.fetchData();
        return;
      }
      this.enabled = true;
      this.number = (this.newChannelNumber ? this.newChannelNumber : 0);
      this.name = '';
      this.logoUrl = '';
      this.tags = [];
      this.epgSourceOptions = [];
      this.epgSourceDefaultOptions = [];
      this.epgSourceId = '';
      this.epgSourceName = '';
      this.epgChannelDefaultOptions = [];
      this.epgChannelOptions = [];
      this.epgChannel = '';

      this.listOfPlaylists = [];
      this.listOfChannelSources = [];
      this.listOfChannelSourcesToRefresh = [];
      this.refreshHint = '';

      this.channelSourceOptions = [];
      this.channelSourceOptionsFiltered = [];
      this.channelSources = [];
      this.suggestedStreams = [];
    },

    // following method is REQUIRED
    // (don't change its name --> "hide")
    hide() {
      this.$refs.channelInfoDialogRef.hide();
    },

    onDialogHide() {
      // required to be emitted
      // when QDialog emits "hide" event
      this.$emit('ok', {});
      this.$emit('hide');
    },

    fetchData: function() {
      // Fetch from server
      axios({
        method: 'GET',
        url: '/tic-api/channels/settings/' + this.channelId,
      }).then((response) => {
        this.enabled = response.data.data.enabled;
        this.number = response.data.data.number;
        this.name = response.data.data.name;
        this.logoUrl = response.data.data.logo_url;
        this.tags = response.data.data.tags;
        // Fetch data for EPG
        this.epgSourceId = response.data.data.guide.epg_id;
        this.epgSourceName = response.data.data.guide.epg_name;
        this.epgChannel = response.data.data.guide.channel_id;
        // Fetch list of channel sources and pipe to a list ordered by the 'priority'
        this.listOfChannelSources = response.data.data.sources.map((source) => ({
          ...source,
          use_hls_proxy: !!source.use_hls_proxy,
        })).sort((a, b) => b.priority - a.priority);
        this.listOfChannelSourcesToRefresh = [];
        this.fetchSuggestions();
        // Enable saving the form
        this.canSave = true;
      });
    },
    fetchSuggestions: function() {
      if (!this.channelId) {
        this.suggestedStreams = [];
        return;
      }
      axios({
        method: 'GET',
        url: `/tic-api/channels/${this.channelId}/stream-suggestions`,
      }).then((response) => {
        this.suggestedStreams = response.data.data || [];
      }).catch(() => {
        this.suggestedStreams = [];
      });
    },
    addSuggestedStream: function(suggestion) {
      if (!suggestion) {
        return;
      }
      const exists = (this.listOfChannelSources || []).some((source) => {
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
      this.listOfChannelSources.push({
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
      this.suggestedStreams = this.suggestedStreams.filter(item => item.id !== suggestion.id);
      this.dismissSuggestedStream(suggestion, {silent: true, skipLocalRemove: true});
    },
    dismissSuggestedStream: function(suggestion, options = {}) {
      if (!suggestion) {
        return;
      }
      const dismiss = () => axios({
        method: 'POST',
        url: `/tic-api/channels/${this.channelId}/stream-suggestions/${suggestion.id}/dismiss`,
      }).then(() => {
        if (!options.skipLocalRemove) {
          this.suggestedStreams = this.suggestedStreams.filter(item => item.id !== suggestion.id);
        }
      }).catch(() => {
        if (!options.silent) {
          this.$q.notify({color: 'negative', message: 'Failed to dismiss suggestion'});
        }
      });

      if (options.silent) {
        dismiss();
        return;
      }

      this.$q.dialog({
        title: 'Dismiss Stream Suggestion?',
        message: 'This will permanently hide this suggestion for this channel. This cannot be undone.',
        cancel: true,
        persistent: true,
      }).onOk(() => dismiss());
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
              title: stream?.stream_name || this.name || 'Stream',
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
      if (options.useChannelSource && this.channelId && stream?.id) {
        try {
          const response = await axios.get(`/tic-api/channels/${this.channelId}/sources/${stream.id}/preview`);
          if (response.data.success) {
            this.videoStore.showPlayer({
              url: response.data.preview_url,
              title: stream?.stream_name || this.name || 'Stream',
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
        title: stream?.stream_name || this.name || 'Stream',
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
      if (options.useChannelSource && this.channelId && stream?.id) {
        try {
          const response = await axios.get(`/tic-api/channels/${this.channelId}/sources/${stream.id}/preview`);
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
    fetchEpgData: function() {
      // Fetch from server
      axios({
        method: 'GET',
        url: '/tic-api/epgs/get',
      }).then((response) => {
        this.epgSourceOptions = [];
        for (let i in response.data.data) {
          let epg = response.data.data[i];
          this.epgSourceOptions.push(
            {
              label: epg.name,
              value: epg.id,
            },
          );
        }
        this.epgSourceDefaultOptions = [...this.epgSourceOptions];
      });
      axios({
        method: 'GET',
        url: '/tic-api/epgs/channels',
      }).then((response) => {
        this.epgChannelAllOptions = {};
        for (let epg_id in response.data.data) {
          let epg_channels = response.data.data[epg_id];
          this.epgChannelAllOptions[epg_id] = [];
          for (let i = 0; i < epg_channels.length; i++) {
            let channel_info = epg_channels[i];
            this.epgChannelAllOptions[epg_id].push(
              {
                label: channel_info.display_name,
                value: channel_info.channel_id,
              },
            );
          }
        }
        this.updateCurrentEpgChannelOptions();
      });
    },
    fetchPlaylistData: function() {
      axios({
        method: 'get',
        url: '/tic-api/playlists/get',
      }).then((response) => {
        this.listOfPlaylists = response.data.data;
      }).catch(() => {
        this.$q.notify({
          color: 'negative',
          position: 'top',
          message: 'Failed to fetch the list of sources',
          icon: 'report_problem',
          actions: [{icon: 'close', color: 'white'}],
        });
      });
    },
    updateCurrentEpgChannelOptions: function() {
      if (this.epgSourceId) {
        this.epgChannelDefaultOptions = this.epgChannelAllOptions[this.epgSourceId];
        this.epgChannelOptions = this.epgChannelAllOptions[this.epgSourceId];
      }
    },
    buildChannelPayload: function(refreshSources) {
      const epgInfo = this.epgSourceOptions.find((item) => item.value === this.epgSourceId);
      if (epgInfo) {
        this.epgSourceName = epgInfo.label;
      }
      return {
        enabled: this.enabled,
        name: this.name,
        logo_url: this.logoUrl,
        connections: this.connections,
        tags: this.tags,
        number: this.newChannelNumber || this.number || 0,
        guide: {
          epg_id: this.epgSourceId,
          epg_name: this.epgSourceName,
          channel_id: this.epgChannel,
        },
        sources: this.listOfChannelSources,
        refresh_sources: refreshSources,
      };
    },
    save: function() {
      const url = this.channelId ? `/tic-api/channels/settings/${this.channelId}/save` : '/tic-api/channels/new';
      const data = this.buildChannelPayload(this.listOfChannelSourcesToRefresh);
      axios({
        method: 'POST',
        url,
        data,
      }).then(() => {
        this.$q.notify({
          color: 'positive',
          icon: 'cloud_done',
          message: 'Saved',
          timeout: 200,
        });
        this.refreshHint = '';
        this.hide();
      }).catch(() => {
        this.$q.notify({
          color: 'negative',
          position: 'top',
          message: 'Failed to save settings',
          icon: 'report_problem',
          actions: [{icon: 'close', color: 'white'}],
        });
      });
    },
    deleteChannel: function() {
      let channelId = this.channelId;
      if (!channelId) {
        console.warn(`No channel ID provided - '${channelId}'`);
        return;
      }
      axios({
        method: 'DELETE',
        url: `/tic-api/channels/settings/${channelId}/delete`,
      }).then((response) => {
        // Save success, show feedback
        this.$q.notify({
          color: 'positive',
          icon: 'cloud_done',
          message: 'Channel successfully deleted',
          timeout: 200,
        });
        this.hide();
      }).catch(() => {
        this.$q.notify({
          color: 'negative',
          position: 'top',
          message: 'Failed to delete channel',
          icon: 'report_problem',
          actions: [{icon: 'close', color: 'white'}],
        });
      });
    },
    addTag: function() {
      if (this.newTag) {
        this.tags[this.tags.length] = this.newTag;
        this.newTag = null;
      }
    },
    filterEpg(val, update) {
      if (val === '') {
        update(() => {
          this.epgChannelOptions = this.epgChannelDefaultOptions;
        });
        return;
      }

      update(() => {
        const needle = String(val).toLowerCase();
        this.epgChannelOptions = this.epgChannelDefaultOptions.filter((v) => {
          return String(v.label || '').toLowerCase().indexOf(needle) > -1;
        });
      });
    },
    filterEpgSource(val, update) {
      if (val === '') {
        update(() => {
          this.epgSourceOptions = this.epgSourceDefaultOptions;
        });
        return;
      }

      update(() => {
        const needle = String(val).toLowerCase();
        this.epgSourceOptions = this.epgSourceDefaultOptions.filter((v) => {
          return String(v.label || '').toLowerCase().indexOf(needle) > -1;
        });
      });
    },
    selectChannelSourceFromList: function() {
      this.$q.dialog({
        component: ChannelStreamSelectorDialog,
        componentProps: {
          hideStreams: [],
        },
      }).onOk((payload) => {
        if (typeof payload.selectedStreams !== 'undefined' && payload.selectedStreams !== null) {
          // Add selected stream to list
          let enabledStreams = structuredClone(this.listOfChannelSources);
          for (const i in payload.selectedStreams) {
            // Check if this sources is already added...
            const foundItem = enabledStreams.find((item) => {
              if (item.playlist_id !== payload.selectedStreams[i].playlist_id) {
                return false;
              }
              if (payload.selectedStreams[i].stream_url && item.stream_url) {
                return item.stream_url === payload.selectedStreams[i].stream_url;
              }
              return item.stream_name === payload.selectedStreams[i].stream_name;
            });
            if (foundItem) {
              // Value already exists
              console.warn('Channel source already exists');
              continue;
            }
            const playlistDetails = this.listOfPlaylists.find((item) => {
              return parseInt(item.id) === parseInt(payload.selectedStreams[i].playlist_id);
            });
            enabledStreams.push({
              source_type: 'playlist',
              stream_id: payload.selectedStreams[i].id,
              playlist_id: payload.selectedStreams[i].playlist_id,
              playlist_name: playlistDetails.name,
              stream_name: payload.selectedStreams[i].stream_name,
              stream_url: payload.selectedStreams[i].stream_url,
              use_hls_proxy: false,
            });
          }
          this.listOfChannelSources = enabledStreams;
          // NOTE: Do not save the current settings here! We want to be able to undo these changes.
        }
      }).onDismiss(() => {
      });
    },
    refreshChannelSourceFromPlaylist: function(index) {
      if (!this.listOfChannelSources[index].playlist_id) {
        return;
      }
      let refreshSources = structuredClone(this.listOfChannelSourcesToRefresh);
      // TODO: Add logic to not add the same thing multiple times
      refreshSources.push({
        playlist_id: this.listOfChannelSources[index].playlist_id,
        playlist_name: this.listOfChannelSources[index].playlist_name,
        stream_name: this.listOfChannelSources[index].stream_name,
      });
      this.listOfChannelSourcesToRefresh = refreshSources;
      if (!this.channelId) {
        return;
      }
      const payload = this.buildChannelPayload(refreshSources);
      axios.post(`/tic-api/channels/settings/${this.channelId}/save`, payload).then(() => {
        this.$q.notify({
          color: 'positive',
          icon: 'refresh',
          message: 'Channel source refreshed',
          timeout: 200,
        });
        this.refreshHint = 'Stream refreshed; click Save to persist.';
      }).catch(() => {
        this.$q.notify({
          color: 'negative',
          position: 'top',
          message: 'Failed to refresh channel source',
          icon: 'report_problem',
          actions: [{icon: 'close', color: 'white'}],
        });
        this.refreshHint = '';
      });
    },
    removeChannelSourceFromList: function(index) {
      this.listOfChannelSources.splice(index, 1);
    },
    addManualChannelSource: function() {
      let enabledStreams = structuredClone(this.listOfChannelSources);
      enabledStreams.push({
        source_type: 'manual',
        playlist_id: null,
        playlist_name: 'Manual URL',
        stream_name: 'Manual URL',
        stream_url: '',
        use_hls_proxy: false,
      });
      this.listOfChannelSources = enabledStreams;
    },
  },
  computed: {
    dragOptions() {
      return {
        animation: 100,
        group: 'channelStreams',
        disabled: false,
        ghostClass: 'ghost',
        direction: 'vertical',
        delay: 200,
        delayOnTouchOnly: true,
      };
    },
  },
  watch: {
    epgSourceId(value) {
      if (value && this.epgChannelAllOptions) {
        this.updateCurrentEpgChannelOptions();
      }
    },
  },
  setup() {
    const videoStore = useVideoStore();
    return {videoStore};
  },
};
</script>

<style>

</style>
