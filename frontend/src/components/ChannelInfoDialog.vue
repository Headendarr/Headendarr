<template>
  <TicDialogWindow
    v-model="isOpen"
    title="Channel Settings"
    width="900px"
    :persistent="isDirty"
    :prevent-close="isDirty"
    :close-tooltip="closeTooltip"
    :actions="dialogActions"
    @action="onDialogAction"
    @close-request="onCloseRequest"
    @hide="onDialogHide"
  >
    <div class="q-pa-lg q-gutter-md">
      <q-form class="tic-form-layout" @submit.prevent="save">
        <template v-if="loading">
          <q-skeleton type="QToggle" />
          <q-skeleton type="QInput" />
          <q-skeleton type="QInput" />
        </template>

        <template v-else>
          <TicToggleInput
            v-model="enabled"
            label="Enabled"
            description="Enable this channel for TVH publish and guide updates."
          />

          <q-separator />

          <h5 class="q-my-none">Channel Details</h5>

          <TicTextInput
            v-model="number"
            label="Channel Number"
            description="To change channel numbers, go to the channel list. You can drag channels to reorder them, or click a channel number to type an exact value."
            readonly
          />

          <TicTextInput
            v-model="name"
            label="Channel Name"
            description="Display name for this channel across TIC and TVH."
          />

          <TicTextareaInput
            v-model="logoUrl"
            label="Logo URL"
            description="Remote logo URL to cache and serve from TIC."
            :rows="2"
            :autogrow="true"
          />

          <div>
            <q-select
              v-model="tags"
              outlined
              use-input
              use-chips
              multiple
              hide-dropdown-icon
              input-debounce="0"
              new-value-mode="add-unique"
              label="Categories"
            />
            <div class="tic-input-description text-caption text-grey-7">
              Categories/tags to apply in TVH.
            </div>
          </div>

          <q-separator />

          <h5 class="q-my-none">Programme Guide</h5>

          <TicSelectInput
            v-model="epgSourceId"
            :options="epgSourceOptions"
            option-label="label"
            option-value="value"
            :emit-value="true"
            :map-options="true"
            :clearable="false"
            :behavior="$q.screen.lt.md ? 'dialog' : 'menu'"
            label="EPG Source"
            description="Select which EPG source this channel maps to."
            @filter="filterEpgSource"
          />

          <TicSelectInput
            v-model="epgChannel"
            :options="epgChannelOptions"
            option-label="label"
            option-value="value"
            :emit-value="true"
            :map-options="true"
            :clearable="false"
            :behavior="$q.screen.lt.md ? 'dialog' : 'menu'"
            label="EPG Channel"
            description="Select the guide channel from the selected EPG source."
            @filter="filterEpg"
          />

          <q-separator />

          <h5 class="q-my-none">Channel Streams</h5>

          <q-list bordered separator class="rounded-borders">
            <draggable
              v-model="listOfChannelSources"
              group="channels"
              item-key="local_key"
              handle=".handle"
              :component-data="{tag: 'ul', name: 'flip-list', type: 'transition'}"
              v-bind="dragOptions"
            >
              <template #item="{element, index}">
                <q-item :key="`source-${element.local_key || index}`" class="q-px-none rounded-borders">
                  <q-item-section avatar class="handle q-px-sm q-mx-sm">
                    <q-avatar rounded>
                      <q-icon name="format_line_spacing" style="max-width: 30px; cursor: grab">
                        <q-tooltip class="bg-white text-primary">Drag to set stream priority</q-tooltip>
                      </q-icon>
                    </q-avatar>
                  </q-item-section>

                  <q-separator inset vertical class="gt-xs" />

                  <q-item-section side class="q-px-sm q-mx-sm" style="max-width: 60px">
                    <q-item-label lines="1" class="text-left">
                      <span class="text-weight-medium">{{ index + 1 }}</span>
                    </q-item-label>
                  </q-item-section>

                  <q-separator inset vertical class="gt-xs" />

                  <q-item-section top class="q-mx-md">
                    <q-item-label lines="1" class="text-left">
                      <span class="text-weight-medium">{{ element.stream_name }}</span>
                    </q-item-label>
                    <q-item-label caption lines="1" class="text-left">
                      {{ element.playlist_name }}
                    </q-item-label>

                    <div v-if="element.source_type === 'manual' || !element.playlist_id" class="sub-setting q-mt-sm">
                      <TicTextInput
                        v-model="element.stream_url"
                        label="Stream URL"
                        description="Manual stream URL for this source."
                      />
                      <TicToggleInput
                        v-model="element.use_hls_proxy"
                        label="Use HLS Proxy"
                        description="Route this stream through TIC HLS proxy."
                      />
                    </div>

                    <div
                      v-if='refreshHint && element.source_type !== "manual" && element.playlist_id'
                      class="text-italic text-caption text-primary q-mt-xs"
                    >
                      {{ refreshHint }}
                    </div>
                  </q-item-section>

                  <q-item-section side class="q-mr-md">
                    <TicListActions
                      :actions="getSourceActions(element, index)"
                      @action="onSourceAction"
                    />
                  </q-item-section>
                </q-item>
              </template>
            </draggable>
          </q-list>

          <div class="row q-gutter-sm justify-end">
            <TicButton
              icon="add"
              label="Add Stream"
              color="primary"
              @click="selectChannelSourceFromList"
            />
            <TicButton
              icon="add_link"
              label="Add Manual URL"
              color="primary"
              variant="outline"
              @click="addManualChannelSource"
            />
          </div>

          <template v-if="suggestedStreams && suggestedStreams.length">
            <q-separator />

            <h5 class="q-my-none">Suggested Streams</h5>

            <q-list bordered separator class="rounded-borders">
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
                  <TicListActions
                    :actions="getSuggestedActions(suggestion)"
                    @action="onSuggestedAction"
                  />
                </q-item-section>
              </q-item>
            </q-list>
          </template>
        </template>
      </q-form>
    </div>
  </TicDialogWindow>
</template>

<script>
import axios from 'axios';
import {copyToClipboard} from 'quasar';
import draggable from 'vuedraggable';
import {useVideoStore} from 'stores/video';
import ChannelStreamSelectorDialog from 'components/ChannelStreamSelectorDialog.vue';
import TicDialogWindow from 'components/ui/dialogs/TicDialogWindow.vue';
import TicConfirmDialog from 'components/ui/dialogs/TicConfirmDialog.vue';
import TicButton from 'components/ui/buttons/TicButton.vue';
import TicListActions from 'components/ui/buttons/TicListActions.vue';
import TicTextInput from 'components/ui/inputs/TicTextInput.vue';
import TicTextareaInput from 'components/ui/inputs/TicTextareaInput.vue';
import TicToggleInput from 'components/ui/inputs/TicToggleInput.vue';
import TicSelectInput from 'components/ui/inputs/TicSelectInput.vue';

export default {
  name: 'ChannelInfoDialog',
  components: {
    draggable,
    TicDialogWindow,
    TicButton,
    TicListActions,
    TicTextInput,
    TicTextareaInput,
    TicToggleInput,
    TicSelectInput,
  },
  props: {
    channelId: {
      type: [String, Number],
      default: null,
    },
    newChannelNumber: {
      type: Number,
      default: null,
    },
  },
  emits: ['ok', 'hide'],
  data() {
    return {
      isOpen: false,
      loading: false,
      saving: false,
      hasSavedInSession: false,
      initialStateSignature: '',
      enabled: true,
      number: 0,
      name: '',
      logoUrl: '',
      tags: [],
      epgSourceOptions: [],
      epgSourceDefaultOptions: [],
      epgSourceId: null,
      epgSourceName: '',
      epgChannelAllOptions: {},
      epgChannelDefaultOptions: [],
      epgChannelOptions: [],
      epgChannel: '',
      listOfPlaylists: [],
      listOfChannelSources: [],
      listOfChannelSourcesToRefresh: [],
      refreshHint: '',
      suggestedStreams: [],
      nextSourceKey: 1,
    };
  },
  computed: {
    isDirty() {
      if (!this.initialStateSignature) {
        return false;
      }
      return this.currentStateSignature() !== this.initialStateSignature;
    },
    closeTooltip() {
      return this.isDirty ? 'Unsaved changes. Save before closing or discard changes.' : 'Close';
    },
    dialogActions() {
      const actions = [
        {
          id: 'save',
          icon: 'save',
          label: 'Save',
          color: 'positive',
          disable: this.loading || this.saving,
          class: this.isDirty ? 'save-action-pulse' : '',
          tooltip: this.isDirty ? 'Save changes' : 'No unsaved changes',
        },
      ];
      if (this.channelId) {
        actions.push({
          id: 'delete',
          icon: 'delete',
          label: 'Delete',
          color: 'negative',
          disable: this.loading || this.saving,
          tooltip: 'Delete channel',
        });
      }
      return actions;
    },
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
    epgSourceId() {
      if (this.epgChannelAllOptions) {
        this.updateCurrentEpgChannelOptions();
      }
    },
  },
  methods: {
    show() {
      this.isOpen = true;
      this.loading = true;
      this.saving = false;
      this.hasSavedInSession = false;

      Promise.all([this.fetchEpgData(), this.fetchPlaylistData()]).then(() => {
        if (this.channelId) {
          return this.fetchData();
        }
        this.applyDefaultState();
        return Promise.resolve();
      }).finally(() => {
        this.captureInitialState();
        this.loading = false;
      });
    },
    hide() {
      this.isOpen = false;
    },
    onDialogHide() {
      this.$emit('ok', {saved: this.hasSavedInSession});
      this.$emit('hide');
    },
    onDialogAction(action) {
      if (action.id === 'save') {
        this.save();
        return;
      }
      if (action.id === 'delete') {
        this.deleteChannel();
      }
    },
    onCloseRequest() {
      if (!this.isDirty) {
        this.hide();
        return;
      }
      this.$q.dialog({
        component: TicConfirmDialog,
        componentProps: {
          title: 'Discard Changes?',
          message: 'You have unsaved changes. Close this dialog and discard them?',
          icon: 'warning',
          iconColor: 'warning',
          confirmLabel: 'Discard',
          confirmIcon: 'delete',
          confirmColor: 'negative',
          cancelLabel: 'Keep Editing',
          persistent: true,
        },
      }).onOk(() => {
        this.hide();
      });
    },
    applyDefaultState() {
      this.enabled = true;
      this.number = this.newChannelNumber || 0;
      this.name = '';
      this.logoUrl = '';
      this.tags = [];
      this.epgSourceId = null;
      this.epgSourceName = '';
      this.epgChannel = '';
      this.listOfChannelSources = [];
      this.listOfChannelSourcesToRefresh = [];
      this.refreshHint = '';
      this.suggestedStreams = [];
      this.updateCurrentEpgChannelOptions();
    },
    captureInitialState() {
      this.initialStateSignature = this.currentStateSignature();
    },
    currentStateSignature() {
      return JSON.stringify({
        enabled: this.enabled,
        number: this.number,
        name: this.name,
        logoUrl: this.logoUrl,
        tags: this.tags,
        epgSourceId: this.epgSourceId,
        epgChannel: this.epgChannel,
        sources: (this.listOfChannelSources || []).map((source) => ({
          source_type: source.source_type || 'playlist',
          stream_id: source.stream_id || null,
          playlist_id: source.playlist_id || null,
          stream_name: source.stream_name || '',
          stream_url: source.stream_url || '',
          use_hls_proxy: !!source.use_hls_proxy,
        })),
      });
    },
    withLocalKey(source) {
      const currentKey = source.local_key || source.id || source.stream_id;
      if (currentKey) {
        return {...source, local_key: String(currentKey)};
      }
      const key = `local-${this.nextSourceKey}`;
      this.nextSourceKey += 1;
      return {...source, local_key: key};
    },
    fetchData() {
      return axios({
        method: 'GET',
        url: '/tic-api/channels/settings/' + this.channelId,
      }).then((response) => {
        this.enabled = response.data.data.enabled;
        this.number = response.data.data.number;
        this.name = response.data.data.name;
        this.logoUrl = response.data.data.logo_url;
        this.tags = response.data.data.tags;
        this.epgSourceId = response.data.data.guide.epg_id;
        this.epgSourceName = response.data.data.guide.epg_name;
        this.epgChannel = response.data.data.guide.channel_id;
        this.listOfChannelSources = response.data.data.sources.map((source) => this.withLocalKey({
          ...source,
          use_hls_proxy: !!source.use_hls_proxy,
        })).sort((a, b) => b.priority - a.priority);
        this.listOfChannelSourcesToRefresh = [];
        return this.fetchSuggestions();
      });
    },
    fetchSuggestions() {
      if (!this.channelId) {
        this.suggestedStreams = [];
        return Promise.resolve();
      }
      return axios({
        method: 'GET',
        url: `/tic-api/channels/${this.channelId}/stream-suggestions`,
      }).then((response) => {
        this.suggestedStreams = response.data.data || [];
      }).catch(() => {
        this.suggestedStreams = [];
      });
    },
    addSuggestedStream(suggestion) {
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
      this.listOfChannelSources.push(this.withLocalKey({
        stream_id: suggestion.stream_id,
        playlist_id: suggestion.playlist_id,
        playlist_name: suggestion.playlist_name || 'Playlist',
        priority: 0,
        stream_name: suggestion.stream_name,
        stream_url: suggestion.stream_url,
        use_hls_proxy: false,
        source_type: 'playlist',
        xc_account_id: null,
      }));
      this.suggestedStreams = this.suggestedStreams.filter((item) => item.id !== suggestion.id);
      this.dismissSuggestedStream(suggestion, {silent: true, skipLocalRemove: true});
    },
    dismissSuggestedStream(suggestion, options = {}) {
      if (!suggestion) {
        return;
      }
      const dismiss = () => axios({
        method: 'POST',
        url: `/tic-api/channels/${this.channelId}/stream-suggestions/${suggestion.id}/dismiss`,
      }).then(() => {
        if (!options.skipLocalRemove) {
          this.suggestedStreams = this.suggestedStreams.filter((item) => item.id !== suggestion.id);
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
        component: TicConfirmDialog,
        componentProps: {
          title: 'Dismiss Stream Suggestion?',
          message: 'This will permanently hide this suggestion for this channel. This cannot be undone.',
          icon: 'warning',
          iconColor: 'warning',
          confirmLabel: 'Dismiss',
          confirmIcon: 'close',
          confirmColor: 'negative',
          cancelLabel: 'Cancel',
          persistent: true,
        },
      }).onOk(() => dismiss());
    },
    getSourceActions(stream, index) {
      const actions = [
        {
          id: 'preview',
          icon: 'play_arrow',
          label: 'Preview stream',
          color: 'primary',
          payload: {stream, index},
        },
        {
          id: 'copy-url',
          icon: 'link',
          label: 'Copy stream URL',
          color: 'primary',
          payload: {stream, index},
        },
      ];
      if (stream.source_type !== 'manual' && stream.playlist_id) {
        actions.push({
          id: 'refresh',
          icon: 'refresh',
          label: 'Refresh stream',
          color: 'primary',
          payload: {stream, index},
        });
      }
      actions.push({
        id: 'remove',
        icon: 'delete',
        label: 'Remove stream',
        color: 'negative',
        payload: {stream, index},
      });
      return actions;
    },
    onSourceAction(action) {
      const payload = action.payload || {};
      if (action.id === 'preview') {
        this.previewChannelStream(payload.stream, {useChannelSource: true});
      } else if (action.id === 'copy-url') {
        this.copyChannelStreamUrl(payload.stream, {useChannelSource: true});
      } else if (action.id === 'refresh') {
        this.refreshChannelSourceFromPlaylist(payload.index);
      } else if (action.id === 'remove') {
        this.removeChannelSourceFromList(payload.index);
      }
    },
    getSuggestedActions(suggestion) {
      return [
        {
          id: 'preview',
          icon: 'play_arrow',
          label: 'Preview stream',
          color: 'primary',
          payload: {suggestion},
        },
        {
          id: 'copy-url',
          icon: 'link',
          label: 'Copy stream URL',
          color: 'primary',
          payload: {suggestion},
        },
        {
          id: 'add',
          icon: 'add',
          label: 'Add to channel',
          color: 'primary',
          payload: {suggestion},
        },
        {
          id: 'dismiss',
          icon: 'close',
          label: 'Dismiss suggestion',
          color: 'negative',
          payload: {suggestion},
        },
      ];
    },
    onSuggestedAction(action) {
      const suggestion = action.payload?.suggestion;
      if (action.id === 'preview') {
        this.previewChannelStream(suggestion, {usePlaylistStream: true});
      } else if (action.id === 'copy-url') {
        this.copyChannelStreamUrl(suggestion, {usePlaylistStream: true});
      } else if (action.id === 'add') {
        this.addSuggestedStream(suggestion);
      } else if (action.id === 'dismiss') {
        this.dismissSuggestedStream(suggestion);
      }
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
    fetchEpgData() {
      const epgFetch = axios({
        method: 'GET',
        url: '/tic-api/epgs/get',
      }).then((response) => {
        this.epgSourceOptions = (response.data.data || []).map((epg) => ({
          label: epg.name,
          value: epg.id,
        }));
        this.epgSourceDefaultOptions = [...this.epgSourceOptions];
      });

      const epgChannelsFetch = axios({
        method: 'GET',
        url: '/tic-api/epgs/channels',
      }).then((response) => {
        this.epgChannelAllOptions = {};
        for (const epgId in response.data.data) {
          const epgChannels = response.data.data[epgId];
          this.epgChannelAllOptions[epgId] = epgChannels.map((channelInfo) => ({
            label: channelInfo.display_name,
            value: channelInfo.channel_id,
          }));
        }
        this.updateCurrentEpgChannelOptions();
      });

      return Promise.all([epgFetch, epgChannelsFetch]);
    },
    fetchPlaylistData() {
      return axios({
        method: 'GET',
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
    updateCurrentEpgChannelOptions() {
      if (!this.epgSourceId || !this.epgChannelAllOptions) {
        this.epgChannelDefaultOptions = [];
        this.epgChannelOptions = [];
        return;
      }
      const selected = this.epgChannelAllOptions[this.epgSourceId] ||
        this.epgChannelAllOptions[String(this.epgSourceId)] || [];
      this.epgChannelDefaultOptions = selected;
      this.epgChannelOptions = selected;
    },
    buildChannelPayload(refreshSources) {
      const epgInfo = this.epgSourceOptions.find((item) => item.value === this.epgSourceId);
      if (epgInfo) {
        this.epgSourceName = epgInfo.label;
      }
      return {
        enabled: this.enabled,
        name: this.name,
        logo_url: this.logoUrl,
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
    save() {
      if (this.saving) {
        return;
      }
      this.saving = true;

      const url = this.channelId ? `/tic-api/channels/settings/${this.channelId}/save` : '/tic-api/channels/new';
      const data = this.buildChannelPayload(this.listOfChannelSourcesToRefresh);
      axios({
        method: 'POST',
        url,
        data,
      }).then(() => {
        this.hasSavedInSession = true;
        this.captureInitialState();
        this.refreshHint = '';
        this.$q.notify({
          color: 'positive',
          icon: 'cloud_done',
          message: 'Saved',
          timeout: 300,
        });
        this.hide();
      }).catch(() => {
        this.$q.notify({
          color: 'negative',
          position: 'top',
          message: 'Failed to save settings',
          icon: 'report_problem',
          actions: [{icon: 'close', color: 'white'}],
        });
      }).finally(() => {
        this.saving = false;
      });
    },
    deleteChannel() {
      if (!this.channelId) {
        return;
      }
      this.$q.dialog({
        component: TicConfirmDialog,
        componentProps: {
          title: 'Delete Channel?',
          message: 'Deleting this channel is final and cannot be undone.',
          icon: 'warning',
          iconColor: 'warning',
          confirmLabel: 'Delete',
          confirmIcon: 'delete',
          confirmColor: 'negative',
          cancelLabel: 'Cancel',
          persistent: true,
        },
      }).onOk(() => {
        this.deleteChannelConfirmed();
      });
    },
    deleteChannelConfirmed() {
      axios({
        method: 'DELETE',
        url: `/tic-api/channels/settings/${this.channelId}/delete`,
      }).then(() => {
        this.hasSavedInSession = true;
        this.$q.notify({
          color: 'positive',
          icon: 'cloud_done',
          message: 'Channel successfully deleted',
          timeout: 300,
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
    filterEpg(value, update) {
      if (value === '') {
        update(() => {
          this.epgChannelOptions = this.epgChannelDefaultOptions;
        });
        return;
      }

      update(() => {
        const needle = String(value).toLowerCase();
        this.epgChannelOptions = this.epgChannelDefaultOptions.filter((option) => {
          return String(option.label || '').toLowerCase().includes(needle);
        });
      });
    },
    filterEpgSource(value, update) {
      if (value === '') {
        update(() => {
          this.epgSourceOptions = this.epgSourceDefaultOptions;
        });
        return;
      }

      update(() => {
        const needle = String(value).toLowerCase();
        this.epgSourceOptions = this.epgSourceDefaultOptions.filter((option) => {
          return String(option.label || '').toLowerCase().includes(needle);
        });
      });
    },
    selectChannelSourceFromList() {
      this.$q.dialog({
        component: ChannelStreamSelectorDialog,
        componentProps: {
          hideStreams: [],
        },
      }).onOk((payload) => {
        if (!payload?.selectedStreams) {
          return;
        }
        const enabledStreams = structuredClone(this.listOfChannelSources || []);
        for (const selectedStream of payload.selectedStreams) {
          const foundItem = enabledStreams.find((item) => {
            if (item.playlist_id !== selectedStream.playlist_id) {
              return false;
            }
            if (selectedStream.stream_url && item.stream_url) {
              return item.stream_url === selectedStream.stream_url;
            }
            return item.stream_name === selectedStream.stream_name;
          });
          if (foundItem) {
            continue;
          }
          const playlistDetails = this.listOfPlaylists.find((item) => {
            return parseInt(item.id, 10) === parseInt(selectedStream.playlist_id, 10);
          });
          enabledStreams.push(this.withLocalKey({
            source_type: 'playlist',
            stream_id: selectedStream.id,
            playlist_id: selectedStream.playlist_id,
            playlist_name: playlistDetails?.name || 'Playlist',
            stream_name: selectedStream.stream_name,
            stream_url: selectedStream.stream_url,
            use_hls_proxy: false,
          }));
        }
        this.listOfChannelSources = enabledStreams;
      });
    },
    refreshChannelSourceFromPlaylist(index) {
      if (!this.listOfChannelSources[index]?.playlist_id) {
        return;
      }
      const refreshSources = structuredClone(this.listOfChannelSourcesToRefresh || []);
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
          timeout: 300,
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
    removeChannelSourceFromList(index) {
      this.listOfChannelSources.splice(index, 1);
    },
    addManualChannelSource() {
      const enabledStreams = structuredClone(this.listOfChannelSources || []);
      enabledStreams.push(this.withLocalKey({
        source_type: 'manual',
        playlist_id: null,
        playlist_name: 'Manual URL',
        stream_name: 'Manual URL',
        stream_url: '',
        use_hls_proxy: false,
      }));
      this.listOfChannelSources = enabledStreams;
    },
  },
  setup() {
    const videoStore = useVideoStore();
    return {videoStore};
  },
};
</script>

<style scoped>
:deep(.save-action-pulse) {
  animation: savePulse 1.2s ease-in-out infinite;
}

@keyframes savePulse {
  0% {
    transform: scale(1);
  }
  50% {
    transform: scale(1.06);
  }
  100% {
    transform: scale(1);
  }
}

.tic-form-layout > *:not(:last-child) {
  margin-bottom: 24px;
}

.tic-input-description {
  margin-top: 0;
  margin-left: 8px;
}
</style>
