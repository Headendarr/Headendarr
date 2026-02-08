<template>
  <q-page padding>

    <div class="q-pa-none">

      <div class="row">
        <div :class="uiStore.showHelp ? 'col-sm-7 col-md-8 help-main' : 'col-12 help-main help-main--full'">

          <q-card flat>
            <q-card-section :class="$q.platform.is.mobile ? 'q-px-none' : ''">
              <div class="row q-gutter-xs q-mt-xs justify-between">
                <div class="col-auto">
                  <q-btn-group>
                    <q-btn
                      @click="openPlaylistSettings(null)"
                      color="primary"
                      icon-right="add"
                      label="Add Playlist" />
                  </q-btn-group>
                </div>
              </div>
            </q-card-section>

            <q-card-section :class="$q.platform.is.mobile ? 'q-px-none' : ''">
              <div class="q-gutter-sm">
                <q-list bordered separator class="rounded-borders">

                  <q-item
                    v-for="(playlist, index) in listOfPlaylists"
                    v-bind:key="index"
                    :class="playlist.enabled ? '' : 'bg-grey-3'">

                    <q-item-section avatar top>
                      <q-icon name="playlist_play" color="" size="34px" />
                    </q-item-section>

                    <q-item-section top>
                      <q-item-label lines="1">
                        <span class="text-weight-medium">{{ playlist.name }}</span>
                        <span class="text-grey-8"> - {{ playlist.url }}</span>
                      </q-item-label>
                      <q-item-label caption lines="1">
                        Connections Limit: {{ playlist.connections }}
                      </q-item-label>
                    </q-item-section>

                    <q-item-section top side>
                      <div class="text-grey-8 q-gutter-xs">
                        <q-btn-dropdown
                          flat dense rounded
                          size="12px"
                          no-icon-animation
                          dropdown-icon="more_vert">
                          <q-list>

                            <q-item clickable v-close-popup @click="updatePlaylist(playlist.id)">
                              <q-item-section avatar>
                                <q-icon color="info" name="update" />
                              </q-item-section>
                              <q-item-section>
                                <q-item-label>Update</q-item-label>
                              </q-item-section>
                            </q-item>

                            <q-item clickable v-close-popup @click="openPlaylistSettings(playlist.id)">
                              <q-item-section avatar>
                                <q-icon color="grey-8" name="tune" />
                              </q-item-section>
                              <q-item-section>
                                <q-item-label>Configure</q-item-label>
                              </q-item-section>
                            </q-item>

                            <q-item clickable v-close-popup @click="removePlaylist(playlist.id)">
                              <q-item-section avatar>
                                <q-icon color="negative" name="delete" />
                              </q-item-section>
                              <q-item-section>
                                <q-item-label>Delete</q-item-label>
                              </q-item-section>
                            </q-item>

                          </q-list>
                        </q-btn-dropdown>

                      </div>
                    </q-item-section>

                  </q-item>

                </q-list>
              </div>
            </q-card-section>
          </q-card>
        </div>
        <div :class="uiStore.showHelp ? 'col-sm-5 col-md-4 help-panel' : 'help-panel help-panel--hidden'">
          <q-slide-transition>
            <q-card v-show="uiStore.showHelp" class="note-card q-my-md">
              <q-card-section>
                <div class="text-h5 q-mb-none">Setup Steps:</div>
                <q-list>

                <q-separator inset spaced />

                <q-item>
                  <q-item-section>
                    <q-item-label>
                      1. Add one or more playlists. Configure these playlists with a name, URL and connection limit.
                    </q-item-label>
                  </q-item-section>
                </q-item>
                <q-item>
                  <q-item-section>
                    <q-item-label>
                      2. Click on the kebab menu for each added playlist and click on the <b>Update</b> button to fetch
                      the playlist and import it into TIC's database.
                    </q-item-label>
                  </q-item-section>
                </q-item>

                </q-list>
              </q-card-section>
              <q-card-section>
                <div class="text-h5 q-mb-none">Notes:</div>
                <q-list>

                <q-separator inset spaced />

                <q-item-label class="text-primary">
                  Use HLS proxy:
                </q-item-label>
                <q-item>
                  <q-item-section>
                    <q-item-label>
                      Configuring a HLS proxy for your playlist will proxy any requests through that proxy server.
                      <br>
                      This has the benefit of:
                      <ul>
                        <li>
                          Inject custom HTTP headers in all outbound proxied requests.
                        </li>
                        <li>
                          Prefetch and caching video segments (.ts files) so multiple clients only download from one common source.
                        </li>
                        <li>
                          Bypass CORS restrictions.
                        </li>
                      </ul>
                    </q-item-label>
                  </q-item-section>
                </q-item>

                </q-list>
              </q-card-section>
            </q-card>
          </q-slide-transition>
        </div>
      </div>

    </div>

  </q-page>
</template>

<script>
import {defineComponent, ref} from 'vue';
import axios from 'axios';
import PlaylistInfoDialog from 'components/PlaylistInfoDialog.vue';
import {copyToClipboard} from 'quasar';
import {useUiStore} from 'stores/ui';

export default defineComponent({
  name: 'PlaylistsPage',

  setup() {
    return {
      uiStore: useUiStore(),
    };
  },
  data() {
    return {
      listOfPlaylists: ref([]),
      appUrl: ref(`${window.location.origin}`),

      // Application Settings
      enableStreamBuffer: ref(null),
      defaultFfmpegPipeArgs: ref(null),
    };
  },
  methods: {
    fetchSettings: function() {
      // Fetch current settings
      axios({
        method: 'get',
        url: '/tic-api/playlists/get',
      }).then((response) => {
        this.listOfPlaylists = response.data.data;
      }).catch(() => {
        this.$q.notify({
          color: 'negative',
          position: 'top',
          message: 'Failed to fetch settings',
          icon: 'report_problem',
          actions: [{icon: 'close', color: 'white'}],
        });
      });
      axios({
        method: 'get',
        url: '/tic-api/get-settings',
      }).then((response) => {
        this.appUrl = response.data.data.app_url;
        this.enableStreamBuffer = response.data.data.enable_stream_buffer;
        this.defaultFfmpegPipeArgs = response.data.data.default_ffmpeg_pipe_args;
      }).catch(() => {
        this.$q.notify({
          color: 'negative',
          position: 'top',
          message: 'Failed to fetch settings',
          icon: 'report_problem',
          actions: [{icon: 'close', color: 'white'}],
        });
      });
    },
    openPlaylistSettings: function(playlistId) {
      if (!playlistId) {
        playlistId = null;
      }
      // Display the dialog
      this.$q.dialog({
        component: PlaylistInfoDialog,
        componentProps: {
          playlistId: playlistId,
        },
      }).onOk((payload) => {
        this.fetchSettings();
      }).onDismiss(() => {
      });
    },
    updatePlaylist: function(playlistId) {
      // Fetch current settings
      this.$q.loading.show();
      axios({
        method: 'POST',
        url: `/tic-api/playlists/update/${playlistId}`,
      }).then((response) => {
        this.$q.loading.hide();
        this.$q.notify({
          color: 'positive',
          icon: 'cloud_done',
          message: 'Playlist update queued',
          timeout: 200,
        });
      }).catch(() => {
        this.$q.loading.hide();
        this.$q.notify({
          color: 'negative',
          position: 'top',
          message: 'Failed to queue update of playlist',
          icon: 'report_problem',
          actions: [{icon: 'close', color: 'white'}],
        });
      });
    },
    removePlaylist: function(playlistId) {
      // Fetch current settings
      this.$q.loading.show();
      axios({
        method: 'DELETE',
        url: `/tic-api/playlists/${playlistId}/delete`,
      }).then((response) => {
        this.$q.loading.hide();
        this.$q.notify({
          color: 'positive',
          icon: 'cloud_done',
          message: 'Playlist removed',
          timeout: 200,
        });
        this.fetchSettings();
      }).catch(() => {
        this.$q.loading.hide();
        this.$q.notify({
          color: 'negative',
          position: 'top',
          message: 'Failed to remove playlist',
          icon: 'report_problem',
          actions: [{icon: 'close', color: 'white'}],
        });
      });
    },
    copyUrlToClipboard: function(textToCopy) {
      copyToClipboard(textToCopy).then(() => {
        // Notify user of success
        this.$q.notify({
          color: 'green',
          textColor: 'white',
          icon: 'done',
          message: 'URL copied to clipboard',
        });
      }).catch((err) => {
        // Handle the error (e.g., clipboard API not supported)et
        console.error('Copy failed', err);
        this.$q.notify({
          color: 'red',
          textColor: 'white',
          icon: 'error',
          message: 'Failed to copy URL',
        });
      });
    },
    save: function() {
      // Save settings
      let postData = {
        settings: {
          app_url: this.appUrl,
          enable_stream_buffer: this.enableStreamBuffer,
          default_ffmpeg_pipe_args: this.defaultFfmpegPipeArgs,
        },
      };
      axios({
        method: 'POST',
        url: '/tic-api/save-settings',
        data: postData,
      }).then((response) => {
        // Save success, show feedback
        this.fetchSettings();
        this.$q.notify({
          color: 'positive',
          icon: 'cloud_done',
          message: 'Saved',
          timeout: 200,
        });
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
  },
  created() {
    this.fetchSettings();
  },
});
</script>

<style scoped>
.help-main {
  transition: flex-basis 0.25s ease, max-width 0.25s ease;
}

.help-main--full {
  flex: 0 0 100%;
  max-width: 100%;
}

.help-panel--hidden {
  flex: 0 0 0%;
  max-width: 0%;
  padding: 0;
  overflow: hidden;
}
</style>
