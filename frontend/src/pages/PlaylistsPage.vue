<template>
  <q-page>
    <div class="q-pa-md">
      <div class="row">
        <div :class="uiStore.showHelp && !$q.screen.lt.md ? 'col-sm-7 col-md-8 help-main' : 'col-12 help-main help-main--full'">
          <q-card flat>
            <q-card-section :class="$q.platform.is.mobile ? 'q-px-none' : ''">
              <div class="row items-center q-col-gutter-sm justify-between">
                <div :class="$q.screen.lt.sm ? 'col-12' : 'col-auto'">
                  <TicButton
                    label="Add Source"
                    icon="add"
                    color="primary"
                    :class="$q.screen.lt.sm ? 'full-width' : ''"
                    @click="openPlaylistSettings(null)" />
                </div>
                <div :class="$q.screen.lt.sm ? 'col-12' : 'col-12 col-sm-6 col-md-5'">
                  <TicSearchInput
                    v-model="searchQuery"
                    label="Search sources"
                    placeholder="Name, URL, type..."
                  />
                </div>
              </div>
            </q-card-section>

            <q-card-section :class="$q.platform.is.mobile ? 'q-px-none' : ''">
              <div class="q-gutter-sm">
                <q-list bordered separator class="rounded-borders">
                  <q-item
                    v-for="playlist in filteredPlaylists"
                    :key="playlist.id"
                    :class="playlist.enabled ? '' : 'disabled-item'">
                    <q-item-section avatar top>
                      <q-icon :name="playlistHasIssue(playlist) ? 'warning' : 'playlist_play'"
                              :color="playlistHasIssue(playlist) ? 'warning' : ''"
                              size="34px" />
                    </q-item-section>

                    <q-item-section top>
                      <q-item-label class="row items-center no-wrap q-gutter-sm">
                        <span class="text-weight-medium">{{ playlist.name }}</span>
                        <q-chip
                          v-if="playlistHasIssue(playlist)"
                          dense
                          color="orange-6"
                          text-color="white">
                          <q-icon name="warning" :class="$q.screen.gt.lg ? 'q-mr-xs' : ''" />
                          <span v-if="$q.screen.gt.lg">Needs attention</span>
                        </q-chip>
                      </q-item-label>
                      <q-item-label lines="1">
                        <span class="text-grey-8">{{ playlist.url }}</span>
                      </q-item-label>
                      <q-item-label caption lines="1">
                        Connections: {{ playlist.connections }} • Type: {{ formatPlaylistType(playlist.type) }}
                      </q-item-label>
                      <q-item-label v-if="playlistHasIssue(playlist)" caption class="text-warning">
                        Last update failed{{ playlistErrorTime(playlist) ? ` (${playlistErrorTime(playlist)})` : '' }}: {{
                        playlistErrorMessage(playlist) }}
                      </q-item-label>
                    </q-item-section>

                    <q-item-section top side>
                      <TicListActions
                        :actions="playlistActions(playlist)"
                        @action="(action) => handlePlaylistAction(action, playlist)"
                      />
                    </q-item-section>
                  </q-item>
                  <q-item v-if="!filteredPlaylists.length">
                    <q-item-section>
                      <q-item-label class="text-grey-7">
                        No sources found.
                      </q-item-label>
                    </q-item-section>
                  </q-item>
                </q-list>
              </div>
            </q-card-section>
          </q-card>
        </div>
        <TicResponsiveHelp v-model="uiStore.showHelp">
          <q-card-section>
                <div class="text-h5 q-mb-none">Setup Steps:</div>
                <q-list>

                  <q-separator inset spaced />

                  <q-item>
                    <q-item-section>
                      <q-item-label>
                        1. Add one or more stream sources. Configure these sources with a name, URL and connection
                        limit. The connection limit is used to pick a fallback source if a provider has reached its
                        allowed concurrent connections.
                      </q-item-label>
                    </q-item-section>
                  </q-item>
                  <q-item>
                    <q-item-section>
                      <q-item-label>
                        2. Choose a <b>Source Type</b>. M3U sources use a direct URL, while Xtream Codes sources use a
                        host, username and password to pull data via the XC API.
                      </q-item-label>
                    </q-item-section>
                  </q-item>
                  <q-item>
                    <q-item-section>
                      <q-item-label>
                        3. Optionally select a User Agent. This is used when Headendarr fetches M3U or XC data to avoid
                        provider blocks and improve compatibility.
                      </q-item-label>
                    </q-item-section>
                  </q-item>
                  <q-item>
                    <q-item-section>
                      <q-item-label>
                        4. Click on the kebab menu for each added source and click on the <b>Update</b> button to fetch
                        the stream list and import it into Headendarr's database.
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
                        Configuring a HLS proxy for your source will proxy any requests through that proxy server.
                        <br>
                        This has the benefit of:
                        <ul>
                          <li>
                            Inject custom HTTP headers in all outbound proxied requests.
                          </li>
                          <li>
                            Prefetch and caching video segments (.ts files) so multiple clients only download from one
                            common source.
                          </li>
                          <li>
                            Bypass CORS restrictions.
                          </li>
                        </ul>
                      </q-item-label>
                    </q-item-section>
                  </q-item>

                  <q-separator inset spaced />

                  <q-item-label class="text-primary">
                    Initial update is manual:
                  </q-item-label>
                  <q-item>
                    <q-item-section>
                      <q-item-label>
                        Adding a source does not download it immediately. Use the kebab menu and click <b>Update</b>
                        to fetch the stream list.
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
import axios from 'axios';
import PlaylistInfoDialog from 'components/PlaylistInfoDialog.vue';
import {useUiStore} from 'stores/ui';
import {TicButton, TicConfirmDialog, TicListActions, TicResponsiveHelp, TicSearchInput} from 'components/ui';

export default defineComponent({
  name: 'PlaylistsPage',
  components: {
    TicButton,
    TicListActions,
    TicResponsiveHelp,
    TicSearchInput,
  },

  setup() {
    return {
      uiStore: useUiStore(),
    };
  },
  data() {
    return {
      listOfPlaylists: [],
      searchQuery: '',
    };
  },
  computed: {
    filteredPlaylists() {
      const query = (this.searchQuery || '').trim().toLowerCase();
      if (!query) {
        return this.listOfPlaylists;
      }
      return this.listOfPlaylists.filter((playlist) => {
        const values = [
          playlist?.name,
          playlist?.url,
          playlist?.type,
          playlist?.user_agent,
          playlist?.health?.error,
        ];
        return values.some((value) => String(value || '').toLowerCase().includes(query));
      });
    },
  },
  methods: {
    playlistHasIssue(playlist) {
      return playlist?.health?.status === 'error';
    },
    playlistErrorMessage(playlist) {
      const error = playlist?.health?.error || 'Unknown download/import error';
      return error.length > 180 ? `${error.substring(0, 177)}...` : error;
    },
    playlistErrorTime(playlist) {
      const ts = playlist?.health?.last_failure_at;
      if (!ts) {
        return '';
      }
      try {
        return new Date(ts * 1000).toLocaleString();
      } catch {
        return '';
      }
    },
    formatPlaylistType: function(type) {
      if (!type) {
        return 'M3U';
      }
      return String(type).toUpperCase();
    },
    fetchSettings: function() {
      axios({
        method: 'get',
        url: '/tic-api/playlists/get',
      }).then((response) => {
        const playlists = response.data.data || [];
        playlists.sort((a, b) => (a?.name ?? '').localeCompare(b?.name ?? '', undefined, {numeric: true}));
        this.listOfPlaylists = playlists;
      }).catch(() => {
        this.$q.notify({
          color: 'negative',
          position: 'top',
          message: 'Failed to fetch sources',
          icon: 'report_problem',
          actions: [{icon: 'close', color: 'white'}],
        });
      });
    },
    playlistActions: function(playlist) {
      return [
        {id: 'update', icon: 'update', label: 'Update', color: 'info', tooltip: 'Update'},
        {
          id: 'configure',
          icon: 'tune',
          label: 'Configure Source',
          color: 'grey-8',
          tooltip: `Configure ${playlist.name || 'source'}`,
        },
        {id: 'delete', icon: 'delete', label: 'Delete', color: 'negative', tooltip: 'Delete'},
      ];
    },
    handlePlaylistAction: function(action, playlist) {
      if (action.id === 'update') {
        this.updatePlaylist(playlist.id);
        return;
      }
      if (action.id === 'configure') {
        this.openPlaylistSettings(playlist.id);
        return;
      }
      if (action.id === 'delete') {
        this.removePlaylist(playlist.id);
      }
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
      }).onOk(() => {
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
      const playlist = this.listOfPlaylists.find((item) => Number(item.id) === Number(playlistId));
      const playlistName = playlist?.name || `Source ${playlistId}`;
      this.$q.dialog({
        component: TicConfirmDialog,
        componentProps: {
          title: 'Delete Source?',
          message: `Delete "${playlistName}"? This action is final and cannot be undone.`,
          icon: 'warning',
          iconColor: 'negative',
          confirmLabel: 'Delete',
          confirmIcon: 'delete',
          confirmColor: 'negative',
          cancelLabel: 'Cancel',
          persistent: true,
        },
      }).onOk(() => {
        this.$q.loading.show();
        axios({
          method: 'DELETE',
          url: `/tic-api/playlists/${playlistId}/delete`,
        }).then(() => {
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
