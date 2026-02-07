<template>
  <q-layout view="hHh LpR lFf">
    <q-header elevated class="bg-primary text-white" height-hint="98">
      <q-toolbar class="bg-primary text-white q-my-md shadow-2">
        <q-toolbar-title class="q-mx-lg">
          <q-avatar size="2rem" font-size="82px">
            <img src="~assets/icon.png">
          </q-avatar>
          TIC (<u>T</u>Vheadend <u>I</u>PTV <u>C</u>onfig)
        </q-toolbar-title>
        <q-separator dark vertical inset />

        <q-tabs v-if="aioMode" no-caps align="left">
          <!--TODO: Find a way to prevent this from being destroyed from the DOM when showTvheadendAdmin is False-->
          <q-btn icon-right="fa-solid fa-window-restore" label="Show Tvheadend Backend"
                 @click="loadTvheadendAdmin = true; showTvheadendAdmin = true" />
          <q-dialog
            v-if="aioMode"
            v-model="showTvheadendAdmin"
            :class="{'hidden': !showTvheadendAdmin}"
            full-screen full-width persistent>
            <q-card class="full-screen-card">
              <q-bar class="bg-primary text-white">
                <div class="text-h6">Tvheadend Backend</div>
                <q-space />
                <q-btn
                  flat round dense
                  icon="open_in_new"
                  href="/tic-tvh/" target="_blank"
                  @click="showTvheadendAdmin = false">
                  <q-tooltip class="bg-white text-primary">
                    Open in new window
                  </q-tooltip>
                </q-btn>
                <q-btn
                  flat round dense
                  icon="close"
                  @click="showTvheadendAdmin = false">
                  <q-tooltip class="bg-white text-primary">
                    Close
                  </q-tooltip>
                </q-btn>
              </q-bar>

              <q-card-section class="full-screen-iframe">
                <iframe id="f1" ref="frame1" :src="(firstRun) ? '' : '/tic-tvh/'"></iframe>
              </q-card-section>
            </q-card>
          </q-dialog>
          <q-separator dark vertical inset />

          <q-btn-dropdown stretch flat
                          content-class="connection-details-dropdown"
                          label="Show Connection Details">


            <q-card class="my-card" flat bordered>
              <q-card-section horizontal>
                <q-card-section>

                  <q-card class="note-card q-my-md">
                    <q-card-section>
                      <div class="text-h6">How to use the Proxied M3U Playlists:</div>
                      Filtered M3U playlist URLs are designed for use with Jellyfin/Emby.
                      But they can also be used for any other client that supports M3U playlists.
                      <br>
                      Configure Jellyfin (or Emby) as follows:
                      <br>
                      <ol>
                        <li>
                          For each of the <span class="text-bold text-blue-7">Proxied M3U Playlists</span>
                          listed:
                          <ol type="a">
                            <li>
                              Create a new <b>M3U Tuner</b> in Jellyfin's <b>Live TV</b> device settings.
                            </li>
                            <li>
                              Copy
                              (
                              <q-icon name="content_copy" />
                              )
                              the URL of the playlist to the <b>File or URL</b> field in Jellyfin.
                            </li>
                            <li>
                              Configure the <b>Simultaneous stream limit</b> section in Jellyfin with the
                              <b>Connections Limit</b> specified.
                            </li>
                            <li>
                              Save the <b>Live TV Tuner Setup</b> form in Jellyfin.
                            </li>
                          </ol>
                        </li>
                        <li>
                          Create a new "XMLTV" <b>TV Guide Data Provider</b> in Jellyfin's <b>Live TV</b> device
                          settings.
                          <ol type="a">
                            <li>
                              Copy
                              (
                              <q-icon name="content_copy" />
                              )
                              the <span class="text-bold text-orange-7">XMLTV Guide</span> URL to the <b>File or URL</b>
                              field in Jellyfin.
                            </li>
                            <li>
                              Save the <b>Xml TV</b> form in Jellyfin.
                            </li>
                          </ol>
                        </li>
                      </ol>
                    </q-card-section>
                    <q-card-section>
                      <div class="text-h6">How to use the Proxied HDHomeRun Tuner Emulators:</div>
                      The HDHomeRun Tuner Emulator URLs are designed for use with Jellyfin, Emby and Plex.
                      <br><br>
                      For each of the <span class="text-bold text-green-7">HDHomeRun Tuner Emulators</span>
                      listed above:
                      <ol>
                        <li>
                          Create a new <b>HDHomeRun</b> tuner device in Jellyfin, Emby or Plex's <b>Live TV/DVR</b>
                          settings.
                        </li>
                        <li>
                          Copy
                          (
                          <q-icon name="content_copy" />
                          )
                          the URL of the HDHomeRun Tuner Emulator to Jellyfin, Emby or Plex.
                        </li>
                        <li>
                          <b>(Plex Only)</b> Click the link "Have an XMLTV guide on your server? Click here to use it.".
                          Copy
                          (
                          <q-icon name="content_copy" />
                          )
                          the <span class="text-bold text-orange-7">XMLTV Guide</span> URL above to the
                          <b>XMLTV GUIDE</b> field in Plex.
                        </li>
                        <li>
                          <b>(Jellyfin & Emby)</b> Add a new "XMLTV" <b>TV Guide Data Provider</b>.
                          Copy
                          (
                          <q-icon name="content_copy" />
                          )
                          the <span class="text-bold text-orange-7">XMLTV Guide</span> URL above to the
                          <b>File or URL</b> field in Jellyfin/Emby.
                        </li>
                      </ol>
                      <br>
                    </q-card-section>
                  </q-card>
                </q-card-section>

                <q-separator vertical />

                <q-card-section>

                  <q-list>

                    <q-item-label header>EPG</q-item-label>
                    <q-item clickable @click="copyUrlToClipboard(epgUrl)"
                            tabindex="0">
                      <q-item-section avatar>
                        <q-avatar
                          icon="calendar_month"
                          color="secondary"
                          text-color="white" />
                      </q-item-section>
                      <q-item-section>
                        <q-item-label class="text-bold text-orange-7">XMLTV Guide</q-item-label>
                        <q-item-label caption>{{ epgUrl }}</q-item-label>
                      </q-item-section>
                      <q-item-section side>
                        <q-icon name="content_copy" />
                      </q-item-section>
                    </q-item>

                    <q-separator inset spaced />

                    <q-item-label header>Proxied M3U Playlists</q-item-label>
                    <q-item v-for="playlist in enabledPlaylists" :key="`x.${playlist}`"
                            clickable
                            @click="copyUrlToClipboard(`${appUrl}/tic-api/tvh_playlist/${playlist.id}/channels.m3u?stream_key=${currentStreamingKey}`)"
                            tabindex="0">
                      <q-item-section avatar>
                        <q-avatar
                          v-if="true"
                          icon="playlist_play"
                          color="secondary"
                          text-color="white" />
                      </q-item-section>
                      <q-item-section>
                        <q-item-label class="text-bold text-blue-7">{{ playlist.name }}</q-item-label>
                        <q-item-label caption>{{ appUrl }}/tic-api/tvh_playlist/{{ playlist.id }}/channels.m3u?stream_key={{ currentStreamingKey }}
                        </q-item-label>
                        <q-item-label caption>Connections Limit: {{ playlist.connections }}</q-item-label>
                      </q-item-section>
                      <q-item-section side>
                        <q-icon name="content_copy" />
                      </q-item-section>
                    </q-item>

                    <q-separator inset spaced />

                    <q-item-label header>Proxied HDHomeRun Tuner Emulators</q-item-label>
                    <q-item v-for="playlist in enabledPlaylists" :key="`x.${playlist}`"
                            clickable @click="copyUrlToClipboard(`${appUrl}/tic-api/hdhr_device/${currentStreamingKey}/${playlist.id}`)"
                            tabindex="0">
                      <q-item-section avatar>
                        <q-avatar size="2rem" font-size="82px">
                          <img src="~assets/hd-icon.png">
                        </q-avatar>
                      </q-item-section>
                      <q-item-section>
                        <q-item-label class="text-bold text-green-7">{{ playlist.name }}</q-item-label>
                        <q-item-label caption>{{ appUrl }}/tic-api/hdhr_device/{{ currentStreamingKey }}/{{ playlist.id }}</q-item-label>
                      </q-item-section>
                      <q-item-section side>
                        <q-icon name="content_copy" />
                      </q-item-section>
                    </q-item>
                  </q-list>
                </q-card-section>
              </q-card-section>
            </q-card>
          </q-btn-dropdown>
          <q-separator dark vertical inset />
        </q-tabs>
      </q-toolbar>
    </q-header>

    <q-drawer
      v-model="leftDrawerOpen"
      show-if-above
      elevated
      side="left" behavior="desktop"
      class="drawer-layout"
    >
      <div class="drawer-content">
        <div class="drawer-scroll">
          <q-list>
            <q-item-label header>
              <!--          Essential Links-->
            </q-item-label>

            <EssentialLink
              v-for="link in essentialLinks"
              :key="link.title"
              v-bind="link"
            />
          </q-list>

          <q-separator class="q-my-lg" />

          <q-list>
            <q-item-label style="padding-left:10px" header>
              <q-btn
                flat round dense
                :color="pendingTasksStatus === 'paused' ? '' : ''"
                :icon="pendingTasksStatus === 'paused' ? 'play_circle' : 'pause_circle'"
                :tooltip="'running'"
                @click="tasksPauseResume">
                <q-tooltip class="bg-accent">
                  Background task queue {{ pendingTasksStatus }}
                </q-tooltip>
              </q-btn>
              Upcoming background tasks:
            </q-item-label>
            <q-item
              v-for="(task, index) in pendingTasks"
              :key="index">
              <q-item-section avatar>
                <q-icon
                  color="primary"
                  :class="pendingTasksStatus === 'paused' ? 'rotating-icon' : ''"
                  :name="pendingTasksStatus === 'paused' ? 'motion_photos_on' : task.icon" />
              </q-item-section>

              <q-item-section>
                <q-item-label caption>{{ task.name }}</q-item-label>
              </q-item-section>
            </q-item>
          </q-list>
        </div>

        <div class="drawer-footer">
          <q-separator class="q-my-md" />
          <q-list>
            <q-item>
              <q-item-section>
                <q-item-label>{{ currentUsername }}</q-item-label>
                <q-item-label caption>User</q-item-label>
              </q-item-section>
              <q-item-section side>
                <q-btn dense flat icon="account_circle">
                  <q-menu>
                    <q-list style="min-width: 160px;">
                      <q-item clickable v-close-popup @click="goToUserSettings">
                        <q-item-section>Settings</q-item-section>
                      </q-item>
                      <q-item clickable v-close-popup @click="logout">
                        <q-item-section>Logout</q-item-section>
                      </q-item>
                    </q-list>
                  </q-menu>
                </q-btn>
              </q-item-section>
            </q-item>
          </q-list>
        </div>
      </div>
    </q-drawer>

    <q-page-container>
      <router-view />
    </q-page-container>
  </q-layout>
</template>

<style>
.connection-details-dropdown {
  max-height: calc(100vh - 100px);
  overflow-y: auto;
}

.rotating-icon {
  animation: spin 2s linear infinite;
}

@keyframes spin {
  0% {
    transform: rotate(0deg);
  }
  100% {
    transform: rotate(360deg);
  }
}

.hidden {
  display: none;
}

.full-screen-card {
  display: flex;
  flex-direction: column;
  height: 100vh;
  width: 100vw;
  padding: 1px;
}

.full-screen-iframe {
  flex: 1;
  overflow: hidden;
  padding: 0;
}

.full-screen-iframe iframe {
  width: 100%;
  height: 100%;
  border: none;
}

.note-card {
  background-color: #fff3cd;
  border: 1px solid #ffeeba;
  border-radius: 8px;
  box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
}

.warning-card {
  background-color: #fdddcd;
  border: 1px solid #ffdfc4;
  border-radius: 8px;
  box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
}

.sub-setting {
  margin-left: 30px;
  padding-top: 8px;
  padding-left: 8px;
  border-left: solid thin var(--q-primary);
}

.drawer-layout {
  display: flex;
  flex-direction: column;
}

.drawer-content {
  display: flex;
  flex-direction: column;
  height: 100%;
}

.drawer-scroll {
  flex: 1;
  overflow-y: auto;
}

.drawer-footer {
  margin-top: auto;
  padding-bottom: 8px;
}
</style>

<script>
import {defineComponent, onMounted, ref, computed} from 'vue';
import EssentialLink from 'components/EssentialLink.vue';
import pollForBackgroundTasks from 'src/mixins/backgroundTasksMixin';
import aioStartupTasks from 'src/mixins/aioFunctionsMixin';
import axios from 'axios';
import {copyToClipboard, useQuasar} from 'quasar';
import {useAuthStore} from 'stores/auth';
import {useRouter} from 'vue-router';

const linksList = [
  {
    title: 'General',
    caption: 'General TVH-IPTV-Config settings',
    icon: 'tune',
    link: '/general',
  },
  {
    title: 'TVheadend',
    caption: 'TVheadend Settings',
    icon: 'img:icons/tvh-icon.svg',
    link: '/tvheadend',
  },
  {
    title: 'Playlists',
    caption: 'Configure Playlist Sources',
    icon: 'playlist_play',
    link: '/playlists',
  },
  {
    title: 'EPGs',
    caption: 'Configure a list of EPG sources',
    icon: 'calendar_month',
    link: '/epgs',
  },
  {
    title: 'Channels',
    caption: 'Configure Channels',
    icon: 'queue_play_next',
    link: '/channels',
  },
  {
    title: 'Users',
    caption: 'Manage users and roles',
    icon: 'manage_accounts',
    link: '/users',
    adminOnly: true,
  },
];

export default defineComponent({
  name: 'MainLayout',

  components: {
    EssentialLink,
  },

  setup() {
    const $q = useQuasar();
    const router = useRouter();
    const authStore = useAuthStore();
    const leftDrawerOpen = ref(false);
    const tasksArePaused = ref(false);
    const {pendingTasks, pendingTasksStatus} = pollForBackgroundTasks();
    const {firstRun, aioMode} = aioStartupTasks();

    const loadTvheadendAdmin = ref(true);
    const showTvheadendAdmin = ref(false);
    const appUrl = ref(window.location.origin);

    const enabledPlaylists = ref([]);

    const copyUrlToClipboard = (textToCopy) => {
      copyToClipboard(textToCopy).then(() => {
        // Notify user of success
        $q.notify({
          color: 'green',
          textColor: 'white',
          icon: 'done',
          message: 'URL copied to clipboard',
        });
      }).catch((err) => {
        // Handle the error (e.g., clipboard API not supported)
        console.error('Copy failed', err);
        $q.notify({
          color: 'red',
          textColor: 'white',
          icon: 'error',
          message: 'Failed to copy URL',
        });
      });
    };

    const tasksPauseResume = () => {
      // tasksArePaused.value = !tasksArePaused.value;
      // Your logic to toggle pause/resume the tasks
      axios({
        method: 'GET',
        url: '/tic-api/toggle-pause-background-tasks',
      }).catch(() => {
        this.$q.notify({
          color: 'negative',
          position: 'top',
          message: 'Failed to pause task queue',
          icon: 'report_problem',
          actions: [{icon: 'close', color: 'white'}],
        });
      });
    };

    onMounted(() => {
      // Fetch current settings
      axios({
        method: 'get',
        url: '/tic-api/get-settings',
      }).then((response) => {
        appUrl.value = response.data.data.app_url;
      }).catch(() => {
      });
      // Fetch playlists list
      axios({
        method: 'get',
        url: '/tic-api/playlists/get',
      }).then((response) => {
        enabledPlaylists.value = response.data.data;
      }).catch(() => {
      });
    });

    const isAdmin = computed(() => (authStore.user?.roles || []).includes('admin'));
    const filteredLinks = computed(() => linksList.filter((link) => !link.adminOnly || isAdmin.value));
    const currentUsername = computed(() => authStore.user?.username || 'User');
    const currentStreamingKey = computed(() => authStore.user?.streaming_key || 'STREAM_KEY');

    const logout = async () => {
      await authStore.logout();
      await router.push({path: '/login'});
    };

    const goToUserSettings = async () => {
      await router.push({path: '/user-settings'});
    };

    const epgUrl = computed(() => `${appUrl.value}/xmltv.php?stream_key=${currentStreamingKey.value}`);

    return {
      firstRun,
      aioMode,
      loadTvheadendAdmin,
      showTvheadendAdmin,
      enabledPlaylists,
      appUrl,
      epgUrl,
      essentialLinks: filteredLinks,
      currentUsername,
      currentStreamingKey,
      leftDrawerOpen,
      toggleLeftDrawer() {
        leftDrawerOpen.value = !leftDrawerOpen.value;
      },
      copyUrlToClipboard,
      pendingTasks,
      pendingTasksStatus,
      tasksPauseResume,
      tasksArePaused,
      logout,
      goToUserSettings,
    };
  },
});
</script>
