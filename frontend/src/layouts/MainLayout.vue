<template>
  <q-layout view="hHh LpR lFf">
    <q-header elevated class="bg-primary text-white" height-hint="98">
      <q-toolbar class="bg-primary text-white q-py-md shadow-2">
        <q-btn
          flat
          round
          dense
          :icon="drawerToggleIcon"
          class="q-ml-sm"
          @click="toggleLeftDrawer"
        />
        <q-toolbar-title class="q-mx-lg">
          <q-avatar size="2rem" font-size="82px">
            <img src="~assets/icon.png">
          </q-avatar>
          TIC (<u>T</u>Vheadend <u>I</u>PTV <u>C</u>onfig)
        </q-toolbar-title>
        <q-separator dark vertical inset />

        <q-tabs v-if="aioMode" no-caps align="left">
          <!--TODO: Find a way to prevent this from being destroyed from the DOM when showTvheadendAdmin is False-->
          <q-btn
            :icon-right="!compactHeader ? 'fa-solid fa-window-restore' : void 0"
            :icon="compactHeader ? 'fa-solid fa-window-restore' : void 0"
            :label="compactHeader ? '' : 'Show Tvheadend Backend'"
            :flat="compactHeader"
            :round="compactHeader"
            dense
            class="q-mx-sm"
            @click="loadTvheadendAdmin = true; showTvheadendAdmin = true"
          />
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

          <q-btn-dropdown
            v-if="!isConnectionDialogCompact"
            stretch
            flat
            :round="compactHeader"
            :icon="compactHeader ? 'link' : void 0"
            :label="compactHeader ? '' : 'Show Connection Details'"
            class="q-mx-md"
            content-class="connection-details-dropdown tic-dropdown-menu"
          >
            <ConnectionDetailsPanel
              :enabled-playlists="enabledPlaylists"
              :connection-base-url="connectionBaseUrl"
              :current-streaming-key="currentStreamingKey"
              :epg-url="epgUrl"
              :xc-playlist-url="xcPlaylistUrl"
              @copy-url="copyUrlToClipboard"
            />
          </q-btn-dropdown>
          <q-btn
            v-else
            :icon-right="!compactHeader ? 'link' : void 0"
            :icon="compactHeader ? 'link' : void 0"
            :label="compactHeader ? '' : 'Show Connection Details'"
            :flat="compactHeader"
            :round="compactHeader"
            dense
            class="q-mx-md"
            @click="showConnectionDetailsDialog = true"
          />
          <TicDialogWindow
            v-if="isConnectionDialogCompact"
            v-model="showConnectionDetailsDialog"
            title="Connection Details"
            width="100vw"
            position="left"
          >
            <ConnectionDetailsPanel
              :enabled-playlists="enabledPlaylists"
              :connection-base-url="connectionBaseUrl"
              :current-streaming-key="currentStreamingKey"
              :epg-url="epgUrl"
              :xc-playlist-url="xcPlaylistUrl"
              :show-top-help-hint="true"
              @copy-url="copyUrlToClipboard"
            />
          </TicDialogWindow>
          <q-separator dark vertical inset />
          <q-btn
            id="header-help-toggle"
            flat
            round
            dense
            icon="help_outline"
            class="q-ml-sm"
            :color="uiStore.showHelp ? 'secondary' : 'white'"
            @click="toggleHelp">
            <q-tooltip class="bg-white text-primary">
              {{ uiStore.showHelp ? 'Hide setup help' : 'Show setup help' }}
            </q-tooltip>
          </q-btn>
        </q-tabs>
      </q-toolbar>
    </q-header>

    <q-drawer
      v-model="leftDrawerOpen"
      :mini="drawerMini"
      :width="drawerWidth"
      elevated
      side="left"
      :overlay="isMobileDrawer"
      :behavior="drawerBehavior"
      :show-if-above="!isMobileDrawer"
      class="drawer-layout"
    >
      <div class="drawer-content">
        <div v-if="isMobileDrawer" class="drawer-mobile-header">
          <q-toolbar class="bg-card-head text-primary q-py-sm">
            <q-btn
              outline
              dense
              round
              icon="arrow_back"
              color="grey-7"
              class="drawer-mobile-close-btn"
              @click="leftDrawerOpen = false"
            >
              <q-tooltip class="bg-white text-primary no-wrap" style="max-width: none">
                Close
              </q-tooltip>
            </q-btn>
            <q-space />
            <div class="text-h6 text-primary ellipsis text-right q-pr-xs">
              Menu
            </div>
          </q-toolbar>
        </div>
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

          <q-separator class="q-my-lg" v-if="!drawerMini" />

          <q-list v-if="!drawerMini">
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
                  :name="pendingTasksStatus === 'paused' && task.taskState === 'queued' ? 'pause_circle' : task.icon" />
                <q-tooltip class="bg-white text-primary">
                  {{ task.name }}
                </q-tooltip>
              </q-item-section>

              <q-item-section>
                <q-item-label caption>{{ task.name }}</q-item-label>
              </q-item-section>
            </q-item>
          </q-list>
        </div>

        <div :class="['drawer-footer', { 'drawer-footer--mini': drawerMini }]">
          <q-separator class="q-my-md" />
          <q-list v-if="drawerMini">
            <q-item>
              <q-item-section avatar class="items-center">
                <q-btn dense round color="primary" icon="account_circle" size="md">
                  <q-menu>
                    <q-list style="min-width: 160px;">
                      <q-item clickable v-close-popup @click="goToUserSettings">
                        <q-item-section>User Settings</q-item-section>
                      </q-item>
                      <q-item clickable v-close-popup @click="logout">
                        <q-item-section>Logout</q-item-section>
                      </q-item>
                    </q-list>
                  </q-menu>
                  <q-tooltip class="bg-white text-primary">
                    {{ currentUsername }}
                  </q-tooltip>
                </q-btn>
              </q-item-section>
            </q-item>
          </q-list>
          <q-list v-else>
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

    <FloatingPlayer />
  </q-layout>
</template>

<style>
.connection-details-dropdown {
  max-height: calc(100vh - 100px);
  overflow-y: auto;
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
  padding-left: 16px;
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
  padding: 8px 0 16px;
  background: #fff;
  position: sticky;
  bottom: 0;
  z-index: 1;
}

.drawer-footer--mini {
  padding-bottom: 16px;
}

.drawer-footer--mini .q-item {
  justify-content: center;
}
</style>

<script>
import {defineComponent, onMounted, ref, computed, watch} from 'vue';
import EssentialLink from 'components/EssentialLink.vue';
import FloatingPlayer from 'components/FloatingPlayer.vue';
import ConnectionDetailsPanel from 'components/ConnectionDetailsPanel.vue';
import {TicDialogWindow} from 'components/ui';
import pollForBackgroundTasks from 'src/mixins/backgroundTasksMixin';
import aioStartupTasks from 'src/mixins/aioFunctionsMixin';
import axios from 'axios';
import {copyToClipboard, useQuasar} from 'quasar';
import {useAuthStore} from 'stores/auth';
import {useUiStore} from 'stores/ui';
import {useRouter} from 'vue-router';

const linksList = [
  {
    title: 'Sources',
    caption: 'Configure Stream Sources',
    icon: 'playlist_play',
    link: '/playlists',
  },
  {
    title: 'EPGs',
    caption: 'Configure EPG Sources',
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
    title: 'TV Guide',
    caption: 'View EPG grid and preview streams',
    icon: 'live_tv',
    link: '/guide',
    streamerOnly: true,
  },
  {
    title: 'DVR',
    caption: 'Schedule and manage recordings',
    icon: 'dvr',
    link: '/dvr',
    streamerOnly: true,
  },
  {
    title: 'Users',
    caption: 'Manage users and roles',
    icon: 'manage_accounts',
    link: '/users',
    adminOnly: true,
  },
  {
    title: 'TVheadend',
    caption: 'TVheadend Settings',
    icon: 'tvh-icon',
    link: '/tvheadend',
  },
  {
    title: 'Settings',
    caption: 'Application Settings',
    icon: 'tune',
    link: '/settings',
    adminOnly: true,
  },
];

export default defineComponent({
  name: 'MainLayout',

  components: {
    EssentialLink,
    FloatingPlayer,
    ConnectionDetailsPanel,
    TicDialogWindow,
  },

  setup() {
    const $q = useQuasar();
    const router = useRouter();
    const authStore = useAuthStore();
    const uiStore = useUiStore();
    const leftDrawerOpen = ref(true);
    const drawerMini = ref(false);
    const tasksArePaused = ref(false);
    const {pendingTasks, pendingTasksStatus} = pollForBackgroundTasks();
    const {firstRun, aioMode} = aioStartupTasks();

    const loadTvheadendAdmin = ref(true);
    const showTvheadendAdmin = ref(false);
    const appUrl = ref(window.location.origin);
    const connectionBaseUrl = computed(() => window.location.origin);

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

    const isMobileDrawer = computed(() => $q.screen.width < 600);
    const isCompactDrawer = computed(() => $q.screen.width >= 600 && $q.screen.width < 1024);
    const compactHeader = computed(() => $q.screen.width <= 1024);
    const isConnectionDialogCompact = computed(() => $q.screen.width <= 1023);
    const showConnectionDetailsDialog = ref(false);
    const drawerBehavior = computed(() => (isMobileDrawer.value ? 'mobile' : 'desktop'));
    const drawerWidth = computed(() => (isMobileDrawer.value ? Math.round($q.screen.width * 0.9) : 300));
    const drawerToggleIcon = computed(() => {
      if (isMobileDrawer.value) {
        return leftDrawerOpen.value ? 'close' : 'menu';
      }
      if (isCompactDrawer.value) {
        return drawerMini.value ? 'menu' : 'close';
      }
      return leftDrawerOpen.value ? 'menu_open' : 'menu';
    });

    const applyDrawerMode = () => {
      if (isMobileDrawer.value) {
        leftDrawerOpen.value = false;
        drawerMini.value = false;
      } else if (isCompactDrawer.value) {
        leftDrawerOpen.value = true;
        drawerMini.value = true;
      } else {
        leftDrawerOpen.value = true;
        drawerMini.value = false;
      }
    };

    onMounted(() => {
      applyDrawerMode();
      // Fetch current settings
      axios({
        method: 'get',
        url: '/tic-api/get-settings',
      }).then((response) => {
        appUrl.value = response.data.data.app_url;
        const theme = uiStore.loadThemeForUser(authStore.user?.username);
        uiStore.loadTimeFormatForUser(authStore.user?.username);
        $q.dark.set(theme === 'dark');
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

    watch([isMobileDrawer, isCompactDrawer], () => {
      applyDrawerMode();
    });

    watch(
      () => authStore.user?.username,
      (username) => {
        const theme = uiStore.loadThemeForUser(username);
        uiStore.loadTimeFormatForUser(username);
        $q.dark.set(theme === 'dark');
      },
    );

    const roles = computed(() => authStore.user?.roles || []);
    const isAdmin = computed(() => roles.value.includes('admin'));
    const isStreamer = computed(() => roles.value.includes('streamer'));
    const filteredLinks = computed(() => linksList.filter((link) => {
      if (link.adminOnly && !isAdmin.value) {
        return false;
      }
      if (link.streamerOnly && !(isAdmin.value || isStreamer.value)) {
        return false;
      }
      return true;
    }));
    const currentUsername = computed(() => authStore.user?.username || 'User');
    const currentStreamingKey = computed(() => authStore.user?.streaming_key || 'STREAM_KEY');

    const logout = async () => {
      await authStore.logout();
      await router.push({path: '/login'});
    };

    const goToUserSettings = async () => {
      await router.push({path: '/user-settings'});
    };

    const epgUrl = computed(() => (
      `${connectionBaseUrl.value}/xmltv.php?username=${currentUsername.value}&password=${currentStreamingKey.value}`
    ));
    const xcPlaylistUrl = computed(() => (
      `${connectionBaseUrl.value}/get.php?username=${currentUsername.value}&password=${currentStreamingKey.value}`
    ));
    const toggleHelp = () => {
      uiStore.toggleHelp();
    };

    return {
      firstRun,
      aioMode,
      loadTvheadendAdmin,
      showTvheadendAdmin,
      enabledPlaylists,
      appUrl,
      connectionBaseUrl,
      epgUrl,
      xcPlaylistUrl,
      essentialLinks: filteredLinks,
      currentUsername,
      currentStreamingKey,
      leftDrawerOpen,
      drawerMini,
      isMobileDrawer,
      drawerBehavior,
      drawerWidth,
      drawerToggleIcon,
      toggleLeftDrawer() {
        if (isMobileDrawer.value) {
          leftDrawerOpen.value = !leftDrawerOpen.value;
          return;
        }
        if (isCompactDrawer.value) {
          drawerMini.value = !drawerMini.value;
          leftDrawerOpen.value = true;
          return;
        }
        leftDrawerOpen.value = !leftDrawerOpen.value;
      },
      copyUrlToClipboard,
      pendingTasks,
      pendingTasksStatus,
      tasksPauseResume,
      tasksArePaused,
      logout,
      goToUserSettings,
      uiStore,
      toggleHelp,
      compactHeader,
      isConnectionDialogCompact,
      showConnectionDetailsDialog,
    };
  },
});
</script>
