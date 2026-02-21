<template>
  <q-card class="my-card" flat bordered>
    <q-banner
      v-if="showTopHelpHint"
      class="bg-blue-1 text-blue-10 q-mx-sm q-mt-sm q-mb-none"
      dense
      rounded
    >
      Scroll to the bottom for setup help and usage notes.
    </q-banner>
    <q-card-section :horizontal="!isCompact">
      <q-card-section :class="isCompact ? 'q-pa-sm' : ''">
        <q-list>
          <q-item-label header>EPG</q-item-label>
          <q-item :dense="isCompact" clickable tabindex="0" @click="$emit('copy-url', epgUrl)">
            <q-item-section avatar>
              <q-avatar :size="avatarSize" icon="calendar_month" color="secondary" text-color="white" />
            </q-item-section>
            <q-item-section>
              <q-item-label class="text-bold text-orange-7">XMLTV Guide</q-item-label>
              <q-item-label v-if="!isCompact" caption>{{ epgUrl }}</q-item-label>
            </q-item-section>
            <q-item-section side>
              <q-icon name="content_copy" />
            </q-item-section>
          </q-item>

          <q-separator inset spaced />

          <q-item-label header>Combined Playlist</q-item-label>
          <q-item :dense="isCompact" clickable tabindex="0" @click="$emit('copy-url', xcPlaylistUrl)">
            <q-item-section avatar>
              <q-avatar :size="avatarSize" icon="playlist_play" color="secondary" text-color="white" />
            </q-item-section>
            <q-item-section>
              <q-item-label class="text-bold text-purple-7">Combined Playlist</q-item-label>
              <q-item-label v-if="!isCompact" caption>{{ xcPlaylistUrl }}</q-item-label>
            </q-item-section>
            <q-item-section side>
              <q-icon name="content_copy" />
            </q-item-section>
          </q-item>

          <q-separator inset spaced />

          <q-item-label header>Connection Limited Playlists</q-item-label>
          <q-item
            v-for="playlist in enabledPlaylists"
            :key="`m3u.${playlist.id}`"
            :dense="isCompact"
            clickable
            tabindex="0"
            @click="$emit('copy-url', `${connectionBaseUrl}/tic-api/playlist/${playlist.id}.m3u?stream_key=${currentStreamingKey}`)"
          >
            <q-item-section avatar>
              <q-avatar :size="avatarSize" icon="playlist_play" color="secondary" text-color="white" />
            </q-item-section>
            <q-item-section>
              <q-item-label class="text-bold text-blue-7">{{ playlist.name }}</q-item-label>
              <q-item-label v-if="!isCompact" caption>
                {{ connectionBaseUrl }}/tic-api/playlist/{{ playlist.id
                }}.m3u?stream_key={{ currentStreamingKey }}
              </q-item-label>
              <q-item-label caption>Connections Limit: {{ playlist.connections }}</q-item-label>
            </q-item-section>
            <q-item-section side>
              <q-icon name="content_copy" />
            </q-item-section>
          </q-item>

          <q-separator inset spaced />

          <q-item-label header>HDHomeRun Tuner Emulators</q-item-label>
          <q-item
            :dense="isCompact"
            clickable
            tabindex="0"
            @click="$emit('copy-url', `${connectionBaseUrl}/tic-api/hdhr_device/${currentStreamingKey}/combined`)"
          >
            <q-item-section avatar>
              <q-avatar :size="avatarSize" :font-size="isPhone ? '54px' : '82px'">
                <img src="~assets/hd-icon.png">
              </q-avatar>
            </q-item-section>
            <q-item-section>
              <q-item-label class="text-bold text-teal-7">Combined HDHomeRun</q-item-label>
              <q-item-label v-if="!isCompact" caption>
                {{ connectionBaseUrl }}/tic-api/hdhr_device/{{ currentStreamingKey }}/combined
              </q-item-label>
            </q-item-section>
            <q-item-section side>
              <q-icon name="content_copy" />
            </q-item-section>
          </q-item>
          <q-item
            v-for="playlist in enabledPlaylists"
            :key="`hdhr.${playlist.id}`"
            :dense="isCompact"
            clickable
            tabindex="0"
            @click="$emit('copy-url', `${connectionBaseUrl}/tic-api/hdhr_device/${currentStreamingKey}/${playlist.id}`)"
          >
            <q-item-section avatar>
              <q-avatar :size="avatarSize" :font-size="isPhone ? '54px' : '82px'">
                <img src="~assets/hd-icon.png">
              </q-avatar>
            </q-item-section>
            <q-item-section>
              <q-item-label class="text-bold text-green-7">{{ playlist.name }}</q-item-label>
              <q-item-label v-if="!isCompact" caption>
                {{ connectionBaseUrl }}/tic-api/hdhr_device/{{ currentStreamingKey }}/{{ playlist.id }}
              </q-item-label>
            </q-item-section>
            <q-item-section side>
              <q-icon name="content_copy" />
            </q-item-section>
          </q-item>
        </q-list>
      </q-card-section>

      <q-separator :vertical="!isCompact" :spaced="isCompact" />

      <q-card-section :class="isCompact ? 'q-pa-sm' : ''">
        <q-card class="note-card q-my-md">
          <q-card-section>
            <div class="text-h6">How to use the XMLTV Guide:</div>
            Use the <span class="text-bold text-orange-7">XMLTV Guide</span> URL with clients that
            need a guide-only feed or separate XMLTV configuration.
          </q-card-section>
          <q-card-section>
            <div class="text-h6">How to use TVHeadend clients:</div>
            Connect TVHeadend-capable clients directly to TVHeadend when you want TVHeadend to handle
            tuning and stream limits.
            <br><br>
            Use the TVHeadend client URL on port <b>9981</b> (HTTP) or <b>9982</b> (HTSP) and ensure those
            ports are reachable from your client devices.
            <br><br>
            Use these credentials:
            <ul>
              <li><b>Username</b>: your Headendarr username</li>
              <li><b>Password</b>: your streaming key</li>
            </ul>
          </q-card-section>
          <q-card-section>
            <div class="text-h6">How to use XC clients:</div>
            Use IPTV clients that support Xtream Codes logins with the <span
            class="text-bold text-purple-7">Combined Playlist</span>
            URL. This is recommended for clients that cannot set per-playlist connection limits.
            <br><br>
            If the XC playlist is routed through TVHeadend, it enforces connection limits, which prevents
            streams from dropping when a channel is already in use.
            <br><br>
            Use these credentials:
            <ul>
              <li><b>Username</b>: your Headendarr username</li>
              <li><b>Password</b>: your streaming key</li>
            </ul>
            <br>
            Compatibility routes are also available:
            <ul>
              <li>
                <b>M3U (XC compat)</b>:
                <code>{{ connectionBaseUrl }}/get.php?username=&lt;username&gt;&password=&lt;stream_key&gt;</code>
              </li>
              <li>
                <b>XMLTV (XC compat)</b>:
                <code>{{ connectionBaseUrl }}/xmltv.php?username=&lt;username&gt;&password=&lt;stream_key&gt;</code>
              </li>
            </ul>
          </q-card-section>
          <q-card-section>
            <div class="text-h6">How to use the Connection Limited Playlists:</div>
            Filtered M3U playlist URLs are designed for use with Jellyfin/Emby.
            But they can also be used for any other client that supports M3U playlists.
            <br>
            Configure Jellyfin (or Emby) as follows:
            <br>
            <ol>
              <li>
                For each of the <span class="text-bold text-blue-7">Connection Limited Playlists</span> listed:
                <ol type="a">
                  <li>Create a new <b>M3U Tuner</b> in Jellyfin's <b>Live TV</b> device settings.</li>
                  <li>
                    Copy (
                    <q-icon name="content_copy" />
                    ) the URL of the playlist to the
                    <b>File or URL</b> field in Jellyfin.
                  </li>
                  <li>
                    Configure the <b>Simultaneous stream limit</b> section in Jellyfin with the
                    <b>Connections Limit</b> specified.
                  </li>
                  <li>Save the <b>Live TV Tuner Setup</b> form in Jellyfin.</li>
                </ol>
              </li>
              <li>
                Create a new "XMLTV" <b>TV Guide Data Provider</b> in Jellyfin's <b>Live TV</b> device settings.
                <ol type="a">
                  <li>
                    Copy (
                    <q-icon name="content_copy" />
                    ) the <span class="text-bold text-orange-7">XMLTV Guide</span>
                    URL to the <b>File or URL</b> field in Jellyfin.
                  </li>
                  <li>Save the <b>Xml TV</b> form in Jellyfin.</li>
                </ol>
              </li>
            </ol>
          </q-card-section>
          <q-card-section>
            <div class="text-h6">How to use the HDHomeRun Tuner Emulators:</div>
            The HDHomeRun Tuner Emulator URLs are designed for use with Jellyfin, Emby and Plex.
            <br><br>
            For each of the <span class="text-bold text-green-7">HDHomeRun Tuner Emulators</span> listed above:
            <ol>
              <li>Create a new <b>HDHomeRun</b> tuner device in Jellyfin, Emby or Plex's <b>Live TV/DVR</b> settings.
              </li>
              <li>
                Copy (
                <q-icon name="content_copy" />
                ) the URL of the HDHomeRun Tuner Emulator to Jellyfin, Emby or Plex.
              </li>
              <li>
                <b>(Plex Only)</b> Click the link "Have an XMLTV guide on your server? Click here to use it.".
                Copy (
                <q-icon name="content_copy" />
                ) the <span class="text-bold text-orange-7">XMLTV Guide</span> URL above
                to the <b>XMLTV GUIDE</b> field in Plex.
              </li>
              <li>
                <b>(Jellyfin & Emby)</b> Add a new "XMLTV" <b>TV Guide Data Provider</b>.
                Copy (
                <q-icon name="content_copy" />
                ) the <span class="text-bold text-orange-7">XMLTV Guide</span> URL above
                to the <b>File or URL</b> field in Jellyfin/Emby.
              </li>
            </ol>
            <br>
          </q-card-section>
        </q-card>
      </q-card-section>
    </q-card-section>
  </q-card>
</template>

<script setup>
import {computed} from 'vue';
import {useQuasar} from 'quasar';

defineProps({
  enabledPlaylists: {
    type: Array,
    default: () => [],
  },
  connectionBaseUrl: {
    type: String,
    required: true,
  },
  currentStreamingKey: {
    type: String,
    required: true,
  },
  epgUrl: {
    type: String,
    required: true,
  },
  xcPlaylistUrl: {
    type: String,
    required: true,
  },
  showTopHelpHint: {
    type: Boolean,
    default: false,
  },
});

defineEmits(['copy-url']);

const $q = useQuasar();
const isCompact = computed(() => $q.screen.width <= 1023);
const isPhone = computed(() => $q.screen.width <= 600);
const avatarSize = computed(() => (isCompact.value ? '24px' : '32px'));
</script>

<style scoped>
@media (max-width: 600px) {
  :deep(.q-item__section--avatar) {
    min-width: 28px;
  }
}
</style>
