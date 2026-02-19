<template>
  <q-page>
    <div class="q-pa-md">
      <div class="row">
        <div
          :class="uiStore.showHelp && !$q.screen.lt.md ? 'col-sm-7 col-md-8 help-main' : 'col-12 help-main help-main--full'">
          <q-card flat>
            <q-card-section :class="$q.platform.is.mobile ? 'q-px-none' : ''">
              <TicListToolbar
                :add-action="{label: 'Add EPG', icon: 'add'}"
                :search="{label: 'Search EPG sources', placeholder: 'Name, URL, description...'}"
                :search-value="searchQuery"
                @add="openEpgSettings(null)"
                @update:search-value="searchQuery = $event"
              />
            </q-card-section>

            <q-card-section :class="$q.platform.is.mobile ? 'q-px-none' : ''">
              <div class="q-gutter-sm">
                <q-list bordered separator class="rounded-borders">
                  <q-item
                    v-for="epg in filteredEpgs"
                    :key="epg.id"
                    :class="epg.enabled ? '' : 'disabled-item'">
                    <q-item-section avatar top>
                      <q-icon :name="epgHasIssue(epg) ? 'warning' : 'calendar_month'"
                              :color="epgHasIssue(epg) ? 'warning' : ''"
                              size="34px" />
                    </q-item-section>

                    <q-item-section top>
                      <q-item-label class="row items-center no-wrap q-gutter-sm">
                        <span class="text-weight-medium">{{ epg.name }}</span>
                        <q-chip
                          v-if="epgLastUpdatedAgo(epg)"
                          dense
                          color="blue-7"
                          text-color="white"
                        >
                          <q-icon name="schedule" class="q-mr-xs" />
                          {{ epgLastUpdatedAgo(epg) }}
                          <q-tooltip>
                            EPG was last updated {{ epgLastUpdatedAgo(epg, false) }}
                          </q-tooltip>
                        </q-chip>
                        <q-chip
                          v-if="epgHasIssue(epg)"
                          dense
                          color="orange-6"
                          text-color="white">
                          <q-icon name="warning" :class="$q.screen.gt.lg ? 'q-mr-xs' : ''" />
                          <span v-if="$q.screen.gt.lg">Needs attention</span>
                          <q-tooltip>
                            There was an issue with the last update.
                          </q-tooltip>
                        </q-chip>
                      </q-item-label>
                      <q-item-label lines="1">
                        <span class="text-grey-8">{{ epg.url }}</span>
                      </q-item-label>
                      <q-item-label caption lines="1">
                        <span v-if="epg.description" v-html="epg.description"></span>
                        <span v-else>No description</span>
                      </q-item-label>
                      <q-item-label v-if="epgHasIssue(epg)" caption class="text-warning">
                        Last update failed{{ epgErrorTime(epg) ? ` (${epgErrorTime(epg)})` : '' }}: {{ epgErrorMessage(
                        epg) }}
                      </q-item-label>
                    </q-item-section>

                    <q-item-section top side>
                      <TicListActions
                        :actions="epgActions(epg)"
                        @action="(action) => handleEpgAction(action, epg)"
                      />
                    </q-item-section>
                  </q-item>
                  <q-item v-if="!filteredEpgs.length">
                    <q-item-section>
                      <q-item-label class="text-grey-7">
                        No EPG sources found.
                      </q-item-label>
                    </q-item-section>
                  </q-item>
                </q-list>
              </div>
            </q-card-section>
          </q-card>

          <q-card flat>
            <q-card-section :class="$q.platform.is.mobile ? 'q-px-none' : ''">
              <q-form @submit.prevent="save" class="tic-form-layout">
                <h5 class="text-primary q-mt-none q-mb-none">Additional EPG Metadata</h5>

                <TicToggleInput
                  v-model="enableTmdbMetadata"
                  label="Fetch missing data from TMDB"
                />

                <div
                  v-if="enableTmdbMetadata"
                  class="sub-setting">
                  <q-skeleton
                    v-if="tmdbApiKey === null"
                    type="QInput" />
                  <TicTextInput
                    v-else
                    v-model="tmdbApiKey"
                    label="Your TMDB account API key"
                    description="Can be found at 'https://www.themoviedb.org/settings/api'."
                  />
                </div>

                <q-separator />

                <TicToggleInput
                  v-model="enableGoogleImageSearchMetadata"
                  label="Attempt to fetch missing programme images from Google Image Search"
                  description="This only fetches the first Google image result for the programme title when TMDB has no result."
                />

                <div>
                  <TicButton label="Save" icon="save" type="submit" color="positive" />
                </div>
              </q-form>
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
                    1. Add one or more program guides. Configure EPG sources with a name and URL.
                    URLs can be gzip compressed (.xml.gz) or uncompressed (.xml).
                  </q-item-label>
                </q-item-section>
              </q-item>
              <q-item>
                <q-item-section>
                  <q-item-label>
                    2. Click on the kebab menu for each added EPG and click on the <b>Update</b> button to fetch
                    the EPG and import it into Headendarr's database.
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
                Fetch missing data from TMDB:
              </q-item-label>
              <q-item>
                <q-item-section>
                  <q-item-label>
                    Configuring the background EPG builder to fetch missing data from TMDB will add significant time
                    to the process.
                    Everytime the programme guide is updated, this background EPG builder process will have to
                    re-fetch any images.
                  </q-item-label>
                </q-item-section>
              </q-item>

              <q-separator inset spaced />

              <q-item-label class="text-primary">
                Attempt to fetch missing programme images from Google Image Search:
              </q-item-label>
              <q-item>
                <q-item-section>
                  <q-item-label>
                    This will cause a lot of google image searches.
                    It is highly likely that you will flag your IP as a bot source with google if you enable this.
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
                    Adding an EPG source does not download it immediately. Use the kebab menu and click
                    <b>Update</b> to fetch and import the guide data.
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
import {useUiStore} from 'stores/ui';
import EpgInfoDialog from 'components/EpgInfoDialog.vue';
import EpgReviewDialog from 'components/EpgReviewDialog.vue';
import {
  TicButton,
  TicConfirmDialog,
  TicListActions,
  TicListToolbar,
  TicResponsiveHelp,
  TicTextInput,
  TicToggleInput,
} from 'components/ui';

export default defineComponent({
  name: 'EpgsPage',
  components: {
    TicButton,
    TicListActions,
    TicListToolbar,
    TicResponsiveHelp,
    TicTextInput,
    TicToggleInput,
  },

  setup() {
    return {
      uiStore: useUiStore(),
    };
  },
  data() {
    return {
      listOfEpgs: [],
      searchQuery: '',
      enableTmdbMetadata: null,
      tmdbApiKey: null,
      enableGoogleImageSearchMetadata: null,
      epgStatusPollTimer: null,
      epgStatusPollInFlight: false,
    };
  },
  computed: {
    filteredEpgs() {
      const query = (this.searchQuery || '').trim().toLowerCase();
      if (!query) {
        return this.listOfEpgs;
      }
      return this.listOfEpgs.filter((epg) => {
        const values = [
          epg?.name,
          epg?.url,
          epg?.description,
          epg?.health?.error,
        ];
        return values.some((value) => String(value || '').toLowerCase().includes(query));
      });
    },
  },
  methods: {
    epgHasIssue(epg) {
      return epg?.health?.status === 'error';
    },
    epgErrorMessage(epg) {
      const error = epg?.health?.error || 'Unknown download/import error';
      return error.length > 180 ? `${error.substring(0, 177)}...` : error;
    },
    epgErrorTime(epg) {
      const ts = epg?.health?.last_failure_at;
      if (!ts) {
        return '';
      }
      try {
        return new Date(ts * 1000).toLocaleString();
      } catch {
        return '';
      }
    },
    epgLastUpdatedAgo(epg, compactOverride = null) {
      const ts = Number(epg?.health?.last_success_at || 0);
      if (!ts) {
        return '';
      }
      const secondsAgo = Math.max(0, Math.floor(Date.now() / 1000) - ts);
      const compact = compactOverride === null ? this.$q.screen.lt.md : Boolean(compactOverride);
      if (secondsAgo < 60) {
        return compact ? 'now' : 'just now';
      }
      const minutes = Math.floor(secondsAgo / 60);
      if (minutes < 60) {
        return compact ? `${minutes}m` : `${minutes}m ago`;
      }
      const hours = Math.floor(minutes / 60);
      if (hours < 24) {
        return compact ? `${hours}h` : `${hours}h ago`;
      }
      const days = Math.floor(hours / 24);
      if (days < 7) {
        return compact ? `${days}d` : `${days}d ago`;
      }
      const weeks = Math.floor(days / 7);
      if (weeks < 5) {
        return compact ? `${weeks}w` : `${weeks}w ago`;
      }
      const months = Math.floor(days / 30);
      if (months < 12) {
        return compact ? `${months}mo` : `${months}mo ago`;
      }
      const years = Math.floor(days / 365);
      return compact ? `${years}y` : `${years}y ago`;
    },
    fetchEpgList: function({silent = false} = {}) {
      return axios({
        method: 'get',
        url: '/tic-api/epgs/get',
      }).then((response) => {
        const epgs = response.data.data || [];
        epgs.sort((a, b) => (a?.name ?? '').localeCompare(b?.name ?? '', undefined, {numeric: true}));
        this.listOfEpgs = epgs;
      }).catch(() => {
        if (!silent) {
          this.$q.notify({
            color: 'negative',
            position: 'top',
            message: 'Failed to fetch EPG list',
            icon: 'report_problem',
            actions: [{icon: 'close', color: 'white'}],
          });
        }
      });
    },
    fetchEpgMetadataSettings: function({silent = false} = {}) {
      return axios({
        method: 'get',
        url: '/tic-api/get-settings',
      }).then((response) => {
        this.enableTmdbMetadata = response.data.data.epgs.enable_tmdb_metadata;
        this.tmdbApiKey = response.data.data.epgs.tmdb_api_key;
        this.enableGoogleImageSearchMetadata = response.data.data.epgs.enable_google_image_search_metadata;
      }).catch(() => {
        if (!silent) {
          this.$q.notify({
            color: 'negative',
            position: 'top',
            message: 'Failed to fetch settings',
            icon: 'report_problem',
            actions: [{icon: 'close', color: 'white'}],
          });
        }
      });
    },
    fetchSettings: function({silent = false} = {}) {
      return Promise.all([
        this.fetchEpgList({silent}),
        this.fetchEpgMetadataSettings({silent}),
      ]);
    },
    startEpgStatusPolling() {
      this.stopEpgStatusPolling();
      this.epgStatusPollTimer = setInterval(() => {
        this.refreshEpgStatusInBackground();
      }, 30000);
    },
    stopEpgStatusPolling() {
      if (this.epgStatusPollTimer) {
        clearInterval(this.epgStatusPollTimer);
        this.epgStatusPollTimer = null;
      }
    },
    refreshEpgStatusInBackground: async function() {
      if (this.epgStatusPollInFlight) {
        return;
      }
      this.epgStatusPollInFlight = true;
      try {
        await this.fetchEpgList({silent: true});
      } finally {
        this.epgStatusPollInFlight = false;
      }
    },
    epgActions: function(epg) {
      const canReview = this.epgCanReview(epg);
      return [
        {
          id: 'review',
          icon: 'fact_check',
          label: 'Review',
          color: 'primary',
          disable: !canReview,
          tooltip: canReview
            ? `Review imported guide coverage for ${epg.name || 'EPG'}`
            : 'Review available after a successful update with imported guide data',
        },
        {id: 'update', icon: 'update', label: 'Update', color: 'info', tooltip: `Update ${epg.name || 'EPG'}`},
        {
          id: 'configure',
          icon: 'tune',
          label: 'Configure EPG',
          color: 'grey-8',
          tooltip: `Configure ${epg.name || 'EPG'}`,
        },
        {id: 'delete', icon: 'delete', label: 'Delete', color: 'negative', tooltip: 'Delete'},
      ];
    },
    handleEpgAction: function(action, epg) {
      if (action.id === 'review') {
        this.openEpgReview(epg);
        return;
      }
      if (action.id === 'update') {
        this.updateEpg(epg.id);
        return;
      }
      if (action.id === 'configure') {
        this.openEpgSettings(epg.id);
        return;
      }
      if (action.id === 'delete') {
        this.deleteEpg(epg.id);
      }
    },
    epgCanReview(epg) {
      const review = epg?.review || {};
      return Boolean(review.can_review);
    },
    openEpgReview(epg) {
      this.$q.dialog({
        component: EpgReviewDialog,
        componentProps: {
          epgId: epg.id,
          epgName: epg.name || '',
        },
      });
    },
    openEpgSettings: function(epgId) {
      if (!epgId) {
        epgId = null;
      }
      // Display the dialog
      this.$q.dialog({
        component: EpgInfoDialog,
        componentProps: {
          epgId: epgId,
        },
      }).onOk(() => {
        this.fetchSettings();
      }).onDismiss(() => {
      });
    },
    updateEpg: function(epgId) {
      // Fetch current settings
      this.$q.loading.show();
      axios({
        method: 'POST',
        url: '/tic-api/epgs/update/' + epgId,
      }).then(() => {
        this.$q.loading.hide();
        this.$q.notify({
          color: 'positive',
          icon: 'cloud_done',
          message: 'EPG update queued',
          timeout: 200,
        });
      }).catch(() => {
        this.$q.loading.hide();
        this.$q.notify({
          color: 'negative',
          position: 'top',
          message: 'Failed to queue EPG update',
          icon: 'report_problem',
          actions: [{icon: 'close', color: 'white'}],
        });
      });
    },
    deleteEpg: function(epgId) {
      const epg = this.listOfEpgs.find((item) => Number(item.id) === Number(epgId));
      const epgName = epg?.name || `EPG ${epgId}`;
      this.$q.dialog({
        component: TicConfirmDialog,
        componentProps: {
          title: 'Delete EPG?',
          message: `Delete "${epgName}"? This action is final and cannot be undone.`,
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
          url: `/tic-api/epgs/settings/${epgId}/delete`,
        }).then(() => {
          this.$q.loading.hide();
          this.fetchSettings();
          this.$q.notify({
            color: 'positive',
            icon: 'cloud_done',
            message: 'EPG deleted',
            timeout: 200,
          });
        }).catch(() => {
          this.$q.loading.hide();
          this.$q.notify({
            color: 'negative',
            position: 'top',
            message: 'Failed to delete EPG',
            icon: 'report_problem',
            actions: [{icon: 'close', color: 'white'}],
          });
        });
      });
    },
    save: function() {
      // Save settings
      const postData = {
        settings: {
          epgs: {
            enable_tmdb_metadata: this.enableTmdbMetadata,
            tmdb_api_key: this.tmdbApiKey,
            enable_google_image_search_metadata: this.enableGoogleImageSearchMetadata,
          },
        },
      };
      axios({
        method: 'POST',
        url: '/tic-api/save-settings',
        data: postData,
      }).then(() => {
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
    this.fetchSettings().finally(() => {
      this.startEpgStatusPolling();
    });
  },
  beforeUnmount() {
    this.stopEpgStatusPolling();
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

.tic-form-layout > *:not(:last-child) {
  margin-bottom: 24px;
}
</style>
