<template>
  <q-dialog
    ref="channelGroupSelectorDialogRef"
    :maximized="$q.platform.is.mobile"
    transition-show="jump-left"
    transition-hide="jump-right"
    full-height
    @hide="onDialogHide">

    <q-card
      v-touch-swipe.touch.left.right="hide"
      :style="$q.platform.is.mobile ? 'max-width: 100vw;' : 'max-width: 95vw;'"
      style="width:1000px;">

      <q-card-section class="bg-card-head">
        <div class="row items-center no-wrap">
          <div
            v-if="$q.platform.is.mobile"
            class="col">
            <q-btn
              v-if="selected.length > 0"
              color="grey-7"
              dense
              round
              flat
              icon="publish"
              v-close-popup />
            <q-btn
              v-else
              color="grey-7"
              dense
              round
              flat
              icon="arrow_back"
              v-close-popup />
          </div>

          <div class="col">
            <div class="text-h6 text-blue-10">
              Select Groups From Source
            </div>
          </div>

          <div
            v-if="!$q.platform.is.mobile"
            class="col-auto">
            <q-btn
              v-if="selected.length > 0"
              color="grey-7"
              dense
              round
              flat
              icon="publish"
              v-close-popup>
              <q-tooltip class="bg-white text-primary">Import Selected Groups As Channels</q-tooltip>
            </q-btn>
            <q-btn
              v-else
              color="grey-7"
              dense
              round
              flat
              icon="close_fullscreen"
              v-close-popup>
              <q-tooltip class="bg-white text-primary">Close</q-tooltip>
            </q-btn>
          </div>
        </div>
      </q-card-section>

      <q-card-section :class="$q.platform.is.mobile ? 'q-px-none' : ''">
        <q-card flat>
          <q-card-section class="q-pa-none">
            <div class="q-pa-none">
              <q-table
                ref="tableRef"
                :style="tableStyle"
                flat bordered
                title="Groups"
                :rows="rows"
                :columns="columns"
                :filter="filter"
                v-model:pagination="pagination"
                :rows-per-page-options="[15]"
                :loading="loading"
                :row-key="genRowIndex"
                :selected-rows-label="getSelectedString"
                v-model:selected="selected"
                selection="multiple"
                binary-state-sort
                @request="fetchGroupsList"
                no-data-label="No groups found in source"
                no-results-label="The filter didn't uncover any results"
              >
                <template v-slot:top-left>
                  <q-select
                    dense
                    outlined
                    v-model="selectedPlaylist"
                    :options="playlistOptions"
                    label="Source"
                    option-label="name"
                    option-value="id"
                    emit-value
                    map-options
                    style="min-width: 250px"
                    @update:model-value="playlistChanged"
                  />
                </template>

                <template v-slot:top-right>
                  <q-input outlined dense debounce="300" v-model="filter" placeholder="Search">
                    <template v-slot:append>
                      <q-icon name="search" />
                    </template>
                  </q-input>
                </template>

                <template v-slot:no-data="{ icon, message, filter }">
                  <div class="full-width row flex-center text-primary q-gutter-sm">
                    <q-icon size="2em" :name="filter ? 'filter_b_and_w' : icon" />
                    <span>{{ message }}</span>
                    <q-icon size="2em" :name="filter ? 'filter_b_and_w' : icon" />
                  </div>
                </template>

                <template v-slot:body="props">
                  <q-tr :props="props">
                    <q-td auto-width>
                      <q-toggle v-model="props.selected" color="primary" />
                    </q-td>
                    <q-td key="name" :props="props"
                          style="max-width: 250px; white-space: normal; word-wrap: break-word;">
                      {{ props.row.name }}
                    </q-td>
                    <q-td key="channel_count" :props="props">
                      <q-badge color="green">
                        {{ props.row.channel_count }} channels
                      </q-badge>
                    </q-td>
                    <q-td key="playlist_name" :props="props">
                      <q-badge color="blue">
                        {{ props.row.playlist_name }}
                      </q-badge>
                    </q-td>
                  </q-tr>
                </template>
              </q-table>
            </div>
          </q-card-section>
        </q-card>
      </q-card-section>
    </q-card>
  </q-dialog>
</template>

<script>
import axios from 'axios';
import {ref} from 'vue';

const columns = [
  {name: 'name', required: true, align: 'left', label: 'Group Name', field: 'name', sortable: true},
  {name: 'channel_count', required: true, align: 'center', label: 'Channels', field: 'channel_count', sortable: true},
  {name: 'playlist_name', required: true, align: 'left', label: 'Source', field: 'playlist_name', sortable: true},
];

export default {
  name: 'ChannelGroupSelectorDialog',
  emits: [
    'ok', 'hide',
  ],
  methods: {
    // following method is REQUIRED
    // (don't change its name --> "show")
    show() {
      this.$refs.channelGroupSelectorDialogRef.show();
      this.fetchPlaylists();
    },

    // following method is REQUIRED
    // (don't change its name --> "hide")
    hide() {
      this.$refs.channelGroupSelectorDialogRef.hide();
    },

    onDialogHide() {
      // required to be emitted
      // when QDialog emits "hide" event
      const returnItems = [];

      for (const i in this.selected) {
        let selected = this.selected[i];
        returnItems.push({
          'group_name': selected.name,
          'playlist_id': selected.playlist_id,
          'playlist_name': selected.playlist_name,
          'channel_count': selected.channel_count,
        });
      }

      console.log('Dialog returning items:', returnItems);

      this.$emit('ok', {selectedGroups: returnItems});
      this.$emit('hide');
    },

    fetchPlaylists() {
      axios({
        method: 'GET',
        url: '/tic-api/playlists/get',
      }).then((response) => {
        this.playlistOptions = response.data.data.map(playlist => ({
          id: playlist.id,
          name: playlist.name,
        }));

        if (this.playlistOptions.length > 0) {
          this.selectedPlaylist = this.playlistOptions[0].id;
          this.fetchGroupsList({
            pagination: this.pagination,
            filter: this.filter,
          });
        }
      }).catch((error) => {
        console.error('Error fetching playlists:', error);
        this.$q.notify({
          color: 'negative',
          position: 'top',
          message: 'Failed to fetch sources',
          icon: 'report_problem',
          actions: [{icon: 'close', color: 'white'}],
        });
      });
    },

    playlistChanged() {
      this.selected = []; // Clear selections when playlist changes
      this.fetchGroupsList({
        pagination: this.pagination,
        filter: this.filter,
      });
    },

    fetchGroupsList: function(props) {
      if (!this.selectedPlaylist) return;

      const {page, rowsPerPage, sortBy, descending} = props.pagination;
      const filter = props.filter;

      // Show loading animation
      this.loading = true;

      // Calculate starting row of data
      const startRow = (page - 1) * rowsPerPage;

      // Prepare data for API request
      let data = {
        start: startRow,
        length: rowsPerPage,
        search_value: filter,
        order_by: sortBy,
        order_direction: descending ? 'desc' : 'asc',
        playlist_id: this.selectedPlaylist,
      };

      // Fetch groups from server
      this.rows = [];
      axios({
        method: 'POST',
        url: '/tic-api/playlists/groups',
        data: data,
      }).then((response) => {
        // Important fix: update rowsNumber with the actual total count
        this.pagination.rowsNumber = response.data.data.total || 0;

        // Get playlist name for the selected playlist
        const playlist = this.playlistOptions.find(p => p.id === this.selectedPlaylist);
        const playlistName = playlist ? playlist.name : '';

        // Process returned data
        const returnedData = [];
        for (let i = 0; i < response.data.data.groups.length; i++) {
          let group = response.data.data.groups[i];
          returnedData.push({
            name: group.name,
            channel_count: group.channel_count || 0,
            playlist_id: this.selectedPlaylist,
            playlist_name: playlistName,
          });
        }

        // Clear out existing data and add new
        this.rows.splice(0, this.rows.length, ...returnedData);

        // Update local pagination object
        this.pagination.page = page;
        this.pagination.rowsPerPage = rowsPerPage;
        this.pagination.sortBy = sortBy;
        this.pagination.descending = descending;

        // Hide loading animation
        this.loading = false;
      }).catch((error) => {
        console.error('Error fetching groups:', error);
        this.loading = false;

        // Reset pagination if there's an error to prevent navigation issues
        this.pagination.page = 1;
        this.rows = [];
        this.pagination.rowsNumber = 0;

        this.$q.notify({
          color: 'negative',
          position: 'top',
          message: 'Failed to fetch groups from source',
          icon: 'report_problem',
          actions: [{icon: 'close', color: 'white'}],
        });
      });
    },

    getSelectedString: function() {
      return this.selected.length === 0 ?
        '' :
        `${this.selected.length} group${this.selected.length > 1 ? 's' : ''} selected of ${this.rows.length}`;
    },

    genRowIndex: function(row) {
      return `${row.playlist_id}-${row.name}`;
    },
  },

  // Update initial pagination settings
  pagination: ref({
    sortBy: 'name',
    descending: false,
    page: 1,
    rowsPerPage: 15,
    rowsNumber: 0,  // Start with 0 until we get the real count
  }),

  computed: {
    tableStyle() {
      if (!this.$refs.channelGroupSelectorDialogRef) return '';

      const dialogHeight = this.$refs.channelGroupSelectorDialogRef.$el.offsetHeight;
      const tableHeight = (dialogHeight - 150);
      return `height:${tableHeight}px;`;
    },
  },
  data: function() {
    return {
      columns,
      rows: ref([]),
      filter: ref(''),
      selected: ref([]),
      playlistOptions: [],
      selectedPlaylist: null,
      pagination: ref({
        sortBy: 'name',
        descending: false,
        page: 1,
        rowsPerPage: 15,
        rowsNumber: 10,
      }),
      loading: ref(false),
    };
  },
};
</script>
