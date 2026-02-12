<template>
  <q-page padding>

    <div class="q-pa-none">

      <div class="row">
        <div :class="uiStore.showHelp ? 'col-sm-7 col-md-8 help-main' : 'col-12 help-main help-main--full'">

          <q-card flat>
            <q-card-section :class="$q.platform.is.mobile ? 'q-px-none' : ''">
              <div class="row q-gutter-xs q-mt-xs justify-between">
                <div class="col-auto">
                  <q-btn-group v-if="bulkEditMode !== true">
                    <q-btn
                      @click="openChannelSettings(null)"
                      class=""
                      color="primary"
                      icon-right="add"
                      label="Add Channel" />
                  </q-btn-group>
                  <q-btn-group v-if="bulkEditMode !== true" class="q-ml-sm">
                    <q-btn
                      @click="openChannelsImport()"
                      class=""
                      color="primary"
                      icon-right="dvr"
                      label="Import Channels from stream source" />
                  </q-btn-group>

                  <q-btn-group v-if="bulkEditMode !== true" class="q-ml-sm">
                    <q-btn
                      @click="openChannelsGroupImport()"
                      class=""
                      color="primary"
                      icon-right="group_work"
                      label="Import Channels by Group" />
                  </q-btn-group>

                  <div v-if="bulkEditMode === true" class="q-mt-md full-width">
                    <div class="row q-gutter-sm">
                      <q-btn
                        @click="showBulkEditCategoriesDialog()"
                        :disabled="!anyChannelsSelectedInBulkEdit"
                        color="primary"
                        label="Edit Categories" />

                      <q-btn
                        @click="selectAllChannels()"
                        color="primary"
                        :label="allChannelsSelected ? 'Deselect All' : 'Select All'" />

                      <q-btn-dropdown
                        color="primary"
                        label="Select by Category">
                        <q-list>
                          <q-item
                            v-for="category in availableCategories"
                            :key="category"
                            clickable
                            @click="selectChannelsByCategory(category)">
                            <q-item-section>{{ category }}</q-item-section>
                          </q-item>
                        </q-list>
                      </q-btn-dropdown>

                      <q-btn
                        @click="triggerRefreshChannelSources()"
                        :disabled="!anyChannelsSelectedInBulkEdit"
                        color="primary"
                        label="Refresh Channel Streams" />

                      <q-btn
                        @click="triggerDeleteChannels()"
                        :disabled="!anyChannelsSelectedInBulkEdit"
                        color="primary"
                        label="Delete Channels" />
                    </div>
                  </div>

                </div>
                <div class="col-auto">
                  <q-btn
                    @click="bulkEditMode = !bulkEditMode"
                    class=""
                    color="primary"
                    :icon-right="bulkEditMode ? `format_line_spacing` : `fact_check`"
                    :label="bulkEditMode ? `Exit Bulk Edit` : `Bulk Edit`" />
                  <!--<q-btn
                     @click="exportChannels()"
                     class=""
                     color="primary"
                     icon-right="file_download"
                     label="Export" />-->
                  <q-dialog
                    v-model="chanelExportDialog"
                    full-width
                    transition-show="scale"
                    transition-hide="scale">
                    <q-card style="max-width: 95vw; width:900px;">
                      <q-card-section class="bg-card-head">
                        <div class="row items-center no-wrap">
                          <div class="col">
                            <div class="text-h6 text-blue-10">
                              Export Config
                            </div>
                            <div>
                              The JSON below contains the total config for TIC. It can be copied, modified and then
                              re-imported to replace the current configuration.
                            </div>
                          </div>
                          <q-btn
                            color="positive"
                            dense
                            round
                            flat
                            icon="save"
                            @click="importConfigJson">
                            <q-tooltip class="bg-white text-primary">Save</q-tooltip>
                          </q-btn>
                          <q-btn
                            color="info"
                            dense
                            round
                            flat
                            icon="copy_all"
                            @click="copyExportJson">
                            <q-tooltip class="bg-white text-primary">Copy Text</q-tooltip>
                          </q-btn>
                          <q-btn
                            color="grey-7"
                            dense
                            round
                            flat
                            icon="close"
                            v-close-popup>
                            <q-tooltip class="bg-white text-primary">Close/Discard</q-tooltip>
                          </q-btn>
                        </div>
                      </q-card-section>

                      <q-card-section class="q-pt-none">

                        <q-card
                          flat bordered
                          class="q-pa-sm"
                          style="width:100%">

                          <q-card-section class="q-pt-none">
                            <!--                        <pre >{{ chanelExportDialogJson }}</pre>-->
                            <q-input
                              v-model="chanelExportDialogJson"
                              rows="60"
                              type="textarea"
                              autogrow
                            />
                          </q-card-section>
                        </q-card>

                      </q-card-section>
                    </q-card>
                  </q-dialog>
                </div>
              </div>
            </q-card-section>

            <q-card-section :class="$q.platform.is.mobile ? 'q-px-none' : ''">
              <div class="q-gutter-sm">

                <!--CHANNEL NUMBER EDIT DIALOG-->
                <q-dialog v-model="channelNumberEditDialogVisible">
                  <q-card>
                    <q-card-section>
                      <q-input
                        label="Edit value"
                        v-model="editedValue"
                        ref="input"
                        type="number"
                        @keydown.enter="submitChannelNumberChange"
                      />
                    </q-card-section>

                    <q-card-actions align="right">
                      <q-btn label="Cancel" @click="channelNumberEditDialogVisible = false" />
                      <q-btn label="Save" color="primary" @click="submitChannelNumberChange" />
                    </q-card-actions>
                  </q-card>
                </q-dialog>

                <!--BULK EDIT DIALOG-->

                <q-dialog v-model="bulkEditCategoriesDialogVisible">
                  <q-card style="width: 700px; max-width: 80vw;">
                    <q-card-section class="bg-grey-8 text-white">
                      <div class="text-h6">Bulk Edit Categories</div>
                    </q-card-section>

                    <!-- Apply Categories Select Menu -->
                    <q-card-section class="q-my-sm q-mx-xl">
                      <q-select
                        v-model="applyCategoriesAction"
                        :options="applyCategoriesOptions"
                        label="Apply Categories Action"
                        outlined
                        hint="See below for details on actions"
                      >
                      </q-select>
                    </q-card-section>

                    <!-- Categories Input -->
                    <q-card-section class="q-my-sm q-mx-xl">
                      <div class="q-gutter-sm">
                        <q-select
                          use-input
                          use-chips
                          multiple
                          hide-dropdown-icon
                          input-debounce="0"
                          new-value-mode="add-unique"
                          v-model="bulkEditCategories"
                          :label="`Categories to ` + applyCategoriesAction"
                        />
                      </div>
                    </q-card-section>

                    <!-- Explanation Text -->
                    <q-card-section class="q-my-sm q-mx-xl text-grey-8">
                      <p><b>Add:</b> Add the categories to the existing list of categories.</p>
                      <p><b>Remove:</b> Remove the entered categories from the channels.</p>
                      <p><b>Replace:</b> Replace the categories with the entered categories.</p>
                      <p>Enter no categories to clear all categories from the channels.</p>
                    </q-card-section>

                    <q-card-actions align="right">
                      <q-btn label="Cancel" @click="bulkEditCategoriesDialogVisible = false" />
                      <q-btn label="Save" color="primary" @click="submitBulkCategoriesChange" />
                    </q-card-actions>
                  </q-card>
                </q-dialog>

                <q-list
                  bordered
                  class="rounded-borders q-pl-none"
                  style="">

                  <draggable
                    group="channels"
                    item-key="number"
                    handle=".handle"
                    :component-data="{ tag: 'ul', name: 'flip-list', type: 'transition' }"
                    v-model="listOfChannels"
                    v-bind="dragOptions"
                    @change="setChannelOrder"
                  >
                    <template #item="{ element, index }">
                      <q-item
                        :key="index"
                        class="q-px-none rounded-borders"
                        :class="channelRowClass(element)">

                        <!--START DRAGGABLE HANDLE-->
                        <q-item-section avatar class="q-px-sm q-mx-sm handle">
                          <q-toggle
                            v-if="bulkEditMode === true"
                            v-model="element.selected"
                            @click="toggleSelection(element)" />
                          <q-avatar
                            v-else
                            rounded>
                            <q-icon name="format_line_spacing" class="" style="max-width: 30px;cursor: grab;">
                              <q-tooltip class="bg-white text-primary">Drag to move and re-order</q-tooltip>
                            </q-icon>
                          </q-avatar>
                        </q-item-section>
                        <!--END DRAGGABLE HANDLE-->

                        <q-separator inset vertical class="gt-xs" />

                        <!--START CHANNEL NUMBER-->
                        <q-item-section
                          class="q-px-sm q-mx-sm cursor-pointer"
                          style="max-width:80px;"
                          @click="bulkEditMode ? element.selected = !element.selected : showChannelNumberMod(index)">
                          <q-item-label lines="1" class="text-left">
                            <span class="q-ml-sm">Channel</span>
                          </q-item-label>
                          <q-item-label caption lines="1"
                                        style="text-decoration: underline; "
                                        class="text-left text-primary q-ml-sm">
                            {{ element.number }}
                          </q-item-label>
                        </q-item-section>
                        <!--END CHANNEL NUMBER-->

                        <q-separator inset vertical class="gt-xs" />

                        <!--START NAME / DESCRIPTION-->
                        <q-item-section
                          top class="q-mx-md"
                          @click="bulkEditMode ? element.selected = !element.selected : ``">
                          <q-item-label lines="1" class="text-left">
                            <q-avatar rounded size="35px">
                              <q-img :src="element.logo_url" class="" style="max-width: 30px;" />
                            </q-avatar>

                            <span class="text-weight-medium text-primary q-ml-sm">{{ element.name }}</span>
                          </q-item-label>
                          <q-item-label caption lines="1" class="text-left q-ml-none">
                            <div class="row">
                              <div class="col-sm-3">
                                Stream sources:
                              </div>
                              <div class="col-sm-9">
                                {{ Object.keys(element.sources).map(key => element.sources[key].playlist_name) }}
                              </div>
                            </div>
                            <div class="row">
                              <div class="col-sm-3">
                                Categories:
                              </div>
                              <div class="col-sm-9">
                                {{ element.tags }}
                              </div>
                            </div>
                          </q-item-label>
                        </q-item-section>
                        <!--END NAME / DESCRIPTION-->

                        <q-separator inset vertical class="gt-xs" />

                        <!--START EPG DETAILS-->
                        <q-item-section
                          class="q-px-sm q-mx-sm"
                          @click="bulkEditMode ? element.selected = !element.selected : ``">
                          <q-item-label lines="1" class="text-left">
                            <div class="row items-center no-wrap">
                              <span class="q-ml-sm">Guide</span>
                              <q-space />
                              <div class="column items-end q-gutter-xs">
                                <q-chip
                                  v-if="enableChannelHealthHighlight && element.status && element.status.state === 'warning'"
                                  dense
                                  color="orange-6"
                                  text-color="white"
                                  clickable
                                  @click.stop="openChannelIssuesDialog(element)">
                                  <q-icon name="warning" :class="$q.screen.gt.lg ? 'q-mr-xs' : ''" />
                                  <span v-if="$q.screen.gt.lg">Needs attention</span>
                                  <q-tooltip class="bg-white text-primary">
                                    {{ channelIssueFirstLabel(element.status) }}
                                  </q-tooltip>
                                </q-chip>
                                <q-chip
                                  v-if="enableChannelHealthHighlight && element.status && element.status.suggestion_count > 0"
                                  dense
                                  color="green-6"
                                  text-color="white"
                                  clickable
                                  @click.stop="openChannelSuggestionsDialog(element)">
                                  <q-icon name="tips_and_updates" :class="$q.screen.gt.lg ? 'q-mr-xs' : ''" />
                                  <span v-if="$q.screen.gt.lg">Stream suggestions</span>
                                  <q-tooltip class="bg-white text-primary">
                                    Potential matching streams are available for this channel.
                                  </q-tooltip>
                                </q-chip>
                              </div>
                            </div>
                          </q-item-label>
                          <q-item-label caption lines="1" class="text-left q-ml-sm">
                            {{ element.guide.epg_name }} - {{ element.guide.channel_id }}
                          </q-item-label>
                        </q-item-section>
                        <!--END EPG DETAILS-->

                        <q-separator inset vertical class="gt-xs" />

                        <q-item-section side class="q-mr-md">
                          <div class="text-grey-8 q-gutter-xs">
                            <q-btn
                              size="12px"
                              flat dense round
                              icon="play_arrow"
                              @click="previewChannel(element)">
                              <q-tooltip class="bg-white text-primary">Preview Stream</q-tooltip>
                            </q-btn>
                            <q-btn
                              size="12px"
                              flat dense round
                              icon="link"
                              @click="copyStreamUrl(element)">
                              <q-tooltip class="bg-white text-primary">Copy Stream URL</q-tooltip>
                            </q-btn>
                            <q-btn
                              size="12px"
                              flat dense round
                              icon="tune"
                              @click="openChannelSettings(element)">
                              <q-tooltip class="bg-white text-primary">Configure</q-tooltip>
                            </q-btn>
                          </div>
                        </q-item-section>

                      </q-item>
                    </template>
                  </draggable>

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
                        1. Start by clicking the <b>Import Channels from stream source</b> button. With this dialog
                        open,
                        select one or more streams from your imported stream sources, then close the dialog to import
                        them into
                        your channel list.
                      </q-item-label>
                    </q-item-section>
                  </q-item>
                  <q-item>
                    <q-item-section>
                      <q-item-label>
                        2. Click on the <b>Configure</b>
                        (
                        <q-icon name="tune" />
                        )
                        button for each added channel.
                        <br>
                        In the Channel Settings dialog that opens you can further configure channel categories and
                        additional streams from other sources.
                      </q-item-label>
                    </q-item-section>
                  </q-item>
                  <q-item>
                    <q-item-section>
                      <q-item-label>
                        3. Click and hold the drag
                        (
                        <q-icon name="format_line_spacing" />
                        )
                        icon to quickly change the order of your channel list.
                      </q-item-label>
                    </q-item-section>
                  </q-item>
                  <q-item>
                    <q-item-section>
                      <q-item-label>
                        4. Click a channel's number to open the channel number editor.
                      </q-item-label>
                    </q-item-section>
                  </q-item>
                  <q-item>
                    <q-item-section>
                      <q-item-label>
                        5. Click the <b>Bulk Edit</b> button above the channel list to modify multiple channels at once.
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
                    Channel Settings - Streams:
                  </q-item-label>
                  <q-item>
                    <q-item-section>
                      <q-item-label>
                        When you open a channel's settings, you can configure multiple streams for each channel.
                        Drag the streams in order of preference. If a stream has reached the connection limits,
                        the next stream will be used automatically.
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

<style scoped>
pre {
  white-space: pre-wrap;
  overflow-wrap: anywhere;
  overflow: hidden;
}

textarea.q-field__native {
  min-height: 200px;
}
</style>

<script>
import {defineComponent, ref} from 'vue';
import axios from 'axios';
import draggable from 'vuedraggable';
import {useUiStore} from 'stores/ui';
import {useVideoStore} from 'stores/video';
import ChannelInfoDialog from 'components/ChannelInfoDialog.vue';
import ChannelStreamSelectorDialog from 'components/ChannelStreamSelectorDialog.vue';
import ChannelGroupSelectorDialog from 'components/ChannelGroupSelectorDialog.vue';
import ChannelSuggestionsDialog from 'components/ChannelSuggestionsDialog.vue';
import ChannelIssuesDialog from 'components/ChannelIssuesDialog.vue';
import {copyToClipboard} from 'quasar';

export default defineComponent({
  name: 'ChannelsPage',
  components: {
    draggable,
  },

  setup() {
    const uiStore = useUiStore();
    const videoStore = useVideoStore();
    return {
      uiStore,
      videoStore,
    };
  },
  data() {
    return {
      bulkEditMode: false,
      chanelExportDialog: false,
      chanelExportDialogJson: '',
      options: {
        dropzoneSelector: '.q-list',
        draggableSelector: '.q-item',
      },
      listOfChannels: [],
      enableChannelHealthHighlight: true,
      selectedChannels: [],

      channelNumberEditDialogVisible: false,
      bulkEditCategoriesDialogVisible: false,
      //newCategory: ref(''),
      bulkEditCategories: [],
      applyCategoriesAction: 'Add', // Selected action
      applyCategoriesOptions: ['Add', 'Remove', 'Replace'], // Options for select menu
      editIndex: '',
      editedValue: '',
    };
  },
  computed: {
    dragOptions() {
      return {
        animation: 100,
        group: 'pluginFlow',
        disabled: false,
        ghostClass: 'ghost',
        direction: 'vertical',
        delay: 200,
        delayOnTouchOnly: true,
      };
    },
    allChannelsSelected() {
      return this.listOfChannels.length > 0 &&
        this.listOfChannels.every(channel => channel.selected);
    },
    anyChannelsSelectedInBulkEdit() {
      // Check if any channels are selected
      return this.listOfChannels.some(channel => channel.selected);
    },
    availableCategories() {
      // Extract all unique categories from channels
      const allCategories = new Set();
      this.listOfChannels.forEach(channel => {
        if (channel.tags && Array.isArray(channel.tags)) {
          channel.tags.forEach(tag => {
            if (tag) allCategories.add(tag);
          });
        }
      });
      return Array.from(allCategories).sort();
    },
  },
  methods: {
    generateNewChannel: function(range, usedValues) {
      for (let i = range[0]; i <= range[1]; i++) {
        if (!usedValues.includes(i)) {
          return i;
        }
      }
      return null;
    },
    selectAllChannels() {
      const shouldSelect = !this.allChannelsSelected;

      // Toggle selection on all channels
      this.listOfChannels.forEach(channel => {
        channel.selected = shouldSelect;

        if (shouldSelect) {
          if (!this.selectedChannels.includes(channel.id)) {
            this.selectedChannels.push(channel.id);
          }
        } else {
          this.selectedChannels = this.selectedChannels.filter(id => id !== channel.id);
        }
      });

      // Show notification
      this.$q.notify({
        color: 'positive',
        message: shouldSelect
          ? `Selected all ${this.listOfChannels.length} channels`
          : 'Deselected all channels',
        icon: shouldSelect ? 'select_all' : 'deselect',
        timeout: 2000,
      });
    },
    selectChannelsByCategory(category) {
      let count = 0;

      // Loop through channels and select those with the specified category
      this.listOfChannels.forEach(channel => {
        if (channel.tags && channel.tags.includes(category)) {
          channel.selected = true;
          if (!this.selectedChannels.includes(channel.id)) {
            this.selectedChannels.push(channel.id);
          }
          count++;
        }
      });

      // Show notification
      this.$q.notify({
        color: 'positive',
        message: `Selected ${count} channels in category "${category}"`,
        icon: 'category',
        timeout: 2000,
      });
    },
    async previewChannel(channel) {
      try {
        const response = await axios.get(`/tic-api/channels/${channel.id}/preview`);
        if (response.data.success) {
          this.videoStore.showPlayer({
            url: response.data.preview_url,
            title: channel.name,
            type: response.data.stream_type || 'auto',
          });
          return;
        }
        this.$q.notify({color: 'negative', message: response.data.message || 'Failed to load preview'});
      } catch (error) {
        console.error('Preview channel error:', error);
        this.$q.notify({color: 'negative', message: 'Failed to load preview'});
      }
    },
    async copyStreamUrl(channel) {
      try {
        const response = await axios.get(`/tic-api/channels/${channel.id}/preview`);
        if (response.data.success) {
          await copyToClipboard(response.data.preview_url);
          this.$q.notify({color: 'positive', message: 'Stream URL copied'});
          return;
        }
        this.$q.notify({color: 'negative', message: response.data.message || 'Failed to copy stream URL'});
      } catch (error) {
        console.error('Copy stream URL error:', error);
        this.$q.notify({color: 'negative', message: 'Failed to copy stream URL'});
      }
    },
    updateNumbers: function(myList, index) {
      for (let i = index + 1; i < myList.length; i++) {
        myList[i].number += 1;
      }
    },
    insertNumberIncrement: function(list, newItem) {
      let inserted = false;
      let conflict = false;
      let lastNumber = 0;
      let newList = [];
      for (let i = 0; i < list.length; i++) {
        const item = list[i];
        if (item.number < newItem.number) {
          newList.push(item);
        } else if (item.number === newItem.number) {
          conflict = true;
          newList.push(newItem);
          inserted = true;
          item.number++;
          newList.push(item);
        } else if (item.number === lastNumber) {
          item.number++;
          newList.push(item);
        } else if (item.number > newItem.number && !inserted) {
          newList.push(newItem);
          inserted = true;
          newList.push(item);
        } else if (item.number > newItem.number) {
          newList.push(item);
        }
        lastNumber = item.number;
      }
      if (!inserted) {
        newList.push(newItem);
      }
      return newList;
    },
    shiftChannelNumber: function(list, movedItemId) {
      let lastNumber = 999;
      for (let i = 0; i < list.length; i++) {
        const item = list[i];
        if (item.id === movedItemId) {
          item.number = parseInt(lastNumber) + 1;
        }
        lastNumber = parseInt(item.number);
      }
    },
    fixNumberIncrement: function(list) {
      let sortedList = list.sort((a, b) => a.number - b.number);
      let lastNumber = 999;
      let newList = [];
      for (let i = 0; i < sortedList.length; i++) {
        const item = sortedList[i];
        if (isNaN(item.number)) {
          item.number = (lastNumber + 1);
          newList.push(item);
        } else if (parseInt(item.number) === parseInt(lastNumber)) {
          item.number++;
          newList.push(item);
        } else if (parseInt(item.number) > parseInt(lastNumber)) {
          newList.push(item);
        } else if (parseInt(item.number) < parseInt(lastNumber)) {
          item.number = (lastNumber + 1);
          newList.push(item);
        } else {
          console.error('--- Missed item ---');
          console.error(lastNumber);
          console.error(item.number);
          console.error((item.number === lastNumber));
          console.error('-------------------');
        }
        lastNumber = item.number;
      }
      list = newList;
      console.debug(list);
    },
    nextAvailableChannelNumber: function(list) {
      let sortedList = list.sort((a, b) => a.number - b.number);
      let lastNumber = 999;
      for (let i = 0; i < sortedList.length; i++) {
        const item = sortedList[i];
        if (parseInt(item.number) > parseInt(lastNumber + 1)) {
          return lastNumber + 1;
        }
        lastNumber = parseInt(item.number);
      }
      return lastNumber + 1;
    },
    fetchChannels: function() {
      // Fetch current settings
      axios({
        method: 'GET',
        url: `/tic-api/channels/get?include_status=${this.enableChannelHealthHighlight ? 'true' : 'false'}`,
      }).then((response) => {
        // Map and sort channels, preserving selected status
        this.listOfChannels = response.data.data.sort((a, b) => a.number - b.number).map(channel => {
          // Check if channel ID exists in selectedChannels
          const isSelected = this.selectedChannels.includes(channel.id);
          return {...channel, selected: isSelected};
        });
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
    fetchUiSettings: function() {
      axios({
        method: 'get',
        url: '/tic-api/get-settings',
      }).then((response) => {
        const uiSettings = response.data.data?.ui_settings || {};
        this.enableChannelHealthHighlight = uiSettings.enable_channel_health_highlight !== false;
        this.fetchChannels();
      }).catch(() => {
        this.enableChannelHealthHighlight = true;
        this.fetchChannels();
      });
    },
    channelRowClass: function(channel) {
      if (!this.enableChannelHealthHighlight) {
        return channel.enabled ? '' : 'bg-grey-3';
      }
      if (!channel.enabled) {
        return 'bg-grey-3';
      }
      if (channel.status && channel.status.state === 'warning') {
        return 'channel-needs-attention';
      }
      return '';
    },
    channelIssueLabels: function(status) {
      if (!status || !status.issues || !status.issues.length) {
        return [];
      }
      const labels = {
        no_sources: 'No streams',
        all_sources_disabled: 'All streams disabled',
        missing_tvh_mux: 'Missing stream in TVHeadend',
        tvh_mux_failed: 'TVHeadend stream failed',
        channel_logo_unavailable: 'Channel logo unavailable',
      };
      return status.issues.map(issue => labels[issue] || issue);
    },
    channelIssueFirstLabel: function(status) {
      const labels = this.channelIssueLabels(status);
      return labels.length ? labels[0] : '';
    },
    openChannelIssuesDialog: function(channel) {
      if (!channel?.status?.issues?.length) {
        return;
      }
      this.$q.dialog({
        component: ChannelIssuesDialog,
        componentProps: {
          channel,
          issues: channel.status.issues,
        },
      }).onOk((payload) => {
        if (payload?.openSettings) {
          this.openChannelSettings(channel);
          return;
        }
        if (payload?.refresh) {
          this.fetchChannels();
        }
      }).onDismiss(() => {
        this.fetchChannels();
      }).onCancel(() => {
        this.fetchChannels();
      });
    },
    openChannelSettings: function(channel) {
      let channelId = null;
      let newChannelNumber = null;
      if (!channel) {
        newChannelNumber = this.nextAvailableChannelNumber(this.listOfChannels);
      } else {
        channelId = channel.id;
      }
      // Display the dialog
      this.$q.dialog({
        component: ChannelInfoDialog,
        componentProps: {
          channelId: channelId,
          newChannelNumber: newChannelNumber,
        },
      }).onOk((payload) => {
        this.fetchChannels();
      }).onDismiss(() => {
      });
    },
    openChannelSuggestionsDialog: function(channel) {
      if (!channel) {
        return;
      }
      this.$q.dialog({
        component: ChannelSuggestionsDialog,
        componentProps: {
          channelId: channel.id,
        },
      }).onOk((payload) => {
        if (payload?.openSettings) {
          this.openChannelSettings(channel);
          return;
        }
        if (payload?.refresh) {
          this.fetchChannels();
        }
      }).onDismiss(() => {
        this.fetchChannels();
      });
    },
    openChannelsImport: function() {
      this.$q.dialog({
        component: ChannelStreamSelectorDialog,
        componentProps: {
          hideStreams: [],
        },
      }).onOk((payload) => {
        if (typeof payload.selectedStreams !== 'undefined' && payload.selectedStreams !== null) {
          // Add selected stream to list
          this.$q.loading.show();
          // Send changes to backend
          let data = {
            channels: [],
          };
          console.log(payload.selectedStreams);
          for (const i in payload.selectedStreams) {
            data.channels.push({
              playlist_id: payload.selectedStreams[i].playlist_id,
              playlist_name: payload.selectedStreams[i].playlist_name,
              stream_id: payload.selectedStreams[i].id,
              stream_name: payload.selectedStreams[i].stream_name,
            });
          }
          axios({
            method: 'POST',
            url: '/tic-api/channels/settings/multiple/add',
            data: data,
          }).then((response) => {
            // Reload from backend
            this.fetchChannels();
            this.$q.loading.hide();
          }).catch(() => {
            // Notify failure
            this.$q.notify({
              color: 'negative',
              position: 'top',
              message: 'An error was encountered while adding new channels.',
              icon: 'report_problem',
              actions: [{icon: 'close', color: 'white'}],
            });
            this.$q.loading.hide();
          });
        }
      }).onDismiss(() => {
      });
    },
    exportChannels: function() {
      this.$q.loading.show({
        message: 'Exporting config. Please wait...',
      });
      axios({
        method: 'GET',
        url: '/tic-api/export-config',
      }).then((response) => {
        // Display dialog with exported json
        this.chanelExportDialogJson = JSON.stringify(response.data.data, null, 2);
        this.chanelExportDialog = true;
        this.$q.loading.hide();
      }).catch(() => {
        this.$q.loading.hide();
        this.$q.notify({
          color: 'negative',
          position: 'top',
          message: 'Failed to fetch the current application config.',
          icon: 'report_problem',
          actions: [{icon: 'close', color: 'white'}],
        });
      });
    },
    copyExportJson: function() {
      copyToClipboard(this.chanelExportDialogJson).then(() => {
        // success!
        this.$q.notify({
          color: 'green',
          textColor: 'white',
          icon: 'done',
          message: 'Channel config copied to clipboard',
        });
      }).catch(() => {
        // fail
      });
    },
    importConfigJson: function() {
      // TODO: Validate JSON formatting
      // TODO: Import JSON to backend
      console.log('TODO');
      // post this.chanelExportDialogJson
    },
    saveChannels: function() {
      // Send changes to backend
      let data = {
        channels: {},
      };
      for (let i = 0; i < this.listOfChannels.length; i++) {
        const item = this.listOfChannels[i];
        data.channels[item.id] = item;
      }
      axios({
        method: 'POST',
        url: '/tic-api/channels/settings/multiple/save',
        data: data,
      }).then((response) => {
        // Reload from backend
        this.fetchChannels();
      }).catch(() => {
        // Notify failure
        this.$q.notify({
          color: 'negative',
          position: 'top',
          message: 'An error was encountered while saving the channel order.',
          icon: 'report_problem',
          actions: [{icon: 'close', color: 'white'}],
        });
      });
    },
    setChannelOrder: function(event) {
      console.log('setChannelOrder');
      const movedItem = event.moved.element;
      // Shift the channel number of item that was moved
      this.shiftChannelNumber(this.listOfChannels, movedItem.id);
      // Fix the channel numbering so there are no duplicates
      this.fixNumberIncrement(this.listOfChannels);
      // Save new channel layout
      this.saveChannels();
    },
    showChannelNumberMod: function(index) {
      console.log(index);
      this.channelNumberEditDialogVisible = true;
      this.editIndex = index;
      this.editedValue = this.listOfChannels[index].number;
      this.$nextTick(() => {
        this.$refs.input.select();
      });
    },
    showBulkEditCategoriesDialog: function() {
      this.bulkEditCategoriesDialogVisible = true;
    },
    toggleSelection(channel) {
      if (channel.selected) {
        this.selectedChannels.push(channel.id);
      } else {
        this.selectedChannels = this.selectedChannels.filter(id => id !== channel.id);
      }
    },
    openChannelsGroupImport: function() {
      this.$q.dialog({
        component: ChannelGroupSelectorDialog,
      }).onOk((payload) => {
        if (typeof payload.selectedGroups !== 'undefined' && payload.selectedGroups.length > 0) {
          // Debug what's being received from the dialog
          console.log('Selected groups payload:', payload.selectedGroups);

          // Add selected groups to list
          this.$q.loading.show();

          // Send changes to backend
          let data = {
            groups: [],
          };

          for (const i in payload.selectedGroups) {
            const group = payload.selectedGroups[i];
            console.log('Processing group:', group); // Debug each group

            data.groups.push({
              playlist_id: group.playlist_id,
              playlist_name: group.playlist_name,
              group_name: group.group_name,
            });
          }

          console.log('Data being sent to backend:', data); // Debug the final payload

          axios({
            method: 'POST',
            url: '/tic-api/channels/settings/groups/add',
            data: data,
          }).then((response) => {
            // Reload from backend
            this.fetchChannels();
            this.$q.loading.hide();

            this.$q.notify({
              color: 'positive',
              icon: 'cloud_done',
              message: `Successfully imported channels from ${payload.selectedGroups.length} group(s)`,
              timeout: 2000,
            });
          }).catch((error) => {
            // Log detailed error information
            console.error('Error response:', error.response ? error.response.data : error);

            // Notify failure
            this.$q.notify({
              color: 'negative',
              position: 'top',
              message: 'An error was encountered while adding channels from groups.',
              icon: 'report_problem',
              actions: [{icon: 'close', color: 'white'}],
            });
            this.$q.loading.hide();
          });
        }
      }).onDismiss(() => {
        // Handle dismiss if needed
      });
    },
    submitBulkCategoriesChange() {
      // Implement your logic to apply category changes
      console.log('Apply categories action:', this.applyCategoriesAction);
      console.log('Selected categories:', this.bulkEditCategories);
      for (let i = 0; i < this.listOfChannels.length; i++) {
        const item = this.listOfChannels[i];
        // Check if the channel is selected
        if (item.selected) {
          switch (this.applyCategoriesAction) {
            case 'Add':
              // Join bulkEditCategories with existing item.tags
              item.tags = [...new Set([...item.tags, ...this.bulkEditCategories])];
              break;
            case 'Remove':
              // Remove bulkEditCategories from existing item.tags
              item.tags = item.tags.filter(tag => !this.bulkEditCategories.includes(tag));
              break;
            case 'Replace':
              // Replace item.tags with bulkEditCategories
              item.tags = [...this.bulkEditCategories];
              break;
            default:
              // Handle default or unexpected case
              break;
          }
        }
      }
      // Hide dialog
      this.bulkEditCategoriesDialogVisible = false;
      // Reset inputs
      this.bulkEditCategories = [];
      this.applyCategoriesAction = 'Add';
      // Save new channel layout
      this.saveChannels();
      this.$q.notify({
        color: 'positive',
        icon: 'cloud_done',
        message: 'Bulk category changes saved.',
        timeout: 2000,
      });
    },
    triggerRefreshChannelSources: function() {
      // Add all channel streams to their respective refresh_sources list
      for (let i = 0; i < this.listOfChannels.length; i++) {
        const item = this.listOfChannels[i];
        // Check if the channel is selected
        if (item.selected) {
          item.refresh_sources = item.sources;
        }
      }
      // Save new channel layout
      this.saveChannels();
      this.$q.notify({
        color: 'positive',
        icon: 'sync',
        message: 'Bulk refresh queued for selected channels.',
        timeout: 2000,
      });
    },
    triggerDeleteChannels: function() {
      // Send changes to backend
      let data = {
        channels: {},
      };
      for (let i = 0; i < this.listOfChannels.length; i++) {
        const item = this.listOfChannels[i];
        // Check if the channel is selected
        if (item.selected) {
          data.channels[item.id] = item;
        }
      }
      axios({
        method: 'POST',
        url: '/tic-api/channels/settings/multiple/delete',
        data: data,
      }).then((response) => {
        // Reload from backend
        this.fetchChannels();
        this.$q.notify({
          color: 'positive',
          icon: 'delete',
          message: 'Selected channels deleted.',
          timeout: 2000,
        });
      }).catch(() => {
        // Notify failure
        this.$q.notify({
          color: 'negative',
          position: 'top',
          message: 'An error was encountered while deleting the selected channels.',
          icon: 'report_problem',
          actions: [{icon: 'close', color: 'white'}],
        });
      });
    },
    submitChannelNumberChange() {
      // Ensure value is a number
      // Ensure value is above 1000 and less than 10000
      if (isNaN(this.editedValue) || this.editedValue < 1000 || this.editedValue > 9999) {
        // Value already exists
        console.error('Value is less than 1000');
        // Notify failure
        this.$q.notify({
          color: 'negative',
          position: 'top',
          message: 'You must enter a number between 1000 and 9999.',
          icon: 'report_problem',
          actions: [{icon: 'close', color: 'white'}],
        });
        return;
      }

      if (this.editedValue === this.listOfChannels[this.editIndex].number) {
        // Value already exists
        console.warn('Value already exists');
        this.channelNumberEditDialogVisible = false;
        return;
      }
      this.listOfChannels[this.editIndex].number = this.editedValue;
      this.channelNumberEditDialogVisible = false;
      // Shift any conflicting numbers
      //this.shiftNumbers(this.listOfChannels, this.listOfChannels[this.editIndex].id)
      // Fix the channel numbering
      this.fixNumberIncrement(this.listOfChannels);
      // Save new channel layout
      this.saveChannels();
    },
  },
  created() {
    this.fetchUiSettings();
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

.channel-needs-attention {
  background: var(--channel-attention-bg);
  border-left: 4px solid var(--channel-attention-border);
  color: var(--channel-attention-text);
}
</style>
