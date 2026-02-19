<template>
  <q-page padding>
    <div class="q-pa-none">
      <div class="row">
        <div
          :class="uiStore.showHelp ? 'col-12 col-md-8 help-main' : 'col-12 help-main help-main--full'">
          <q-card flat>
            <q-card-section :class="$q.platform.is.mobile ? 'q-px-none' : ''">
              <div class="row items-center q-col-gutter-sm justify-between">
                <template v-if="bulkEditMode !== true">
                  <div class="col-12 col-sm-auto">
                    <TicButton
                      label="Add Channel"
                      icon="add"
                      color="primary"
                      class="channels-toolbar-btn"
                      @click="openChannelSettings(null)"
                    />
                  </div>
                  <div class="col-12 col-sm-auto">
                    <TicButton
                      label="Import Channels from stream source"
                      icon="dvr"
                      color="primary"
                      class="channels-toolbar-btn"
                      @click="openChannelsImport()"
                    />
                  </div>
                  <div class="col-12 col-sm-auto">
                    <TicButton
                      label="Import Channels by Group"
                      icon="group_work"
                      color="primary"
                      class="channels-toolbar-btn"
                      @click="openChannelsGroupImport()"
                    />
                  </div>
                </template>

                <template v-else>
                  <div class="col-12 col-sm-auto">
                    <TicButton
                      label="Edit Categories"
                      color="primary"
                      :disable="!anyChannelsSelectedInBulkEdit"
                      class="channels-toolbar-btn"
                      @click="showBulkEditCategoriesDialog()"
                    />
                  </div>
                  <div class="col-12 col-sm-auto">
                    <TicButton
                      :label="allChannelsSelected ? 'Deselect All' : 'Select All'"
                      color="primary"
                      class="channels-toolbar-btn"
                      @click="selectAllChannels()"
                    />
                  </div>
                  <div class="col-12 col-sm-auto">
                    <TicButtonDropdown
                      label="Select by Category"
                      color="primary"
                      class="channels-toolbar-btn"
                    >
                      <q-list>
                        <q-item
                          v-for="category in availableCategories"
                          :key="category"
                          clickable
                          v-close-popup
                          @click="selectChannelsByCategory(category)"
                        >
                          <q-item-section>{{ category }}</q-item-section>
                        </q-item>
                      </q-list>
                    </TicButtonDropdown>
                  </div>
                  <div class="col-12 col-sm-auto">
                    <TicButton
                      label="Refresh Channel Streams"
                      color="info"
                      :disable="!anyChannelsSelectedInBulkEdit"
                      class="channels-toolbar-btn"
                      @click="triggerRefreshChannelSources()"
                    />
                  </div>
                  <div class="col-12 col-sm-auto">
                    <TicButton
                      label="Delete Channels"
                      color="negative"
                      :disable="!anyChannelsSelectedInBulkEdit"
                      class="channels-toolbar-btn"
                      @click="confirmBulkDeleteChannels()"
                    />
                  </div>
                </template>

                <div class="col-12 col-sm-auto">
                  <TicButton
                    :label="bulkEditMode ? 'Exit Bulk Edit' : 'Bulk Edit'"
                    :icon="bulkEditMode ? 'format_line_spacing' : 'fact_check'"
                    color="primary"
                    class="channels-toolbar-btn"
                    @click="bulkEditMode = !bulkEditMode"
                  />
                </div>
              </div>
            </q-card-section>

            <q-card-section :class="$q.platform.is.mobile ? 'q-px-none' : ''">
              <div class="q-gutter-sm">
                <TicDialogPopup
                  v-model="channelNumberEditDialogVisible"
                  title="Edit Channel Number"
                  width="420px"
                  max-width="95vw"
                >
                  <q-form class="tic-form-layout" @submit.prevent="submitChannelNumberChange">
                    <TicNumberInput
                      ref="channelNumberInputRef"
                      v-model="editedValue"
                      label="Channel number"
                      description="Enter a number between 1000 and 9999."
                      :min="1000"
                      :max="9999"
                      @keydown.enter="submitChannelNumberChange"
                    />
                  </q-form>
                  <template #actions>
                    <TicButton
                      label="Cancel"
                      variant="flat"
                      color="grey-7"
                      @click="channelNumberEditDialogVisible = false"
                    />
                    <TicButton label="Save" icon="save" color="positive" @click="submitChannelNumberChange" />
                  </template>
                </TicDialogPopup>

                <TicDialogPopup
                  v-model="bulkEditCategoriesDialogVisible"
                  title="Bulk Edit Categories"
                  width="700px"
                  max-width="95vw"
                >
                  <q-form class="tic-form-layout" @submit.prevent="submitBulkCategoriesChange">
                    <TicSelectInput
                      v-model="applyCategoriesAction"
                      :options="applyCategoriesOptions"
                      label="Apply Categories Action"
                      description="Choose whether to add, remove, or replace categories on selected channels."
                    />
                    <div>
                      <!-- Uses raw q-select to support new-value-mode=add-unique tag entry. -->
                      <q-select
                        v-model="bulkEditCategories"
                        outlined
                        use-input
                        use-chips
                        multiple
                        hide-dropdown-icon
                        input-debounce="0"
                        new-value-mode="add-unique"
                        :label="`Categories to ${applyCategoriesAction}`"
                      />
                      <div class="tic-input-description text-caption text-grey-7">
                        Enter categories manually, then press Enter to add each value.
                      </div>
                    </div>
                    <div class="text-grey-8">
                      <p><b>Add:</b> Add categories to the existing channel categories.</p>
                      <p><b>Remove:</b> Remove entered categories from selected channels.</p>
                      <p><b>Replace:</b> Replace channel categories with entered categories.</p>
                      <p>Leave categories empty to clear all categories from selected channels.</p>
                    </div>
                  </q-form>
                  <template #actions>
                    <TicButton
                      label="Cancel"
                      variant="flat"
                      color="grey-7"
                      @click="bulkEditCategoriesDialogVisible = false"
                    />
                    <TicButton label="Save" icon="save" color="positive" @click="submitBulkCategoriesChange" />
                  </template>
                </TicDialogPopup>

                <q-list bordered separator class="channels-list rounded-borders q-pl-none">
                  <draggable
                    group="channels"
                    item-key="number"
                    handle=".handle"
                    :component-data="{tag: 'ul', name: 'flip-list', type: 'transition'}"
                    v-model="listOfChannels"
                    v-bind="dragOptions"
                    @change="setChannelOrder"
                  >
                    <template #item="{element, index}">
                      <q-item
                        :key="index"
                        class="channel-list-item q-px-none rounded-borders"
                        :class="!$q.screen.lt.md ? channelRowClass(element) : ''"
                      >
                        <template v-if="!$q.screen.lt.md">
                          <q-item-section avatar class="q-px-sm q-mx-sm handle">
                            <q-checkbox
                              v-if="bulkEditMode === true"
                              v-model="element.selected"
                              color="primary"
                              @click="toggleSelection(element)"
                            />
                            <q-avatar v-else rounded>
                              <q-icon name="format_line_spacing" style="max-width: 30px; cursor: grab">
                                <q-tooltip class="bg-white text-primary">Drag to move and re-order</q-tooltip>
                              </q-icon>
                            </q-avatar>
                          </q-item-section>

                          <q-separator inset vertical class="gt-xs" />

                          <q-item-section
                            class="q-px-sm q-mx-sm cursor-pointer"
                            style="max-width: 80px"
                            @click="bulkEditMode ? (element.selected = !element.selected) : showChannelNumberMod(index)"
                          >
                            <q-item-label lines="1" class="text-left">
                              <span class="q-ml-sm">Channel</span>
                            </q-item-label>
                            <q-item-label
                              caption
                              lines="1"
                              style="text-decoration: underline"
                              class="text-left text-primary q-ml-sm"
                            >
                              {{ element.number }}
                            </q-item-label>
                          </q-item-section>

                          <q-separator inset vertical class="gt-xs" />

                          <q-item-section top class="q-mx-md">
                            <q-item-label lines="1" class="text-left">
                              <div class="row items-center no-wrap">
                                <q-avatar rounded size="35px">
                                  <q-img :src="element.logo_url" style="max-width: 30px" />
                                </q-avatar>
                                <span class="text-weight-medium text-primary q-ml-sm channel-name-label">
                                  {{ element.name }}
                                </span>
                                <q-space />
                                <div class="row items-center no-wrap q-gutter-xs">
                                  <q-chip
                                    v-if="
                                      enableChannelHealthHighlight &&
                                      element.status &&
                                      element.status.suggestion_count > 0
                                    "
                                    dense
                                    color="green-6"
                                    text-color="white"
                                    clickable
                                    @click.stop="openChannelSuggestionsDialog(element)"
                                  >
                                    <q-icon name="tips_and_updates" />
                                    <span class="gt-lg q-ml-xs">Stream suggestions</span>
                                    <q-tooltip class="bg-white text-primary">
                                      Potential matching streams are available for this channel.
                                    </q-tooltip>
                                  </q-chip>
                                  <q-chip
                                    v-if="
                                      enableChannelHealthHighlight &&
                                      element.status &&
                                      element.status.state === 'warning'
                                    "
                                    dense
                                    color="orange-6"
                                    text-color="white"
                                    clickable
                                    @click.stop="openChannelIssuesDialog(element)"
                                  >
                                    <q-icon name="warning" />
                                    <span class="gt-lg q-ml-xs">Needs attention</span>
                                    <q-tooltip class="bg-white text-primary">
                                      {{ channelIssueFirstLabel(element.status) }}
                                    </q-tooltip>
                                  </q-chip>
                                </div>
                              </div>
                            </q-item-label>
                            <q-item-label caption lines="1" class="text-left q-ml-none">
                              <div class="channel-meta-list">
                                <div class="channel-meta-row lt-lg">
                                  <div class="channel-meta-label">Guide:</div>
                                  <div class="channel-meta-value">{{ formatGuideLabel(element) }}</div>
                                </div>
                                <div class="channel-meta-row">
                                  <div class="channel-meta-label">Stream sources:</div>
                                  <div class="channel-meta-value">{{ streamSourceNames(element) }}</div>
                                </div>
                                <div class="channel-meta-row">
                                  <div class="channel-meta-label">Categories:</div>
                                  <div class="channel-meta-value">{{ element.tags }}</div>
                                </div>
                              </div>
                            </q-item-label>
                          </q-item-section>

                          <q-separator inset vertical class="gt-xs gt-md" />

                          <q-item-section class="q-px-sm q-mx-sm gt-md">
                            <q-item-label lines="1" class="text-left">
                              <span class="q-ml-sm">Guide</span>
                            </q-item-label>
                            <q-item-label caption lines="1" class="text-left q-ml-sm">
                              {{ formatGuideLabel(element) }}
                            </q-item-label>
                          </q-item-section>

                          <q-separator inset vertical class="gt-xs" />

                          <q-item-section side class="q-mr-md">
                            <TicListActions
                              :actions="channelActions(element)"
                              @action="(action) => handleChannelAction(action, element)"
                            />
                          </q-item-section>
                        </template>

                        <template v-else>
                          <q-item-section>
                            <TicListItemCard v-bind="channelCardProps(element)">
                              <template #header-left>
                                <div class="row items-center no-wrap q-pr-sm">
                                  <div class="handle channel-drag-handle q-mr-sm">
                                    <q-checkbox
                                      v-if="bulkEditMode === true"
                                      v-model="element.selected"
                                      color="primary"
                                      @click="toggleSelection(element)"
                                    />
                                    <q-icon v-else name="format_line_spacing" size="22px" style="cursor: grab" />
                                  </div>
                                  <div>
                                    <div class="text-caption text-grey-7">Reorder</div>
                                    <div v-if="!bulkEditMode" class="text-caption text-grey-6 lt-sm">
                                      Tap and hold to drag
                                    </div>
                                  </div>
                                </div>
                              </template>
                              <template #header-actions>
                                <TicActionButton
                                  v-for="action in channelActions(element)"
                                  :key="`compact-${element.id}-${action.id}`"
                                  :icon="action.icon"
                                  :color="action.color || 'grey-8'"
                                  :tooltip="action.label || ''"
                                  @click="handleChannelAction(action, element)"
                                />
                              </template>
                              <div class="row items-start">
                                <q-avatar rounded class="q-mr-sm channel-card-avatar">
                                  <q-img :src="element.logo_url" />
                                </q-avatar>
                                <div class="col">
                                  <div class="text-weight-medium text-primary">{{ element.name }}</div>
                                  <div
                                    class="text-caption text-grey-7 channel-number-link"
                                    @click="
                                      bulkEditMode
                                        ? (element.selected = !element.selected)
                                        : showChannelNumberMod(index)
                                    "
                                  >
                                    Channel {{ element.number }}
                                  </div>
                                </div>
                                <div class="column items-end q-gutter-xs">
                                  <q-chip
                                    v-if="
                                      enableChannelHealthHighlight &&
                                      element.status &&
                                      element.status.state === 'warning'
                                    "
                                    dense
                                    color="orange-6"
                                    text-color="white"
                                    clickable
                                    @click.stop="openChannelIssuesDialog(element)"
                                  >
                                    <q-icon name="warning" class="q-mr-xs" />
                                    <span class="gt-xs">Needs attention</span>
                                  </q-chip>
                                  <q-chip
                                    v-if="
                                      enableChannelHealthHighlight &&
                                      element.status &&
                                      element.status.suggestion_count > 0
                                    "
                                    dense
                                    color="green-6"
                                    text-color="white"
                                    clickable
                                    @click.stop="openChannelSuggestionsDialog(element)"
                                  >
                                    <q-icon name="tips_and_updates" class="q-mr-xs" />
                                    <span class="gt-xs">Stream suggestions</span>
                                  </q-chip>
                                </div>
                              </div>

                              <div class="row q-col-gutter-sm q-mt-sm">
                                <div class="col-6">
                                  <div class="text-caption text-grey-7">Stream sources</div>
                                  <div class="text-caption channel-wrap-text">{{ streamSourceNames(element) }}</div>
                                </div>
                                <div class="col-6">
                                  <div class="text-caption text-grey-7">Guide</div>
                                  <div class="text-caption channel-wrap-text">{{ formatGuideLabel(element) }}</div>
                                </div>
                                <div class="col-12">
                                  <div class="text-caption text-grey-7">Categories</div>
                                  <div class="text-caption channel-wrap-text">{{ element.tags }}</div>
                                </div>
                              </div>
                            </TicListItemCard>
                          </q-item-section>
                        </template>
                      </q-item>
                    </template>
                  </draggable>
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
                    1. Start by clicking the <b>Import Channels from stream source</b> button. With this dialog
                    open, select one or more streams from your imported stream sources, then close the dialog to
                    import them into your channel list.
                  </q-item-label>
                </q-item-section>
              </q-item>
              <q-item>
                <q-item-section>
                  <q-item-label>
                    2. Click on the <b>Configure</b>
                    (
                    <q-icon name="tune" />
                    ) button for each added channel.
                    <br />
                    In the Channel Settings dialog that opens you can further configure channel categories and
                    additional streams from other sources.
                  </q-item-label>
                </q-item-section>
              </q-item>
              <q-item>
                <q-item-section>
                  <q-item-label>
                    3. Click and hold the drag (
                    <q-icon name="format_line_spacing" />
                    ) icon to quickly change the order of your channel list.
                  </q-item-label>
                </q-item-section>
              </q-item>
              <q-item>
                <q-item-section>
                  <q-item-label> 4. Click a channel's number to open the channel number editor.</q-item-label>
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

              <q-item-label class="text-primary"> Channel Settings - Streams:</q-item-label>
              <q-item>
                <q-item-section>
                  <q-item-label>
                    When you open a channel's settings, you can configure multiple streams for each channel. Drag
                    the streams in order of preference. If a stream has reached the connection limits, the next
                    stream will be used automatically.
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
import draggable from 'vuedraggable';
import {useUiStore} from 'stores/ui';
import {useVideoStore} from 'stores/video';
import ChannelInfoDialog from 'components/ChannelInfoDialog.vue';
import ChannelStreamSelectorDialog from 'components/ChannelStreamSelectorDialog.vue';
import ChannelGroupSelectorDialog from 'components/ChannelGroupSelectorDialog.vue';
import ChannelSuggestionsDialog from 'components/ChannelSuggestionsDialog.vue';
import ChannelIssuesDialog from 'components/ChannelIssuesDialog.vue';
import {copyToClipboard} from 'quasar';
import {
  TicActionButton,
  TicButton,
  TicButtonDropdown,
  TicConfirmDialog,
  TicDialogPopup,
  TicListItemCard,
  TicListActions,
  TicNumberInput,
  TicResponsiveHelp,
  TicSelectInput,
} from 'components/ui';

export default defineComponent({
  name: 'ChannelsPage',
  components: {
    draggable,
    TicActionButton,
    TicButton,
    TicButtonDropdown,
    TicDialogPopup,
    TicListItemCard,
    TicListActions,
    TicNumberInput,
    TicResponsiveHelp,
    TicSelectInput,
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
        delay: 260,
        delayOnTouchOnly: true,
      };
    },
    allChannelsSelected() {
      return this.listOfChannels.length > 0 && this.listOfChannels.every((channel) => channel.selected);
    },
    anyChannelsSelectedInBulkEdit() {
      // Check if any channels are selected
      return this.listOfChannels.some((channel) => channel.selected);
    },
    availableCategories() {
      // Extract all unique categories from channels
      const allCategories = new Set();
      this.listOfChannels.forEach((channel) => {
        if (channel.tags && Array.isArray(channel.tags)) {
          channel.tags.forEach((tag) => {
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
      this.listOfChannels.forEach((channel) => {
        channel.selected = shouldSelect;

        if (shouldSelect) {
          if (!this.selectedChannels.includes(channel.id)) {
            this.selectedChannels.push(channel.id);
          }
        } else {
          this.selectedChannels = this.selectedChannels.filter((id) => id !== channel.id);
        }
      });

      // Show notification
      this.$q.notify({
        color: 'positive',
        message: shouldSelect ? `Selected all ${this.listOfChannels.length} channels` : 'Deselected all channels',
        icon: shouldSelect ? 'select_all' : 'deselect',
        timeout: 2000,
      });
    },
    selectChannelsByCategory(category) {
      let count = 0;

      // Loop through channels and select those with the specified category
      this.listOfChannels.forEach((channel) => {
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
    channelActions: function() {
      return [
        {id: 'preview', icon: 'play_arrow', label: 'Preview Stream', color: 'primary', tooltip: 'Preview Stream'},
        {id: 'copy', icon: 'content_copy', label: 'Copy Stream URL', color: 'grey-8', tooltip: 'Copy Stream URL'},
        {id: 'configure', icon: 'tune', label: 'Configure Channel', color: 'grey-8', tooltip: 'Configure Channel'},
      ];
    },
    handleChannelAction: function(action, channel) {
      if (action.id === 'preview') {
        this.previewChannel(channel);
        return;
      }
      if (action.id === 'copy') {
        this.copyStreamUrl(channel);
        return;
      }
      this.openChannelSettings(channel);
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
          item.number = lastNumber + 1;
          newList.push(item);
        } else if (parseInt(item.number) === parseInt(lastNumber)) {
          item.number++;
          newList.push(item);
        } else if (parseInt(item.number) > parseInt(lastNumber)) {
          newList.push(item);
        } else if (parseInt(item.number) < parseInt(lastNumber)) {
          item.number = lastNumber + 1;
          newList.push(item);
        } else {
          console.error('--- Missed item ---');
          console.error(lastNumber);
          console.error(item.number);
          console.error(item.number === lastNumber);
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
        this.listOfChannels = response.data.data.sort((a, b) => a.number - b.number).map((channel) => {
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
    channelCardProps: function(channel) {
      if (!this.enableChannelHealthHighlight) {
        if (channel.enabled) {
          return {};
        }
        return {
          accentColor: 'var(--tic-list-card-disabled-border)',
          surfaceColor: 'var(--tic-list-card-disabled-bg)',
          headerColor: 'var(--tic-list-card-disabled-header)',
        };
      }
      if (!channel.enabled) {
        return {
          accentColor: 'var(--tic-list-card-disabled-border)',
          surfaceColor: 'var(--tic-list-card-disabled-bg)',
          headerColor: 'var(--tic-list-card-disabled-header)',
        };
      }
      if (channel.status && channel.status.state === 'warning') {
        return {
          accentColor: 'var(--tic-list-card-issues-border)',
          surfaceColor: 'var(--tic-list-card-issues-bg)',
          headerColor: 'var(--tic-list-card-issues-header)',
        };
      }
      return {
        accentColor: '',
        surfaceColor: 'var(--tic-list-card-default-bg)',
        headerColor: 'var(--tic-list-card-default-header-bg)',
      };
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
      return status.issues.map((issue) => labels[issue] || issue);
    },
    channelIssueFirstLabel: function(status) {
      const labels = this.channelIssueLabels(status);
      return labels.length ? labels[0] : '';
    },
    streamSourceNames: function(channel) {
      if (!channel?.sources) {
        return '';
      }
      return Object.keys(channel.sources).map((key) => channel.sources[key]?.playlist_name).filter(Boolean).join(', ');
    },
    formatGuideLabel: function(channel) {
      const epgName = channel?.guide?.epg_name || '';
      const guideChannelId = channel?.guide?.channel_id || '';
      return `${epgName} - ${guideChannelId}`.trim();
    },
    openChannelIssuesDialog: function(channel) {
      if (!channel?.status?.issues?.length) {
        return;
      }
      this.$q.dialog({
        noRouteDismiss: true,
        component: ChannelIssuesDialog,
        componentProps: {
          channel,
          issues: channel.status.issues,
        },
      }).onOk((payload) => {
        if (payload?.openSettings) {
          const channelId = payload?.channelId || channel?.id;
          const targetChannel = this.listOfChannels.find((item) => item?.id === channelId) || channel;
          // Open on next tick so the current dialog teardown fully completes first.
          setTimeout(() => {
            this.openChannelSettings(targetChannel);
          }, 0);
          return;
        }
        if (payload?.refresh) {
          this.fetchChannels();
        }
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
        noRouteDismiss: true,
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
        noRouteDismiss: true,
        component: ChannelSuggestionsDialog,
        componentProps: {
          channelId: channel.id,
        },
      }).onOk((payload) => {
        if (payload?.openSettings) {
          const channelId = payload?.channelId || channel?.id;
          const targetChannel = this.listOfChannels.find((item) => item?.id === channelId) || channel;
          // Open on next tick so the current dialog teardown fully completes first.
          setTimeout(() => {
            this.openChannelSettings(targetChannel);
          }, 0);
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
        noRouteDismiss: true,
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
        const payload = {
          ...item,
          logo_url: item.source_logo_url ?? item.logo_url,
        };
        delete payload.status;
        delete payload.selected;
        data.channels[item.id] = payload;
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
        const inputEl = this.$refs.channelNumberInputRef?.$el?.querySelector('input');
        if (inputEl) {
          inputEl.select();
        }
      });
    },
    showBulkEditCategoriesDialog: function() {
      this.bulkEditCategoriesDialogVisible = true;
    },
    toggleSelection(channel) {
      if (channel.selected) {
        this.selectedChannels.push(channel.id);
      } else {
        this.selectedChannels = this.selectedChannels.filter((id) => id !== channel.id);
      }
    },
    openChannelsGroupImport: function() {
      this.$q.dialog({
        noRouteDismiss: true,
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
              item.tags = item.tags.filter((tag) => !this.bulkEditCategories.includes(tag));
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
    confirmBulkDeleteChannels: function() {
      this.$q.dialog({
        noRouteDismiss: true,
        component: TicConfirmDialog,
        componentProps: {
          title: 'Delete Selected Channels?',
          message:
            'This action is final and cannot be undone. Are you sure you want to delete all selected channels?',
          icon: 'delete_forever',
          iconColor: 'negative',
          confirmLabel: 'Delete',
          confirmIcon: 'delete',
          confirmColor: 'negative',
          cancelLabel: 'Cancel',
        },
      }).onOk(() => {
        this.triggerDeleteChannels();
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
  transition: flex-basis 0.25s ease,
  max-width 0.25s ease;
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
  background: var(--tic-list-card-issues-bg);
  border-left: 4px solid var(--tic-list-card-issues-border);
}

.tic-form-layout > *:not(:last-child) {
  margin-bottom: 24px;
}

.tic-input-description {
  margin-top: 4px;
  margin-left: 8px;
}

.channel-drag-handle {
  min-width: 26px;
  display: inline-flex;
  align-items: center;
  justify-content: center;
}

.channel-number-link {
  text-decoration: underline;
  cursor: pointer;
}

@media (max-width: 1023px) {
  .channels-list {
    border: none;
  }

  .channel-list-item {
    margin-top: 4px;
    margin-bottom: 4px;
    border-bottom: 1px solid var(--q-separator-color);
    padding-left: 0;
    padding-right: 0;
    padding-top: 0;
    padding-bottom: 0;
  }

  .channels-list .channel-list-item:last-child {
    border-bottom: none;
  }

}

@media (max-width: 599px) {
  .channel-list-item {
    border-bottom: 2px solid var(--q-separator-color);
  }
}

.channel-wrap-text {
  display: block;
  white-space: normal;
  word-break: break-word;
  overflow-wrap: anywhere;
}

.channel-meta-list {
  display: flex;
  flex-direction: column;
  gap: 2px;
}

.channel-meta-row {
  display: grid;
  grid-template-columns: minmax(110px, 140px) minmax(0, 1fr);
  column-gap: 8px;
  align-items: start;
}

.channel-meta-label {
  white-space: nowrap;
}

.channel-meta-value {
  min-width: 0;
  white-space: normal;
  word-break: break-word;
  overflow-wrap: anywhere;
}

.channel-name-label {
  min-width: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.channels-toolbar-btn {
  width: 100%;
}

.channel-card-avatar {
  width: 30px;
  height: 30px;
}

@media (max-width: 599px) {
  .channel-card-avatar {
    width: 32px;
    height: 32px;
  }
}
</style>
