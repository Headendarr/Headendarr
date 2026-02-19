<template>
  <q-page>
    <div class="q-pa-md">
      <div class="row">
        <div class="col-12 help-main help-main--full">
          <q-card flat>
            <q-card-section :class="$q.platform.is.mobile ? 'q-px-none' : ''">
              <TicListToolbar
                :add-action="{label: 'Add User', icon: 'person_add'}"
                :search="{label: 'Search users', placeholder: 'Username, role, streaming key...'}"
                :search-value="searchQuery"
                :filters="usersToolbarFilters"
                :collapse-filters-on-mobile="false"
                :sort-action="{label: usersSortLabel}"
                @add="openCreateDialog"
                @update:search-value="searchQuery = $event"
                @filter-change="onUsersToolbarFilterChange"
                @sort="usersSortDialogOpen = true"
              />
            </q-card-section>

            <q-card-section :class="$q.platform.is.mobile ? 'q-px-none' : ''">
              <q-list bordered separator class="rounded-borders">
                <q-item v-for="user in visibleUsers" :key="user.id" class="users-list-item">
                  <q-item-section avatar top>
                    <q-icon name="person" color="primary" />
                  </q-item-section>

                  <q-item-section top>
                    <q-item-label class="row items-center q-gutter-sm">
                      <span class="text-weight-medium">{{ user.username }}</span>
                      <q-chip dense :color="user.is_active ? 'positive' : 'negative'" text-color="white">
                        {{ user.is_active ? 'Active' : 'Disabled' }}
                      </q-chip>
                    </q-item-label>
                    <q-item-label caption> Roles: {{ (user.roles || []).join(', ') || 'None' }}</q-item-label>
                    <q-item-label caption>
                      DVR: {{ dvrModeLabel(user.dvr_access_mode) }}
                    </q-item-label>
                    <q-item-label caption class="row items-center q-gutter-xs no-wrap">
                      <span class="text-weight-medium">Streaming key:</span>
                      <span class="text-mono ellipsis">{{ user.streaming_key || '-' }}</span>
                      <TicActionButton
                        v-if="user.streaming_key"
                        icon="content_copy"
                        color="grey-8"
                        tooltip="Copy streaming key"
                        @click="copyStreamingKey(user.streaming_key)"
                      />
                    </q-item-label>
                  </q-item-section>

                  <q-item-section side top>
                    <TicListActions :actions="userActions(user)" @action="(action) => handleUserAction(action, user)" />
                  </q-item-section>
                </q-item>

                <q-item v-if="!loading && !visibleUsers.length">
                  <q-item-section>
                    <q-item-label class="text-grey-7"> No users found.</q-item-label>
                  </q-item-section>
                </q-item>
              </q-list>

              <div id="users-scroll-anchor" class="users-infinite-anchor">
                <q-infinite-scroll
                  v-if="!loading && hasMoreUsers"
                  ref="usersInfiniteRef"
                  :offset="80"
                  scroll-target="body"
                  @load="onUsersLoad"
                >
                  <template #loading>
                    <div class="row flex-center q-my-md">
                      <q-spinner-dots size="28px" color="primary" />
                    </div>
                  </template>
                </q-infinite-scroll>
              </div>

              <q-inner-loading :showing="loading">
                <q-spinner-dots size="40px" color="primary" />
              </q-inner-loading>
            </q-card-section>
          </q-card>
        </div>
      </div>
    </div>

    <TicDialogWindow
      v-model="showCreate"
      title="Create User"
      width="520px"
      :actions="createDialogActions"
      @action="onCreateDialogAction"
    >
      <div class="q-pa-md">
        <q-form class="tic-form-layout">
          <TicTextInput v-model="form.username" label="Username" description="Unique username for this account." />

          <TicTextInput
            v-model="form.password"
            type="password"
            label="Password"
            description="Set an initial password for this user."
          />

          <TicSelectInput
            v-model="form.roles"
            label="Roles"
            description="Assign one or more access roles."
            :options="roleOptions"
            option-label="label"
            option-value="value"
            :emit-value="true"
            :map-options="true"
            :multiple="false"
            :clearable="false"
            :behavior="$q.screen.lt.md ? 'dialog' : 'menu'"
          />

          <q-banner
            v-if="form.roles.includes('admin')"
            rounded
            dense
            class="bg-blue-1 text-primary"
          >
            Note: Admin users always have DVR access to everyone's recordings.
          </q-banner>
          <TicSelectInput
            v-model="form.dvr_access_mode"
            label="DVR Access"
            description="Choose whether this streaming user can access DVR recordings."
            :options="dvrAccessOptions"
            option-label="label"
            option-value="value"
            :emit-value="true"
            :map-options="true"
            :clearable="false"
            :behavior="$q.screen.lt.md ? 'dialog' : 'menu'"
            :disable="form.roles.includes('admin')"
          />
          <template v-if="form.dvr_access_mode !== 'none'">
            <TicSelectInput
              v-model="form.dvr_retention_policy"
              label="Retention policy"
              :options="retentionPolicyOptions"
              option-label="label"
              option-value="value"
              :emit-value="true"
              :map-options="true"
              :clearable="false"
              :behavior="$q.screen.lt.md ? 'dialog' : 'menu'"
              :disable="form.roles.includes('admin')"
            />
          </template>
        </q-form>
      </div>
    </TicDialogWindow>

    <TicDialogWindow
      v-model="showEdit"
      :title="`Edit User: ${editUser?.username || ''}`"
      width="560px"
      :actions="editDialogActions"
      @action="onEditDialogAction"
    >
      <div class="q-pa-md">
        <q-form class="tic-form-layout users-edit-form">
          <div class="users-edit-section">
            <TicToggleInput
              v-model="form.is_active"
              label="Enabled"
              description="Disable this account to block logins and API use."
            />

            <TicSelectInput
              v-model="form.roles"
              label="Roles"
              description="Assign one or more access roles."
              :options="roleOptions"
              option-label="label"
              option-value="value"
              :emit-value="true"
              :map-options="true"
              :multiple="true"
              :clearable="false"
              :behavior="$q.screen.lt.md ? 'dialog' : 'menu'"
            />

            <TicTextInput
              v-model="form.streaming_key"
              readonly
              label="Streaming Key"
              description="Used for playlist, EPG, and HDHomeRun endpoints."
            >
              <template #append>
                <TicActionButton
                  icon="content_copy"
                  color="grey-8"
                  tooltip="Copy streaming key"
                  @click="copyStreamingKey(form.streaming_key)"
                />
                <TicActionButton
                  icon="refresh"
                  color="warning"
                  tooltip="Rotate streaming key"
                  @click="confirmRotateStreamingKey(editUser)"
                />
              </template>
            </TicTextInput>
          </div>

          <q-separator class="users-edit-separator" />

          <div class="users-edit-heading">DVR</div>
          <div class="users-edit-section">
            <q-banner
              v-if="isAdminUser(editUser)"
              rounded
              dense
              class="bg-blue-1 text-primary"
            >
              NOTE: Admin users always have DVR access to everyone's recordings.
            </q-banner>
            <TicSelectInput
              v-model="form.dvr_access_mode"
              label="DVR Access"
              description="Choose whether this streaming user can access DVR recordings."
              :options="dvrAccessOptions"
              option-label="label"
              option-value="value"
              :emit-value="true"
              :map-options="true"
              :clearable="false"
              :behavior="$q.screen.lt.md ? 'dialog' : 'menu'"
              :disable="isAdminUser(editUser)"
            />
            <template v-if="form.dvr_access_mode !== 'none'">
              <TicSelectInput
                v-model="form.dvr_retention_policy"
                label="Retention policy"
                :options="retentionPolicyOptions"
                option-label="label"
                option-value="value"
                :emit-value="true"
                :map-options="true"
                :clearable="false"
                :behavior="$q.screen.lt.md ? 'dialog' : 'menu'"
                :disable="isAdminUser(editUser)"
              />
            </template>
          </div>
        </q-form>
      </div>
    </TicDialogWindow>

    <TicDialogPopup v-model="usersSortDialogOpen" title="Sort Users" width="560px" max-width="95vw">
      <div class="tic-form-layout">
        <TicSelectInput
          v-model="usersSortDraft.sortBy"
          label="Sort By"
          :options="userSortOptions"
          option-label="label"
          option-value="value"
          :emit-value="true"
          :map-options="true"
          :clearable="false"
          :behavior="$q.screen.lt.md ? 'dialog' : 'menu'"
        />
        <TicSelectInput
          v-model="usersSortDraft.sortDirection"
          label="Direction"
          :options="sortDirectionOptions"
          option-label="label"
          option-value="value"
          :emit-value="true"
          :map-options="true"
          :clearable="false"
          :behavior="$q.screen.lt.md ? 'dialog' : 'menu'"
        />
      </div>
      <template #actions>
        <TicButton label="Clear" variant="flat" color="grey-7" @click="clearUserSortDraft" />
        <TicButton label="Apply" icon="check" color="positive" @click="applyUserSortDraft" />
      </template>
    </TicDialogPopup>
  </q-page>
</template>

<script>
import axios from 'axios';
import {copyToClipboard} from 'quasar';
import {
  TicActionButton,
  TicButton,
  TicDialogPopup,
  TicDialogWindow,
  TicListActions,
  TicListToolbar,
  TicSelectInput,
  TicTextInput,
  TicToggleInput,
} from 'components/ui';

const USERS_PAGE_SIZE = 40;

export default {
  name: 'UsersPage',
  components: {
    TicActionButton,
    TicButton,
    TicDialogPopup,
    TicDialogWindow,
    TicListActions,
    TicListToolbar,
    TicSelectInput,
    TicTextInput,
    TicToggleInput,
  },
  data() {
    return {
      loading: false,
      creating: false,
      saving: false,
      users: [],
      searchQuery: '',
      roleFilter: null,
      usersSortDialogOpen: false,
      usersSort: {
        sortBy: 'username',
        sortDirection: 'asc',
      },
      usersSortDraft: {
        sortBy: 'username',
        sortDirection: 'asc',
      },
      showCreate: false,
      showEdit: false,
      editUser: null,
      visibleUsersCount: USERS_PAGE_SIZE,
      form: {
        username: '',
        password: '',
        roles: null,
        is_active: true,
        streaming_key: '',
        dvr_access_mode: 'none',
        dvr_retention_policy: 'forever',
      },
      roleOptions: [
        {label: 'Admin', value: 'admin'},
        {label: 'Streamer', value: 'streamer'},
      ],
      dvrAccessOptions: [
        {label: 'No DVR access', value: 'none'},
        {label: 'Access own recordings only', value: 'read_write_own'},
        {label: 'Grant access to everyone\'s recordings', value: 'read_all_write_own'},
      ],
      retentionPolicyOptions: [
        {label: '1 day', value: '1_day'},
        {label: '3 days', value: '3_days'},
        {label: '5 days', value: '5_days'},
        {label: '1 week', value: '1_week'},
        {label: '2 weeks', value: '2_weeks'},
        {label: '3 weeks', value: '3_weeks'},
        {label: '1 month', value: '1_month'},
        {label: '2 months', value: '2_months'},
        {label: '3 months', value: '3_months'},
        {label: '6 months', value: '6_months'},
        {label: '1 year', value: '1_year'},
        {label: '2 years', value: '2_years'},
        {label: '3 years', value: '3_years'},
        {label: 'Maintained space', value: 'maintained_space'},
        {label: 'Forever', value: 'forever'},
      ],
    };
  },
  computed: {
    roleFilterOptions() {
      return [{label: 'All', value: null}, ...this.roleOptions];
    },
    usersToolbarFilters() {
      return [
        {
          key: 'role',
          modelValue: this.roleFilter,
          label: 'Role',
          options: this.roleFilterOptions,
          clearable: false,
          dense: true,
          behavior: this.$q.screen.lt.md ? 'dialog' : 'menu',
        },
      ];
    },
    userSortOptions() {
      return [
        {label: 'Username', value: 'username'},
        {label: 'Role', value: 'role'},
        {label: 'Status', value: 'status'},
      ];
    },
    sortDirectionOptions() {
      return [
        {label: 'Ascending', value: 'asc'},
        {label: 'Descending', value: 'desc'},
      ];
    },
    usersSortLabel() {
      const sort = this.userSortOptions.find((item) => item.value === this.usersSort.sortBy);
      return sort ? `Sort: ${sort.label}` : 'Sort';
    },
    filteredUsers() {
      const query = String(this.searchQuery || '').trim().toLowerCase();
      const role = this.roleFilter;
      const filtered = (this.users || []).filter((user) => {
        const roles = Array.isArray(user.roles) ? user.roles : [];
        if (role && !roles.includes(role)) {
          return false;
        }
        if (!query) {
          return true;
        }
        const values = [user.username, user.streaming_key, roles.join(' '), user.is_active ? 'active' : 'disabled'];
        return values.some((value) =>
          String(value || '').toLowerCase().includes(query),
        );
      });
      return this.sortUsers(filtered);
    },
    visibleUsers() {
      return this.filteredUsers.slice(0, this.visibleUsersCount);
    },
    hasMoreUsers() {
      return this.visibleUsersCount < this.filteredUsers.length;
    },
    createDialogActions() {
      return [
        {
          id: 'create',
          label: 'Create',
          icon: 'save',
          color: 'positive',
          loading: this.creating,
        },
      ];
    },
    editDialogActions() {
      return [
        {
          id: 'save',
          label: 'Save',
          icon: 'save',
          color: 'positive',
          loading: this.saving,
        },
      ];
    },
  },
  watch: {
    searchQuery() {
      this.resetVisibleUsers();
    },
    roleFilter() {
      this.resetVisibleUsers();
    },
    usersSort: {
      deep: true,
      handler() {
        this.resetVisibleUsers();
      },
    },
    'form.roles': {
      deep: true,
      handler(nextRoles) {
        const roles = Array.isArray(nextRoles) ? nextRoles : [];
        if (roles.includes('admin')) {
          this.form.dvr_access_mode = 'read_all_write_own';
        }
      },
    },
  },
  methods: {
    async loadUsers() {
      this.loading = true;
      try {
        const response = await axios.get('/tic-api/users');
        this.users = response.data.data || [];
        this.resetVisibleUsers();
      } finally {
        this.loading = false;
      }
    },
    resetVisibleUsers() {
      this.visibleUsersCount = USERS_PAGE_SIZE;
      this.$nextTick(() => {
        if (this.$refs.usersInfiniteRef) {
          this.$refs.usersInfiniteRef.reset();
        }
      });
    },
    onUsersLoad(index, done) {
      if (!this.hasMoreUsers) {
        done(true);
        return;
      }
      this.visibleUsersCount += USERS_PAGE_SIZE;
      done(this.visibleUsersCount >= this.filteredUsers.length);
    },
    onUsersToolbarFilterChange({key, value}) {
      if (key === 'role') {
        this.roleFilter = value;
      }
    },
    sortUsers(users) {
      const direction = this.usersSort?.sortDirection === 'desc' ? -1 : 1;
      const sortBy = this.usersSort?.sortBy || 'username';
      const sorted = [...users];
      sorted.sort((a, b) => {
        if (sortBy === 'status') {
          const aValue = a?.is_active ? 'active' : 'disabled';
          const bValue = b?.is_active ? 'active' : 'disabled';
          return aValue.localeCompare(bValue) * direction;
        }
        if (sortBy === 'role') {
          const aValue = String((a?.roles || [])[0] || '');
          const bValue = String((b?.roles || [])[0] || '');
          return aValue.localeCompare(bValue) * direction;
        }
        const aValue = String(a?.username || '');
        const bValue = String(b?.username || '');
        return aValue.localeCompare(bValue) * direction;
      });
      return sorted;
    },
    clearUserSortDraft() {
      this.usersSortDraft = {sortBy: 'username', sortDirection: 'asc'};
    },
    applyUserSortDraft() {
      this.usersSort = {...this.usersSortDraft};
      this.usersSortDialogOpen = false;
    },
    userActions(user) {
      return [
        {
          id: 'edit',
          icon: 'edit',
          label: 'Edit user',
          color: 'primary',
          tooltip: `Edit ${user.username}`,
        },
        {
          id: 'reset-password',
          icon: 'password',
          label: 'Reset password',
          color: 'warning',
          tooltip: `Reset password for ${user.username}`,
        },
      ];
    },
    handleUserAction(action, user) {
      if (action.id === 'edit') {
        this.openEditDialog(user);
      }
      if (action.id === 'reset-password') {
        this.openResetPassword(user);
      }
    },
    openCreateDialog() {
      this.form = {
        username: '',
        password: '',
        roles: 'streamer',
        is_active: true,
        streaming_key: '',
        dvr_access_mode: 'none',
        dvr_retention_policy: 'forever',
      };
      this.showCreate = true;
    },
    onCreateDialogAction(action) {
      if (action.id === 'create') {
        this.createUser();
      }
    },
    async createUser() {
      if (!this.form.username || !this.form.password) {
        this.$q.notify({color: 'negative', message: 'Username and password are required'});
        return;
      }
      const roles = Array.isArray(this.form.roles) ? this.form.roles : (this.form.roles ? [this.form.roles] : []);
      const isAdmin = roles.includes('admin');
      this.creating = true;
      try {
        await axios.post('/tic-api/users', {
          username: this.form.username,
          password: this.form.password,
          roles,
          dvr_access_mode: isAdmin ? 'read_all_write_own' : this.form.dvr_access_mode,
          dvr_retention_policy: this.form.dvr_retention_policy,
        });
        this.showCreate = false;
        await this.loadUsers();
      } catch {
        this.$q.notify({color: 'negative', message: 'Failed to create user'});
      } finally {
        this.creating = false;
      }
    },
    openEditDialog(user) {
      const isAdmin = this.isAdminUser(user);
      this.editUser = user;
      this.form = {
        roles: [...(user.roles || [])],
        is_active: Boolean(user.is_active),
        streaming_key: user.streaming_key || '',
        dvr_access_mode: isAdmin ? 'read_all_write_own' : (user.dvr_access_mode || 'none'),
        dvr_retention_policy: user.dvr_retention_policy || 'forever',
      };
      this.showEdit = true;
    },
    onEditDialogAction(action) {
      if (action.id === 'save') {
        this.updateUser();
      }
    },
    async updateUser() {
      if (!this.editUser) {
        return;
      }
      const roles = Array.isArray(this.form.roles) ? this.form.roles : (this.form.roles ? [this.form.roles] : []);
      const isAdmin = roles.includes('admin');
      this.saving = true;
      try {
        await axios.put(`/tic-api/users/${this.editUser.id}`, {
          roles,
          is_active: this.form.is_active,
          dvr_access_mode: isAdmin ? 'read_all_write_own' : this.form.dvr_access_mode,
          dvr_retention_policy: this.form.dvr_retention_policy,
        });
        this.showEdit = false;
        await this.loadUsers();
      } catch {
        this.$q.notify({color: 'negative', message: 'Failed to update user'});
      } finally {
        this.saving = false;
      }
    },
    openResetPassword(user) {
      this.$q.dialog({
        title: `Reset Password (${user.username})`,
        message: 'Enter a new password',
        prompt: {
          model: '',
          type: 'password',
        },
        cancel: true,
      }).onOk(async (newPassword) => {
        if (!newPassword) {
          return;
        }
        await axios.post(`/tic-api/users/${user.id}/reset-password`, {password: newPassword});
        this.$q.notify({color: 'positive', message: 'Password updated'});
      });
    },
    async confirmRotateStreamingKey(user) {
      if (!user) {
        return;
      }
      this.$q.dialog({
        title: 'Rotate Streaming Key',
        message: 'Rotate this key? Existing playlist/EPG/HDHomeRun URLs for this user will stop working.',
        cancel: true,
        persistent: true,
        ok: {
          label: 'Rotate',
          color: 'negative',
        },
      }).onOk(async () => {
        await axios.post(`/tic-api/users/${user.id}/rotate-stream-key`);
        await this.loadUsers();
        if (this.editUser && this.editUser.id === user.id) {
          const refreshed = this.users.find((item) => item.id === user.id);
          if (refreshed) {
            this.form.streaming_key = refreshed.streaming_key || '';
          }
        }
      });
    },
    async copyStreamingKey(streamingKey) {
      if (!streamingKey) {
        return;
      }
      await copyToClipboard(streamingKey);
      this.$q.notify({color: 'positive', message: 'Streaming key copied to clipboard'});
    },
    dvrModeLabel(mode) {
      const found = this.dvrAccessOptions.find((item) => item.value === mode);
      return found ? found.label : 'No DVR access';
    },
    isAdminUser(user) {
      const roles = Array.isArray(user?.roles) ? user.roles : [];
      return roles.includes('admin');
    },
  },
  mounted() {
    this.usersSortDraft = {...this.usersSort};
    this.loadUsers();
  },
};
</script>

<style scoped>
.users-list-item {
  align-items: flex-start;
}

.users-infinite-anchor {
  min-height: 1px;
}

.users-edit-heading {
  font-size: 0.95rem;
  font-weight: 600;
  color: var(--q-primary);
}

.users-edit-section {
  display: flex;
  flex-direction: column;
  gap: 24px;
}

.users-edit-separator {
  margin: 8px 0;
}

@media (max-width: 1023px) {
  .users-list-item :deep(.q-item__section--side) {
    padding-left: 8px;
  }
}
</style>
